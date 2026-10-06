# 本地增量数据接入管线

基于 FastAPI + SQLite 的本地增量数据接入服务：按数据源维护增量位点并应用变更，
在重复消费、并发触发与源端结构变更下保证目标数据状态一致且可解释。
仅依赖本地存储与开源依赖，不引入外部服务或真实凭据。

## 架构

```
main.py                 FastAPI 接入层（HTTP 协议入口）
ingestion/
  models.py             变更记录、位点、失败原因、处置状态、重放结果等核心类型
  store.py              SQLite 持久层：目标行 / 位点 / schema 注册表 / 死信 / 重放流水
  schema.py             结构变更兼容规则与缺省值语义
  locks.py              per-source 锁注册表（正常接入与重放共用同一把锁）
  pipeline.py           校验、幂等去重、按数据源互斥、有界重试
  replay.py             失败记录隔离处置（筛选/分页/标注）与受控重放
tests/                  重复消费 / 位点回退 / 结构变更 / 并发触发 / 受控重放场景
```

## 接入协议（HTTP）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/sources/{source}/init` | 注册数据源初始 schema：`{"columns": {"id": "int", ...}}` |
| POST | `/sources/{source}/ingest` | 投递变更批：`{"records": [{position, op, pk, payload, schema_version}]}` |
| POST | `/sources/{source}/schema-changes` | 投递结构变更：`{position, to_version, add_columns, drop_columns, rename_columns}` |
| GET | `/sources/{source}/checkpoint` | 查询当前位点 |
| GET | `/sources/{source}/schema` | 查询目标 schema |
| GET | `/sources/{source}/rows` | 查询目标数据 |
| GET | `/sources/{source}/rejections` | 查询死信（含失败原因与原始记录） |
| GET | `/dead-letters` | 失败记录分页筛选：`source`/`disposition`/`reason`/`limit`/`offset` |
| GET | `/dead-letters/{id}` | 按 id 单独定位一条失败记录 |
| POST | `/dead-letters/{id}/disposition` | 标注处置状态与说明：`{"disposition": "pending"\|"resolved"\|"ignored", "note": "..."}` |
| POST | `/replay` | 受控重放：`{"dead_letter_ids": [id, ...]}`（一条或一小批） |
| GET | `/dead-letters/{id}/replays` | 查询一条失败记录的完整重放流水 |

变更记录字段：`position`（源端单调递增位点）、`op`（`insert`/`update`/`delete`）、
`pk`（主键字典）、`payload`（整行数据，delete 可省略）。

## 位点与并发语义

- **位点**：每个数据源独立维护 `last_position`。批内按位点升序应用；
  应用、拒绝记录都会推进位点并落 `applied_positions` 流水，重启后状态保留。
- **幂等**：位点已应用过的记录被识别为重复消费，幂等跳过（计入 `duplicates`），
  目标数据不重复、不丢失。
- **明确拒绝**：位点回退（`position_regression`）、主键缺失（`missing_primary_key`）、
  主键冲突（`primary_key_conflict`）、字段缺失（`missing_field`）、
  类型不匹配（`type_mismatch`）、未知列（`unknown_column`）逐条拒绝并写入死信表，
  原因可区分，绝不静默跳过。拒绝记录同样推进位点，避免同一坏记录被无界重投。
- **互斥与有序**：同一数据源的接入通过 per-source 锁互斥、批内按位点升序应用；
  不同数据源互不阻塞，单个数据源的失败或重试不影响其他数据源。
- **重试**：存储层异常按可恢复（如 SQLite 瞬时锁定）/不可恢复分类，
  仅可恢复错误做有界退避重试，超过上限即抛出，避免无界重试与资源耗尽。
- **事务**：每批变更在单个 SQLite 事务内完成（写目标行 + 位点流水 + 死信 +
  推进 checkpoint），要么全部成功要么全部回滚。

## 结构兼容规则

- **新增列**：兼容。按类型映射到稳定确定的缺省值
  （`int→0`、`float→0.0`、`str→""`、`bool→false`），并为存量行回填；
  之后的新记录必须携带该列，缺列按 `missing_field` 拒绝，不会静默错列。
- **删列 / 改名**：不兼容。目标 schema 不被修改，原始变更记录完整保留在死信表，
  标记为 `incompatible_schema_change`，可追溯。
- 结构变更同样按位点幂等：同一变更重复投递不会重复应用。

## 失败记录的隔离处置

每条被明确拒绝的记录都以独立行落在 `dead_letters` 表，并带可区分的失败原因
（`reason`）、拒绝说明（`detail`）与**原始记录**（`record`），可单独定位：

- **分页筛选**：`GET /dead-letters` 支持按数据源 `source`、处置状态 `disposition`、
  失败原因 `reason` 过滤，并用 `limit`/`offset` 分页，响应含 `total`。
  查询走 `(source, disposition, reason, id)` 索引，只读取当前一页，不会把堆积的
  失败记录一次性读入内存。
- **处置状态流转**：
  - `pending`（待处理，默认）→ 可标注为 `resolved`（已解决）或 `ignored`（已忽略）；
  - 任何状态都可重新标注回 `pending`（重开）；
  - 每次标注都记录处置时间 `handled_at` 与处理说明 `note`。
- **可追溯**：死信行上保存最近一次重放结果（`replay_state`/`replay_reason`/
  `replay_detail`）、累计重放尝试次数（`replay_attempts`）与最近重放时间；
  `replay_events` 流水表保留每一次实际重放，`GET /dead-letters/{id}/replays` 可查全过程。

## 受控重放与位点、并发的关系

源端数据或结构修好后，可对某一条或一小批失败记录发起受控重放
（`POST /replay`）。其核心规则：

1. **重放是位点之外的受控补偿通道，不碰位点。**
   记录被拒当时 checkpoint 已照常推进（位点只进不退）。重放既不读取、也不校验、
   更不回退 checkpoint，也不写 `applied_positions` 流水——因此它：
   - 不会被“重复消费（位点已应用）”判定拦掉（那只作用于正常接入路径）；
   - 不会倒回位点、不会破坏已经落地的目标状态。
   重放后位点保持不变，可在原拒绝记录与 `replay_events` 上看到最终是
   `applied` 还是再次 `rejected`，全程可追溯。
2. **与正常接入完全相同的校验和写入语义。**
   重放复用同一套逐条校验（`validate_record`）与 `target_rows` 写入：
   - 用**当前最新**目标 schema 重新判定，源端修好后即可通过；
   - 该拒的仍然明确拒，原因与正常接入完全一致、可区分；
   - `insert` 命中已存在主键得到 `primary_key_conflict`，**不会因为走重放就
     悄悄覆盖已落地数据**（只有源端投递的 `update` 才覆盖）。
3. **重放自身幂等。** 只有 `pending` 的死信可重放：重放成功自动流转为
   `resolved`；仍失败则保持 `pending` 并记录最新原因。对已 `resolved`/`ignored`
   的记录重复重放直接计为 `skipped`，不做任何写入、不产生重复数据、不重复计数；
   同一批次里重复的 id 会去重。确需再处理，先标注回 `pending`。
4. **与同源正常接入互斥、按序推进。** 重放与正常接入、结构变更共用同一把
   per-source 锁（`ingestion/locks.py`），同一数据源严格串行；**不同数据源互不
   阻塞**，某个数据源重放慢或失败不会卡住其他数据源的接入与重放。
5. **有界重试。** 重放过程中的存储层异常沿用现有分类：仅可恢复错误（如 SQLite
   瞬时锁定）按 `max_retries`/`backoff_seconds` 退避重试，不可恢复错误立即失败；
   单条重放在独立事务内提交，批次中前序条目成功不受后续失败影响。
6. **批次有界。** 单次重放条目数受 `max_replay_batch_size` 限制（默认 100），
   超过直接拒绝请求，避免一次性重放堆积。

结构变更类死信（`incompatible_schema_change`）同样可重放：用当前结构重新评估
兼容性，删列/改名永远不会被放行；运维把结构路径修正为纯新增列后，重放走与正常
结构变更相同的列注册与存量回填语义。

`POST /replay` 响应汇总每条结果：

```json
{
  "source": "users", "total": 2, "applied": 1, "rejected": 1, "skipped": 0,
  "attempts": 1,
  "items": [
    {"dead_letter_id": 3, "result": "applied", "reason": null, "detail": "", "attempts": 1},
    {"dead_letter_id": 4, "result": "rejected", "reason": "type_mismatch",
     "detail": "column 'name' expects str, got 7", "attempts": 1}
  ]
}
```

## 可配置项

`PipelineConfig`（见 `ingestion/pipeline.py`）：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `max_retries` | 3 | 可恢复错误的最大重试次数（有界，接入与重放共用） |
| `backoff_seconds` | 0.05 | 退避基数，按尝试次数线性增长 |
| `max_replay_batch_size` | 100 | 单次受控重放允许的失败记录条目上限（有界） |

服务数据文件路径通过环境变量 `INGESTION_DB` 配置（默认 `./ingestion.db`）。
旧版本数据库在首次连接时自动为 `dead_letters` 表补齐处置/重放列，无需手工迁移。

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest           # 运行全部测试（日志打印位点、变更流水与重放结果）
uv run pytest tests/test_concurrency.py -v   # 并发触发 / 互斥 / 有界重试
uv run pytest tests/test_replay.py -v        # 处置流转 / 受控重放 / 重放互斥
uv run python main.py   # 本地启动服务（127.0.0.1，随机端口）
```

### 手工验证受控重放（HTTP）

```bash
# 1) 初始结构
curl -s -XPOST localhost:8000/sources/users/init \
  -H 'content-type: application/json' \
  -d '{"columns": {"id": "int", "name": "str"}}'

# 2) 投一条坏数据（name 类型错误）-> 进入死信，位点仍推进到 1
curl -s -XPOST localhost:8000/sources/users/ingest \
  -H 'content-type: application/json' \
  -d '{"records": [{"position": 1, "op": "insert", "pk": {"id": 1},
                    "payload": {"id": 1, "name": 7}}]}'

# 3) 分页筛选失败记录，拿到 id
curl -s 'localhost:8000/dead-letters?source=users&disposition=pending'

# 4) 源端没修时重放 -> 仍被拒（rejected / type_mismatch），状态保持 pending
curl -s -XPOST localhost:8000/replay \
  -H 'content-type: application/json' -d '{"dead_letter_ids": [1]}'

# 5) 源端修好结构后再重放 -> applied，原死信变 resolved；位点始终为 1
curl -s -XPOST localhost:8000/replay \
  -H 'content-type: application/json' -d '{"dead_letter_ids": [1]}'

# 6) 查看可追溯的重放流水
curl -s localhost:8000/dead-letters/1/replays

# 人工标注处置状态（可随时重开为 pending）
curl -s -XPOST localhost:8000/dead-letters/1/disposition \
  -H 'content-type: application/json' \
  -d '{"disposition": "ignored", "note": "源端确认废弃，不再重放"}'
```

> 注：示例第 5 步以“结构修复”为例（如坏记录是缺列/多列，运维对齐目标结构后
> 原记录即合法）；数据型修复则由源端按正常接入补齐正确记录。重放只按**死信中
> 保存的原始记录**重新校验，不会臆造或覆盖数据。

测试覆盖：重复消费与重启恢复（`test_idempotency.py`）、位点回退与可区分失败原因
（`test_rejection.py`）、结构变更兼容与可追溯失败（`test_schema_evolution.py`）、
同一数据源并发触发 / 数据源间隔离 / 有界重试（`test_concurrency.py`）、
失败记录处置流转 / 重放成功与再拒 / 重复重放幂等 / 重放与接入互斥 /
源间隔离与有界重试 / 结构变更重放（`test_replay.py`）、HTTP 端到端协议
（`test_api.py`）。
