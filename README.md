# 本地增量数据接入管线

基于 FastAPI + SQLite 的本地增量数据接入服务：按数据源维护增量位点并应用变更，
在重复消费、并发触发与源端结构变更下保证目标数据状态一致且可解释。
仅依赖本地存储与开源依赖，不引入外部服务或真实凭据。

## 架构

```
main.py                 FastAPI 接入层（HTTP 协议入口）
ingestion/
  models.py             变更记录、位点、失败原因等核心类型
  store.py              SQLite 持久层：目标行 / 位点 / schema 注册表 / 死信表
  schema.py             结构变更兼容规则与缺省值语义
  pipeline.py           校验、幂等去重、按数据源互斥、有界重试
tests/                  重复消费 / 位点回退 / 结构变更 / 并发触发场景
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

## 可配置项

`PipelineConfig`（见 `ingestion/pipeline.py`）：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `max_retries` | 3 | 可恢复错误的最大重试次数（有界） |
| `backoff_seconds` | 0.05 | 退避基数，按尝试次数线性增长 |

服务数据文件路径通过环境变量 `INGESTION_DB` 配置（默认 `./ingestion.db`）。

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest           # 运行全部测试（日志打印位点与变更流水）
uv run pytest tests/test_concurrency.py -v   # 并发触发场景
uv run python main.py   # 本地启动服务（127.0.0.1，随机端口）
```

测试覆盖：重复消费与重启恢复（`test_idempotency.py`）、位点回退与可区分失败原因
（`test_rejection.py`）、结构变更兼容与可追溯失败（`test_schema_evolution.py`）、
同一数据源并发触发 / 数据源间隔离 / 有界重试（`test_concurrency.py`）、
HTTP 端到端协议（`test_api.py`）。
