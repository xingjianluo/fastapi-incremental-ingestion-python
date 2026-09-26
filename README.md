# 本地增量数据接入管线

基于 FastAPI + SQLite（标准库）的本地增量数据接入服务。在**重复消费、并发触发、
源端结构变更**下保证目标数据状态一致且可解释：不重、不丢、失败原因可区分、
全程留有可审计的变更流水。

仅依赖本地存储与仓库已有依赖，不引入外部服务或真实凭据。

## 快速开始

```bash
uv sync                 # 安装依赖（含 dev 组的 pytest）
uv run python main.py   # 启动服务，默认 127.0.0.1:8000，文档见 /docs
```

## 接入协议

### 1. 注册数据源

```
POST /sources/{source}/register
{"columns": {"name": "str", "age": "int", "vip": "bool"}}
```

- 支持列类型：`int` / `float` / `str` / `bool`。
- 注册时的列即**必填列**：之后的 insert 事件必须全部提供，缺失即拒绝。
- 重复注册同名数据源为幂等操作（结构以首次注册为准）。

### 2. 投递变更批次

```
POST /sources/{source}/ingest
{
  "source": "users",
  "events": [
    {"event_id": "e1", "seq": 1, "op": "insert", "pk": "u1",
     "data": {"name": "a", "age": 1, "vip": true}},
    {"event_id": "e2", "seq": 2, "op": "update", "pk": "u1", "data": {"age": 2}},
    {"event_id": "e3", "seq": 3, "op": "delete", "pk": "u2"}
  ],
  "schema_changes": [
    {"event_id": "sc1", "seq": 4,
     "columns": {"name": "str", "age": "int", "vip": "bool", "city": "str"}}
  ]
}
```

- `event_id`：全局唯一事件标识，跨重启去重的依据。
- `seq`：数据源内单调递增的位点；**一个位点对应一个条目**（数据事件或结构变更）。
- `op`：`insert`（幂等键冲突即拒绝）/ `update`（部分列，目标必须存在）/
  `delete`（目标必须存在）。
- 批次内数据事件与结构变更按 `seq` 升序混排应用。

### 3. 查询

| 端点 | 说明 |
| --- | --- |
| `GET /sources` | 已注册数据源列表 |
| `GET /sources/{source}/state` | 位点（watermark）、结构版本、应用/死信计数 |
| `GET /sources/{source}/rows` | 目标表当前数据 |
| `GET /sources/{source}/events` | 已应用变更流水（含结构变更） |
| `GET /sources/{source}/dead-letters` | 失败记录（含原因、详情、原始载荷） |

## 位点与并发语义

### 位点（watermark）

- 每个数据源维护单调递增水位 = 已处理的最大 `seq`。
- **重复消费**：`event_id` 已在流水中的事件幂等跳过，计入 `duplicates`，
  不产生任何写入。
- **位点回退**：未见过的事件但 `seq <= watermark`，拒绝
  （`WATERMARK_REGRESSION`）并写入死信，绝不静默跳过。
- **原子性**：每条事件的“行写入 + 流水登记 + 水位推进”在单个 SQLite
  事务内提交；进程崩溃重启后由唯一约束 `(source, event_id)` 与水位保证
  不重不丢。
- **死信推进水位**：被拒绝的记录写入死信的同时水位越过其位点，避免毒消息
  造成无限重投；修复后的记录必须由源端以**新位点**重新投递。

### 并发

- 同一数据源的任意触发在**进程内锁 + SQLite 持久租约**下全局互斥，
  先到先得；等待超过 `INGEST_LOCK_TIMEOUT` 秒以 `LOCK_TIMEOUT`
  （可恢复错误）失败，由调用方重试。
- 不同数据源使用独立的锁与租约行，互不阻塞；单个数据源的失败与重试
  （退避发生在锁外）不影响其他数据源。
- 租约带过期时间，持有者进程崩溃后租约可被接管，不会永久死锁。
- 重试仅针对**可恢复错误**（`TRANSIENT` / `LOCK_TIMEOUT`），指数退避，
  次数受 `INGEST_MAX_RETRIES` 限制，杜绝无界重试与资源耗尽；
  记录级/结构级错误均为不可恢复，直接拒绝。

### 可区分的失败原因

| reason | 含义 | 可恢复 |
| --- | --- | --- |
| `WATERMARK_REGRESSION` | 位点回退：seq 不高于当前水位 | 否 |
| `PK_CONFLICT` | insert 命中已存在主键 | 否 |
| `MISSING_FIELD` | 必填字段/主键缺失，或引用了不存在的列 | 否 |
| `TYPE_MISMATCH` | 字段类型不匹配且不可安全转换（bool 不视为 int） | 否 |
| `SCHEMA_INCOMPATIBLE` | 不兼容结构变更（删列/改名/改类型） | 否 |
| `UNKNOWN_SOURCE` | 数据源未注册 | 否 |
| `UNKNOWN_PK` | update/delete 目标主键不存在 | 否 |
| `LOCK_TIMEOUT` | 并发触发等待互斥锁超时 | 是 |
| `TRANSIENT` | 存储层临时错误（如 SQLITE_BUSY） | 是 |

所有被拒绝的记录都写入死信队列（`dead_letters` 表），携带原因、详情与
原始载荷，可查询、可审计。

## 结构兼容规则

- **新增列：兼容**。目标表执行 `ALTER TABLE ADD COLUMN` 并带确定缺省值：
  `int→0`、`float→0.0`、`str→''`、`bool→false`。存量行与未提供该列的新事件
  都使用同一缺省值，语义稳定确定。
- **删除列：不兼容**。目标端已有数据列不允许静默丢弃。
- **改名：按“删旧列 + 增新列”判定**，删列部分即不兼容——杜绝目标端静默错列。
- **列类型变更：不兼容**。
- 不兼容变更：整批中止，**原始变更记录完整写入死信**（`SCHEMA_INCOMPATIBLE`），
  已应用部分保持提交，水位停在最后一个成功位点，结构版本不变。

## 可配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `INGEST_DB_PATH` | `ingestion.db` | SQLite 存储路径 |
| `INGEST_LOCK_TIMEOUT` | `5.0` | 同一数据源互斥锁等待上限（秒） |
| `INGEST_MAX_RETRIES` | `3` | 可恢复错误最大重试次数 |
| `INGEST_RETRY_BACKOFF` | `0.05` | 重试退避基数（秒，指数增长） |

## 本地验证

```bash
uv run pytest tests/ -q        # 全部用例
uv run pytest tests/ -q -s     # 带位点与变更流水打印（每个用例结束自动输出）
```

测试覆盖：

- `test_duplicates_and_restart.py`：重复消费幂等、部分失败后重投、
  进程重启后状态恢复（位点/数据/流水/死信）。
- `test_watermark_and_rejections.py`：位点回退拒绝、主键冲突、字段缺失、
  类型不匹配、未知主键——原因逐一可区分，水位单调推进。
- `test_schema_evolution.py`：新增列确定缺省、删列/改名/改类型拒绝并
  保留原始变更记录、同批次结构变更先于依赖它的数据应用。
- `test_concurrency.py`：同一数据源 8 线程并发重复触发恰好一次生效、
  失败数据源的重试不阻塞其他数据源、锁超时为可恢复错误、
  可恢复错误有界重试。
- `test_api.py`：HTTP 层端到端流程与错误响应。

## 代码结构

```
app/
  config.py            # 可配置项（环境变量）
  errors.py            # 失败原因枚举与异常层级
  models.py            # 接入协议模型（事件/批次/结果）
  schema_evolution.py  # 结构变更兼容性判定与缺省值
  validation.py        # 记录级校验（主键/必填列/类型）
  storage.py           # SQLite 原子存储（线程本地连接 + 单事务写入）
  locker.py            # 数据源互斥（进程内锁 + SQLite 租约）
  pipeline.py          # 编排：位点校验、幂等、校验、演进、有界重试
  service.py           # FastAPI 端点
main.py                # 服务入口
tests/                 # 上述全部场景的测试
```
