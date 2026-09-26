"""重复消费与进程重启：目标数据不得重复或丢失。"""

from __future__ import annotations

from app.locker import SourceLocker
from app.models import IngestBatch
from app.pipeline import IngestionPipeline
from app.storage import Storage
from tests.conftest import make_event


def test_duplicate_delivery_is_idempotent(pipeline, storage):
    storage.register_source("s", {"name": "str", "age": "int"})
    batch = IngestBatch(source="s", events=[make_event("e1", 1), make_event("e2", 2)])

    first = pipeline.ingest(batch)
    assert first.applied == 2 and first.duplicates == 0

    # 同一批次原样重投（模拟 at-least-once 重复消费）
    second = pipeline.ingest(batch)
    assert second.applied == 0 and second.duplicates == 2

    rows = storage.list_rows("s")
    assert len(rows) == 2, "重复消费不得产生重复行"
    assert storage.get_watermark("s") == 2


def test_partial_retry_after_failure(pipeline, storage):
    """批次中途失败后重投：已应用事件幂等跳过，后续事件继续推进。"""
    storage.register_source("s", {"name": "str", "age": "int"})
    events = [
        make_event("e1", 1),
        make_event("bad", 2, data={"name": "x"}),  # 缺 age -> 拒绝
        make_event("e3", 3),
    ]
    result = pipeline.ingest(IngestBatch(source="s", events=events))
    assert result.applied == 2
    assert result.rejected[0].reason.value == "MISSING_FIELD"

    # 修复坏记录后以新位点重投（死信已占用 seq=2，语义见 README）：
    # e1/e3 去重，修复记录正常应用
    fixed = [
        make_event("e1", 1),
        make_event("bad-fixed", 4, data={"name": "x", "age": 9}),
        make_event("e3", 3),
    ]
    retry = pipeline.ingest(IngestBatch(source="s", events=fixed))
    assert retry.duplicates == 2 and retry.applied == 1
    assert len(storage.list_rows("s")) == 3
    assert storage.get_watermark("s") == 4


def test_restart_recovers_state(db_path, settings, storage, pipeline):
    """进程重启（重新打开同一库文件）后：位点、数据、流水、死信全部恢复。"""
    storage.register_source("s", {"name": "str", "age": "int"})
    pipeline.ingest(IngestBatch(source="s", events=[make_event("e1", 1), make_event("e2", 2)]))
    pipeline.ingest(IngestBatch(
        source="s", events=[make_event("bad", 3, data={"name": 1, "age": 2})]
    ))

    # 模拟重启：全新连接 / Storage / Locker / Pipeline 实例（WAL 允许新连接）
    storage2 = Storage(db_path)
    pipeline2 = IngestionPipeline(
        storage2, SourceLocker(db_path, settings.lock_timeout_seconds), settings
    )
    assert storage2.get_watermark("s") == 3
    assert len(storage2.list_rows("s")) == 2, "重启后数据不得丢失"
    assert len(storage2.list_dead_letters("s")) == 1, "重启后失败记录可追溯"

    # 重启后重复消费旧事件仍然幂等
    again = pipeline2.ingest(IngestBatch(source="s", events=[make_event("e1", 1)]))
    assert again.duplicates == 1 and again.applied == 0
    assert len(storage2.list_rows("s")) == 2
    storage2.close()
