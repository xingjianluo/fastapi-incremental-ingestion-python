"""源端结构变更：新增列兼容（确定缺省），删列/改名/改类型拒绝且可追溯。"""

from __future__ import annotations

from app.errors import FailureReason
from app.models import ChangeEvent, IngestBatch, SchemaChange
from tests.conftest import make_event


def _schema(event_id, seq, columns):
    return SchemaChange(event_id=event_id, seq=seq, columns=columns)


def test_added_column_gets_stable_default(pipeline, storage):
    storage.register_source("s", {"name": "str", "age": "int"})
    pipeline.ingest(IngestBatch(source="s", events=[
        make_event("e1", 1, data={"name": "a", "age": 1}),
    ]))
    # 新增 city(str) / vip(bool)：存量行与缺字段的新事件都获得确定缺省值
    result = pipeline.ingest(IngestBatch(source="s", schema_changes=[
        _schema("sc1", 2, {"name": "str", "age": "int", "city": "str", "vip": "bool"}),
    ]))
    assert result.applied == 1 and storage.get_schema_version("s") == 2
    row = storage.list_rows("s")[0]
    assert row["city"] == "" and row["vip"] is False

    # 演进后到达的事件不提供新列 -> 使用同样缺省值；提供新列 -> 按新值写入
    pipeline.ingest(IngestBatch(source="s", events=[
        make_event("e3", 3, data={"name": "b", "age": 2}),
        make_event("e4", 4, data={"name": "c", "age": 3, "city": "X", "vip": True}),
    ]))
    rows = {r["pk"]: r for r in storage.list_rows("s")}
    assert rows["e3"]["city"] == "" and rows["e3"]["vip"] is False
    assert rows["e4"]["city"] == "X" and rows["e4"]["vip"] is True
    # 结构变更本身进入流水，重投可幂等
    again = pipeline.ingest(IngestBatch(source="s", schema_changes=[
        _schema("sc1", 2, {"name": "str", "age": "int", "city": "str", "vip": "bool"}),
    ]))
    assert again.duplicates == 1 and again.applied == 0


def test_dropped_column_is_incompatible(pipeline, storage):
    storage.register_source("s", {"name": "str", "age": "int"})
    pipeline.ingest(IngestBatch(source="s", events=[make_event("e1", 1)]))
    result = pipeline.ingest(IngestBatch(source="s", schema_changes=[
        _schema("sc-bad", 2, {"name": "str"}),  # 删除 age
    ]))
    assert result.status == "failed"
    assert result.rejected[0].reason is FailureReason.SCHEMA_INCOMPATIBLE
    assert "删除列" in result.rejected[0].detail
    # 原始变更记录完整保留在死信
    dead = storage.list_dead_letters("s")
    assert dead[0]["event_id"] == "sc-bad"
    assert '"columns"' in dead[0]["payload_json"]
    # 目标端结构未变，已有数据未受损
    assert storage.get_schema_version("s") == 1
    assert storage.get_columns("s") == {"name": "str", "age": "int"}
    assert storage.list_rows("s")[0]["age"] == 1


def test_renamed_column_treated_as_drop_plus_add(pipeline, storage):
    """改名在协议上表现为“删旧列+增新列”：删列即拒绝，不静默错列。"""
    storage.register_source("s", {"name": "str", "age": "int"})
    pipeline.ingest(IngestBatch(source="s", events=[make_event("e1", 1)]))
    result = pipeline.ingest(IngestBatch(source="s", schema_changes=[
        _schema("sc-rename", 2, {"name": "str", "years": "int"}),  # age -> years
    ]))
    assert result.rejected[0].reason is FailureReason.SCHEMA_INCOMPATIBLE
    assert "age" in result.rejected[0].detail
    assert storage.get_columns("s") == {"name": "str", "age": "int"}


def test_type_change_is_incompatible(pipeline, storage):
    storage.register_source("s", {"name": "str", "age": "int"})
    result = pipeline.ingest(IngestBatch(source="s", schema_changes=[
        _schema("sc-type", 1, {"name": "str", "age": "str"}),
    ]))
    assert result.rejected[0].reason is FailureReason.SCHEMA_INCOMPATIBLE
    assert storage.list_dead_letters("s")[0]["event_id"] == "sc-type"


def test_schema_change_then_data_in_same_batch(pipeline, storage):
    """同一批次内结构变更（seq=1）先于依赖新列的数据事件（seq=2）应用。"""
    storage.register_source("s", {"name": "str"})
    result = pipeline.ingest(IngestBatch(source="s", events=[
        ChangeEvent(event_id="e1", seq=2, op="insert", pk="k1",
                    data={"name": "a", "city": "NY"}),
    ], schema_changes=[
        _schema("sc1", 1, {"name": "str", "city": "str"}),
    ]))
    assert result.applied == 2 and result.rejected == []
    row = storage.list_rows("s")[0]
    assert row["city"] == "NY"
