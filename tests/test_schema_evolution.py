"""源端结构变更：新增列缺省语义稳定确定；删列/改名可追溯地失败。"""
import logging

from ingestion.models import ChangeRecord, FailureReason, Op, SchemaChange

logger = logging.getLogger("test.schema")


def _rec(pos, pk_id, **extra):
    payload = {"id": pk_id, "name": f"n{pk_id}"}
    payload.update(extra)
    return ChangeRecord("users", pos, Op.INSERT, {"id": pk_id}, payload)


def test_add_column_backfills_stable_default(store, schema, pipeline):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    pipeline.ingest("users", [_rec(1, 1), _rec(2, 2)])

    pipeline.apply_schema_change(SchemaChange("users", 3, to_version=2,
                                              add_columns={"age": "int"}))
    rows = store.get_rows("users")
    logger.info("after add_column: rows=%s checkpoint=%d", rows,
                store.get_checkpoint("users"))
    # 存量行获得稳定确定的缺省值
    assert all(r["age"] == 0 for r in rows)
    assert store.get_columns("users")["age"] == "int"

    # 新记录必须携带新列，缺列会被明确拒绝而非静默错列
    r = pipeline.ingest("users", [_rec(4, 4, age=18),
                                  ChangeRecord("users", 5, Op.INSERT, {"id": 5},
                                               {"id": 5, "name": "n5"})])
    assert r.applied == 1
    assert r.rejected[0].reason is FailureReason.MISSING_FIELD
    assert store.get_rows("users")[-1]["age"] == 18


def test_drop_and_rename_are_traceable_failures(store, schema, pipeline):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    pipeline.ingest("users", [_rec(1, 1)])

    pipeline.apply_schema_change(SchemaChange("users", 2, to_version=2,
                                              drop_columns=("name",)))
    pipeline.apply_schema_change(SchemaChange(
        "users", 3, to_version=3, rename_columns={"name": "full_name"}))

    rejections = store.list_rejections("users")
    for rej in rejections:
        logger.info("schema rejection: %s", rej)
    # 原始变更记录被保留并标记为可追溯的失败
    assert [r["reason"] for r in rejections] == [
        "incompatible_schema_change", "incompatible_schema_change"]
    assert rejections[0]["record"]["drop_columns"] == ["name"]
    assert rejections[1]["record"]["rename_columns"] == {"name": "full_name"}
    # 目标 schema 未被静默修改，位点正常推进
    assert store.get_columns("users") == {"id": "int", "name": "str"}
    assert store.get_checkpoint("users") == 3


def test_schema_change_is_idempotent(store, schema, pipeline):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    change = SchemaChange("users", 1, to_version=2, add_columns={"age": "int"})
    pipeline.apply_schema_change(change)
    pipeline.apply_schema_change(change)  # 重复投递同一变更
    assert store.get_columns("users") == {"age": "int", "id": "int", "name": "str"}
    assert store.get_checkpoint("users") == 1
    assert store.list_rejections("users") == []
