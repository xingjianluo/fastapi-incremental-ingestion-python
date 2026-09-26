"""位点回退、主键缺失/冲突、字段缺失、类型不匹配：必须明确拒绝且原因可区分。"""
import logging

from ingestion.models import ChangeRecord, FailureReason, Op

logger = logging.getLogger("test.rejection")


def _rec(pos, pk_id=1, payload=None, op=Op.INSERT):
    return ChangeRecord("users", pos, op, {"id": pk_id} if pk_id is not None else {},
                        payload if payload is not None else {"id": 1, "name": "a"})


def test_position_regression_rejected(store, schema, pipeline):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    pipeline.ingest("users", [_rec(1), _rec(2, 2, {"id": 2, "name": "b"})])
    # 手动把位点推进到 10，制造"位点回退"（旧位点且未应用过）
    store.connect().execute(
        "UPDATE checkpoints SET last_position = 10 WHERE source = 'users'")
    r = pipeline.ingest("users", [_rec(5, 5, {"id": 5, "name": "e"})])
    rej = r.rejected[0]
    logger.info("regression rejected: reason=%s detail=%s", rej.reason, rej.detail)
    assert rej.reason is FailureReason.POSITION_REGRESSION
    assert store.get_rows("users") == [
        {"id": 1, "name": "a"}, {"id": 2, "name": "b"}]  # 未写入
    assert store.list_rejections("users")[0]["reason"] == "position_regression"


def test_distinguishable_failure_reasons(store, schema, pipeline):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    pipeline.ingest("users", [_rec(1)])
    r = pipeline.ingest("users", [
        _rec(2, None),                                        # 主键缺失
        _rec(3, 1, {"id": 1, "name": "dup"}),                 # 主键冲突
        _rec(4, 4, {"id": 4, "name": 123}),                   # 类型不匹配
        _rec(5, 5, {"id": 5, "name": "e", "ghost": 1}),       # 未知列
        _rec(6, 6, {"id": 6}),                                # 字段缺失
    ])
    reasons = [rej.reason for rej in r.rejected]
    for rej in r.rejected:
        logger.info("rejected position=%d reason=%s", rej.position, rej.reason)
    assert reasons == [
        FailureReason.MISSING_PRIMARY_KEY,
        FailureReason.PRIMARY_KEY_CONFLICT,
        FailureReason.TYPE_MISMATCH,
        FailureReason.UNKNOWN_COLUMN,
        FailureReason.MISSING_FIELD,
    ]
    assert r.applied == 0
    assert len(store.get_rows("users")) == 1  # 全部拒绝，无静默写入
    # 死信表可按原因区分查询
    recorded = {x["reason"] for x in store.list_rejections("users")}
    assert recorded == {r.value for r in reasons}


def test_rejections_still_advance_checkpoint(store, schema, pipeline):
    """拒绝记录落死信并推进位点，避免同一坏记录被无界重投。"""
    schema.ensure_source("users", {"id": "int", "name": "str"})
    r = pipeline.ingest("users", [_rec(1, 1, {"id": 1, "name": 1})])
    assert r.rejected and store.get_checkpoint("users") == 1
    r2 = pipeline.ingest("users", [_rec(2, 2, {"id": 2, "name": "ok"})])
    assert r2.applied == 1 and store.get_checkpoint("users") == 2
