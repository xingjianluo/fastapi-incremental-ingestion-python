"""位点回退与记录级拒绝：失败原因必须可区分，且不得静默跳过。"""

from __future__ import annotations

import pytest

from app.errors import FailureReason
from app.models import IngestBatch
from tests.conftest import make_event


def test_watermark_regression_is_rejected(pipeline, storage):
    storage.register_source("s", {"name": "str", "age": "int"})
    pipeline.ingest(IngestBatch(source="s", events=[make_event("e1", 1), make_event("e2", 2)]))

    # 新事件但 seq 回退到旧位点 -> WATERMARK_REGRESSION
    result = pipeline.ingest(IngestBatch(source="s", events=[make_event("e-old", 1)]))
    assert result.status == "partial"
    rej = result.rejected[0]
    assert rej.reason is FailureReason.WATERMARK_REGRESSION
    assert "位点回退" in rej.detail
    # 原始记录进入死信，可追溯
    dead = storage.list_dead_letters("s")
    assert dead[-1]["reason"] == "WATERMARK_REGRESSION"
    # 目标数据不受影响
    assert len(storage.list_rows("s")) == 2


def test_pk_conflict_is_rejected(pipeline, storage):
    storage.register_source("s", {"name": "str", "age": "int"})
    pipeline.ingest(IngestBatch(
        source="s", events=[make_event("seed", 1, pk="dup", data={"name": "a", "age": 1})]
    ))
    result = pipeline.ingest(IngestBatch(
        source="s", events=[make_event("c", 2, pk="dup", data={"name": "b", "age": 2})]
    ))
    assert result.rejected[0].reason is FailureReason.PK_CONFLICT
    assert storage.list_dead_letters("s")[0]["reason"] == "PK_CONFLICT"
    # 原行保持不变
    row = [r for r in storage.list_rows("s") if r["pk"] == "dup"][0]
    assert row["name"] == "a" and row["age"] == 1


@pytest.mark.parametrize(
    "event,expected",
    [
        (make_event("m", 1, data={"name": "x"}),
         FailureReason.MISSING_FIELD),
        (make_event("t", 2, data={"name": "x", "age": "not-a-number"}),
         FailureReason.TYPE_MISMATCH),
        (make_event("t2", 3, data={"name": "x", "age": True}),
         FailureReason.TYPE_MISMATCH),  # bool 不被当作 int
        (make_event("u", 4, op="update", pk="ghost", data={"name": "x"}),
         FailureReason.UNKNOWN_PK),
    ],
)
def test_distinguishable_rejection_reasons(pipeline, storage, event, expected):
    storage.register_source("s", {"name": "str", "age": "int"})
    result = pipeline.ingest(IngestBatch(source="s", events=[event]))
    assert result.rejected[0].reason is expected
    assert storage.list_dead_letters("s")[0]["reason"] == expected.value


def test_missing_pk_rejected(pipeline, storage):
    from app.models import ChangeEvent

    storage.register_source("s", {"name": "str"})
    result = pipeline.ingest(IngestBatch(source="s", events=[ChangeEvent(
        event_id="e", seq=1, op="insert", pk=None, data={"name": "a"}
    )]))
    assert result.rejected[0].reason is FailureReason.MISSING_FIELD


def test_watermark_monotonic_after_rejections(pipeline, storage):
    """连续坏记录进死信后，水位仍单调推进，坏记录不会被无限重投。"""
    storage.register_source("s", {"name": "str", "age": "int"})
    result = pipeline.ingest(IngestBatch(source="s", events=[
        make_event("e1", 1, data={"name": 1, "age": 1}),   # type mismatch
        make_event("e2", 2, data={"name": "x"}),            # missing field
    ]))
    assert len(result.rejected) == 2
    assert [r.reason for r in result.rejected] == [
        FailureReason.TYPE_MISMATCH, FailureReason.MISSING_FIELD
    ]
    assert storage.get_watermark("s") == 2
    # 同样的坏记录再投：因水位推进，原位点被识别为位点回退而非反复做字段校验
    again = pipeline.ingest(IngestBatch(source="s", events=[
        make_event("e1", 1, data={"name": 1, "age": 1}),
    ]))
    assert again.rejected[0].reason is FailureReason.WATERMARK_REGRESSION
