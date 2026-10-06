"""失败记录处置状态流转与受控重放：幂等、再拒绝、并发互斥、位点不变。"""
import logging
import threading

import pytest

from ingestion.models import (
    ChangeRecord,
    DeadLetterStatus,
    FailureReason,
    Op,
    ReplayOutcome,
)
from ingestion.pipeline import IngestionPipeline, PipelineConfig

logger = logging.getLogger("test.replay")


def _rec(pos, pk_id=1, payload=None, op=Op.INSERT, source="users"):
    return ChangeRecord(source, pos, op, {"id": pk_id} if pk_id is not None else {},
                        payload if payload is not None else {"id": 1, "name": "a"})


def _make_dead_letter(schema, pipeline, source="users"):
    """制造一条类型不匹配的死信，返回其 id。"""
    schema.ensure_source(source, {"id": "int", "name": "str"})
    r = pipeline.ingest(source, [
        _rec(1, 1, {"id": 1, "name": 123}, source=source)])  # 类型不匹配
    assert r.rejected and r.rejected[0].reason is FailureReason.TYPE_MISMATCH
    letters, total = pipeline._store.list_dead_letters(source=source)
    assert total == 1
    return letters[0].id


def test_dead_letter_status_transitions(store, schema, pipeline):
    """待处理 -> 已解决/已忽略：处置时间与说明可追溯，可按条件筛选分页。"""
    dl_id = _make_dead_letter(schema, pipeline)
    dl = store.get_dead_letter(dl_id)
    assert dl.status is DeadLetterStatus.PENDING
    assert dl.reason is FailureReason.TYPE_MISMATCH

    dl = store.update_dead_letter_status(dl_id, DeadLetterStatus.IGNORED,
                                         "源端确认该记录作废")
    logger.info("dead_letter=%d status=%s updated_at=%s note=%s",
                dl.id, dl.status, dl.updated_at, dl.resolution_note)
    assert dl.status is DeadLetterStatus.IGNORED
    assert dl.updated_at and dl.resolution_note == "源端确认该记录作废"

    dl = store.update_dead_letter_status(dl_id, DeadLetterStatus.RESOLVED, "ok")
    assert dl.status is DeadLetterStatus.RESOLVED

    # 按状态筛选
    items, total = store.list_dead_letters(
        source="users", status=DeadLetterStatus.RESOLVED)
    assert total == 1 and items[0].id == dl_id
    items, total = store.list_dead_letters(
        source="users", status=DeadLetterStatus.PENDING)
    assert total == 0
    # 按原因筛选
    items, total = store.list_dead_letters(
        source="users", reason=FailureReason.TYPE_MISMATCH)
    assert total == 1
    items, total = store.list_dead_letters(
        source="users", reason=FailureReason.MISSING_FIELD)
    assert total == 0
    assert store.update_dead_letter_status(9999, DeadLetterStatus.RESOLVED) is None


def test_dead_letter_pagination(store, schema, pipeline):
    """分页查询不一次性全量读入。"""
    schema.ensure_source("users", {"id": "int", "name": "str"})
    pipeline.ingest("users", [
        _rec(p, p, {"id": p, "name": 1}) for p in range(1, 8)])  # 7 条全拒
    page1, total = store.list_dead_letters(source="users", limit=3, offset=0)
    page2, total2 = store.list_dead_letters(source="users", limit=3, offset=3)
    page3, _ = store.list_dead_letters(source="users", limit=3, offset=6)
    assert total == total2 == 7
    assert [d.position for d in page1] == [1, 2, 3]
    assert [d.position for d in page2] == [4, 5, 6]
    assert [d.position for d in page3] == [7]
    logger.info("pagination total=%d pages=%d/%d/%d",
                total, len(page1), len(page2), len(page3))


def test_replay_applies_fixed_record(store, schema, pipeline):
    """源端数据修好后重放：走同一套校验与写入，位点不变，结果可追溯。"""
    dl_id = _make_dead_letter(schema, pipeline)
    checkpoint_before = store.get_checkpoint("users")
    # 修复源端数据（把 name 改成合法字符串）后重放
    dl = store.get_dead_letter(dl_id)
    dl.record["payload"]["name"] = "fixed"
    store.connect().execute(
        "UPDATE dead_letters SET record = ? WHERE id = ?",
        (__import__("json").dumps(dl.record, ensure_ascii=False), dl_id))

    result = pipeline.replay([dl_id])
    item = result.results[0]
    logger.info("replay dead_letter=%d position=%d outcome=%s checkpoint=%d->%d",
                dl_id, item.position, item.outcome,
                checkpoint_before, store.get_checkpoint("users"))
    assert item.outcome is ReplayOutcome.APPLIED
    assert store.get_rows("users") == [{"id": 1, "name": "fixed"}]
    # 位点不因重放而变化
    assert store.get_checkpoint("users") == checkpoint_before
    # 原死信上能看到最终结果
    dl = store.get_dead_letter(dl_id)
    assert dl.status is DeadLetterStatus.RESOLVED
    assert dl.replay_outcome is ReplayOutcome.APPLIED
    assert dl.replay_attempts == 1 and dl.replayed_at
    history = store.list_replay_history(dl_id)
    assert [h["outcome"] for h in history] == ["applied"]


def test_replay_is_idempotent(store, schema, pipeline):
    """同一条重复重放：不产生重复数据，状态不混乱。"""
    dl_id = _make_dead_letter(schema, pipeline)
    dl = store.get_dead_letter(dl_id)
    dl.record["payload"]["name"] = "fixed"
    store.connect().execute(
        "UPDATE dead_letters SET record = ? WHERE id = ?",
        (__import__("json").dumps(dl.record, ensure_ascii=False), dl_id))

    r1 = pipeline.replay([dl_id])
    r2 = pipeline.replay([dl_id])
    r3 = pipeline.replay([dl_id, dl_id])  # 同批内重复 id
    logger.info("replay outcomes: %s / %s / %s",
                [i.outcome for i in r1.results],
                [i.outcome for i in r2.results],
                [i.outcome for i in r3.results])
    assert r1.results[0].outcome is ReplayOutcome.APPLIED
    assert r2.results[0].outcome is ReplayOutcome.ALREADY_APPLIED
    assert all(i.outcome is ReplayOutcome.ALREADY_APPLIED for i in r3.results)
    assert store.get_rows("users") == [{"id": 1, "name": "fixed"}]  # 无重复
    dl = store.get_dead_letter(dl_id)
    assert dl.replay_attempts == 1  # 后续重放未重复写入
    assert dl.status is DeadLetterStatus.RESOLVED


def test_replay_still_rejected_with_reason(store, schema, pipeline):
    """数据没修好就重放：仍被明确拒绝，原因可区分，死信保持待处理。"""
    dl_id = _make_dead_letter(schema, pipeline)  # name=123 类型不匹配，未修复
    result = pipeline.replay([dl_id])
    item = result.results[0]
    logger.info("replay rejected: reason=%s detail=%s", item.reason, item.detail)
    assert item.outcome is ReplayOutcome.REJECTED
    assert item.reason == FailureReason.TYPE_MISMATCH.value
    assert store.get_rows("users") == []  # 未写入
    dl = store.get_dead_letter(dl_id)
    assert dl.status is DeadLetterStatus.PENDING  # 仍需处置
    assert dl.replay_outcome is ReplayOutcome.REJECTED
    assert dl.replay_reason == FailureReason.TYPE_MISMATCH.value
    assert dl.replay_attempts == 1
    # 重放拒绝不会产生新的死信记录
    _, total = store.list_dead_letters(source="users")
    assert total == 1


def test_replay_never_overwrites_existing_row(store, schema, pipeline):
    """重放 INSERT 命中已存在主键：明确拒绝，绝不悄悄覆盖。"""
    schema.ensure_source("users", {"id": "int", "name": "str"})
    pipeline.ingest("users", [_rec(1, None)])  # 主键缺失 -> 死信
    dl_id = store.list_dead_letters(source="users")[0][0].id
    # 后续正常接入把 id=1 写进去了
    pipeline.ingest("users", [_rec(2, 1, {"id": 1, "name": "real"})])
    # 把死信记录改成 INSERT id=1，重放应撞主键冲突
    dl = store.get_dead_letter(dl_id)
    dl.record["pk"] = {"id": 1}
    dl.record["payload"] = {"id": 1, "name": "replay"}
    store.connect().execute(
        "UPDATE dead_letters SET record = ? WHERE id = ?",
        (__import__("json").dumps(dl.record, ensure_ascii=False), dl_id))
    result = pipeline.replay([dl_id])
    item = result.results[0]
    logger.info("replay conflict: outcome=%s reason=%s", item.outcome, item.reason)
    assert item.outcome is ReplayOutcome.REJECTED
    assert item.reason == FailureReason.PRIMARY_KEY_CONFLICT.value
    assert store.get_rows("users") == [{"id": 1, "name": "real"}]  # 未被覆盖


def test_replay_not_found_and_batch_limit(store, schema, pipeline):
    dl_id = _make_dead_letter(schema, pipeline)
    result = pipeline.replay([9999])
    assert result.results[0].outcome is ReplayOutcome.NOT_FOUND
    small = IngestionPipeline(store, schema,
                              PipelineConfig(max_retries=1, backoff_seconds=0.01,
                                             max_replay_batch=1))
    with pytest.raises(ValueError, match="max_replay_batch"):
        small.replay([dl_id, 9999])


def test_replay_and_ingest_mutual_exclusion(store, schema, pipeline):
    """重放与正常接入并发：同一数据源互斥，无重复写入、状态不混乱。"""
    schema.ensure_source("users", {"id": "int", "name": "str"})
    # 位点 1 类型不匹配进死信；位点 2..N 后续正常接入
    pipeline.ingest("users", [_rec(1, 1, {"id": 1, "name": 1})])
    dl_id = store.list_dead_letters(source="users")[0][0].id
    dl = store.get_dead_letter(dl_id)
    dl.record["payload"]["name"] = "fixed"
    store.connect().execute(
        "UPDATE dead_letters SET record = ? WHERE id = ?",
        (__import__("json").dumps(dl.record, ensure_ascii=False), dl_id))

    barrier = threading.Barrier(2)
    errors, outcomes = [], {}

    def do_ingest():
        try:
            barrier.wait()
            outcomes["ingest"] = pipeline.ingest("users", [
                _rec(p, p, {"id": p, "name": f"n{p}"}) for p in range(2, 30)])
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    def do_replay():
        try:
            barrier.wait()
            outcomes["replay"] = pipeline.replay([dl_id])
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=do_ingest),
               threading.Thread(target=do_replay)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    rows = store.get_rows("users")
    logger.info("concurrent replay+ingest: rows=%d checkpoint=%d replay=%s",
                len(rows), store.get_checkpoint("users"),
                outcomes["replay"].results[0].outcome)
    assert outcomes["replay"].results[0].outcome is ReplayOutcome.APPLIED
    assert outcomes["ingest"].applied == 28
    assert len(rows) == 29  # 重放 1 条 + 正常接入 28 条，无重复无丢失
    assert {"id": 1, "name": "fixed"} in rows
    assert store.get_checkpoint("users") == 29  # 位点只被正常接入推进
    dl = store.get_dead_letter(dl_id)
    assert dl.status is DeadLetterStatus.RESOLVED and dl.replay_attempts == 1


def test_replay_isolated_across_sources(store, schema, pipeline):
    """某个数据源的重放不影响其他数据源的接入与重放。"""
    schema.ensure_source("users", {"id": "int", "name": "str"})
    schema.ensure_source("orders", {"id": "int", "name": "str"})
    pipeline.ingest("users", [_rec(1, 1, {"id": 1, "name": 1}, source="users")])
    pipeline.ingest("orders", [_rec(1, 1, {"id": 1, "name": 1}, source="orders")])
    users_dl = store.list_dead_letters(source="users")[0][0].id
    orders_dl = store.list_dead_letters(source="orders")[0][0].id
    # 只修 orders 的记录
    dl = store.get_dead_letter(orders_dl)
    dl.record["payload"]["name"] = "ok"
    store.connect().execute(
        "UPDATE dead_letters SET record = ? WHERE id = ?",
        (__import__("json").dumps(dl.record, ensure_ascii=False), orders_dl))

    result = pipeline.replay([users_dl, orders_dl])  # 跨数据源批量重放
    by_id = {r.dead_letter_id: r for r in result.results}
    logger.info("cross-source replay: users=%s orders=%s",
                by_id[users_dl].outcome, by_id[orders_dl].outcome)
    assert by_id[users_dl].outcome is ReplayOutcome.REJECTED   # 未修复仍拒
    assert by_id[orders_dl].outcome is ReplayOutcome.APPLIED   # 已修复通过
    assert store.get_rows("orders") == [{"id": 1, "name": "ok"}]
    assert store.get_rows("users") == []
    # users 的重放失败不影响 orders 的位点与数据
    assert store.get_checkpoint("orders") == 1
