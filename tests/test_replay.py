"""失败记录处置状态流转与受控重放。

覆盖：
- 单条定位、按原因/数据源/处置状态筛选 + 分页
- 处置状态流转（pending -> resolved/ignored -> reopen）
- 重放走与正常接入相同的校验：修好后成功、仍坏则按可区分原因再拒
- 重复重放幂等：不产生重复数据、不把状态搞乱，原记录上可追溯最终结果
- 重放不回退/不污染位点，INSERT 不覆盖已存在数据
- 重放与同源正常接入互斥；不同数据源互不阻塞；可恢复错误有界重试
"""
import logging
import sqlite3
import threading

import pytest

from ingestion.locks import SourceLockRegistry
from ingestion.models import (
    ChangeRecord,
    Disposition,
    FailureReason,
    Op,
    ReplayResult,
    SchemaChange,
)
from ingestion.replay import ReplayService

logger = logging.getLogger("test.replay")


@pytest.fixture()
def locks():
    return SourceLockRegistry()


@pytest.fixture()
def pipeline(store, schema, locks):
    from ingestion.pipeline import IngestionPipeline, PipelineConfig
    return IngestionPipeline(store, schema, PipelineConfig(max_retries=2,
                                                           backoff_seconds=0.01),
                             locks)


@pytest.fixture()
def replay_service(store, schema, pipeline, locks):
    return ReplayService(store, schema, pipeline._config, locks)


def _dead_ids(replay_service, source=None):
    return [d.id for d in replay_service.list_dead_letters(source=source)[0]]


def test_locate_filter_and_paginate(store, schema, pipeline, replay_service):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    schema.ensure_source("orders", {"id": "int", "name": "str"})
    pipeline.ingest("users", [
        ChangeRecord("users", 1, Op.INSERT, {"id": 1},
                     {"id": 1, "name": 7}),       # type_mismatch
        ChangeRecord("users", 2, Op.INSERT, {"id": 2},
                     {"id": 2, "name": "ok"}),     # applied
    ])
    pipeline.ingest("orders", [ChangeRecord("orders", 1, Op.INSERT, {"id": 9},
                                            {"id": 9, "name": 8})])

    # 单条定位
    dls, total = replay_service.list_dead_letters()
    assert total == 2
    dl = replay_service.get_dead_letter(dls[0].id)
    assert dl.source == "users" and dl.position == 1
    logger.info("located dead_letter id=%d reason=%s created=%s",
                dl.id, dl.reason, dl.created_at)

    # 按数据源 / 原因 / 处置状态筛选
    users_only, n = replay_service.list_dead_letters(source="users")
    assert n == 1 and users_only[0].source == "users"
    _, n = replay_service.list_dead_letters(reason="type_mismatch")
    assert n == 2
    _, n = replay_service.list_dead_letters(disposition="pending")
    assert n == 2
    _, n = replay_service.list_dead_letters(disposition="resolved")
    assert n == 0

    # 分页：limit=1 两页读完，不会一次载入全部
    page1, total = replay_service.list_dead_letters(limit=1, offset=0)
    page2, _ = replay_service.list_dead_letters(limit=1, offset=1)
    assert total == 2 and len(page1) == 1 and len(page2) == 1
    assert page1[0].id != page2[0].id


def test_disposition_lifecycle_with_time_and_note(store, schema, pipeline,
                                                  replay_service):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    r = pipeline.ingest("users", [ChangeRecord(
        "users", 1, Op.INSERT, {"id": 1}, {"id": 1, "name": 7})])
    dl_id = r.rejected[0] and _dead_ids(replay_service, "users")[0]
    dl = replay_service.get_dead_letter(dl_id)
    assert dl.disposition is Disposition.PENDING and dl.handled_at is None

    marked = replay_service.mark(dl_id, Disposition.IGNORED, "源端不会修，忽略")
    assert marked.disposition is Disposition.IGNORED
    assert marked.handled_at and marked.note == "源端不会修，忽略"
    logger.info("marked id=%d at=%s note=%s",
                marked.id, marked.handled_at, marked.note)

    # 重新打开（仍带处置时间）
    reopened = replay_service.mark(dl_id, Disposition.PENDING, "源端答应修了")
    assert reopened.disposition is Disposition.PENDING
    assert reopened.handled_at is not None


def test_replay_applied_after_structure_fixed(store, schema, pipeline,
                                              replay_service):
    """源端结构修好后重放：相同校验通过并写入，原死信 resolved、位点不动。"""
    schema.ensure_source("users", {"id": "int", "name": "str"})
    # 缺必填字段 name 被拒；位点照常推进到 1
    r = pipeline.ingest("users", [ChangeRecord(
        "users", 1, Op.INSERT, {"id": 1}, {"id": 1})])
    dl_id = _dead_ids(replay_service, "users")[0]
    assert r.rejected[0].reason is FailureReason.MISSING_FIELD
    logger.info("checkpoint before replay=%d rows=%s",
                store.get_checkpoint("users"), store.get_rows("users"))

    # 源端结构修复：目标不再要求 name，原记录随之合法（模拟运维确认后的结构对齐）
    store.set_columns("users", {"id": "int"})

    result = replay_service.replay([dl_id])
    item = result.items[0]
    logger.info("replay result=%s reason=%s attempts=%d",
                item.result, item.reason, item.attempts)
    assert item.result is ReplayResult.APPLIED and result.applied == 1
    assert store.get_rows("users") == [{"id": 1}]

    # 原死信上能看到最终成功，并有流水可追溯
    dl = replay_service.get_dead_letter(dl_id)
    assert dl.disposition is Disposition.RESOLVED
    assert dl.replay_state.value == "applied"
    assert dl.replay_attempts == 1 and dl.handled_at
    assert [e["result"] for e in replay_service.list_replay_events(dl_id)] == [
        "applied"]
    # 位点没有被倒回或改写
    assert store.get_checkpoint("users") == 1
    # applied_positions 流水也没有被重放污染
    assert store.is_position_applied("users", 1)


def test_replay_rejected_again_with_distinguishable_reason(
        store, schema, pipeline, replay_service):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    pipeline.ingest("users", [ChangeRecord(
        "users", 1, Op.INSERT, {"id": 1}, {"id": 1, "name": 7})])
    dl_id = _dead_ids(replay_service, "users")[0]

    # 源端没修，重放必须仍被明确拒，原因可区分
    result = replay_service.replay([dl_id])
    item = result.items[0]
    assert item.result is ReplayResult.REJECTED
    assert item.reason is FailureReason.TYPE_MISMATCH
    assert "expects str" in item.detail

    refreshed = replay_service.get_dead_letter(dl_id)
    assert refreshed.disposition is Disposition.PENDING
    assert refreshed.replay_state.value == "rejected"
    assert refreshed.replay_reason == "type_mismatch"
    assert refreshed.replay_attempts == 1
    assert store.get_rows("users") == []
    assert [e["result"] for e in replay_service.list_replay_events(dl_id)] == [
        "rejected"]
    logger.info("re-rejected reason=%s detail=%s checkpoint=%d",
                item.reason, item.detail, store.get_checkpoint("users"))


def test_duplicate_replay_is_idempotent(store, schema, pipeline, replay_service):
    """成功重放后重复重放：跳过、不产生重复数据/流水、不改位点与状态。"""
    schema.ensure_source("users", {"id": "int", "name": "str"})
    pipeline.ingest("users", [ChangeRecord(
        "users", 1, Op.INSERT, {"id": 1}, {"id": 1})])  # 缺 name -> 死信
    dl_id = _dead_ids(replay_service, "users")[0]
    store.set_columns("users", {"id": "int"})  # 源端修好结构

    first = replay_service.replay([dl_id])
    assert first.applied == 1
    rows_after_first = store.get_rows("users")

    # 连续重复重放（含同一批内重复 id）：全部幂等跳过
    second = replay_service.replay([dl_id, dl_id])
    third = replay_service.replay([dl_id])
    assert (second.applied, second.skipped) == (0, 1)
    assert (third.applied, third.skipped) == (0, 1)
    assert store.get_rows("users") == rows_after_first  # 无重复写入

    dl = replay_service.get_dead_letter(dl_id)
    assert dl.disposition is Disposition.RESOLVED
    assert dl.replay_attempts == 1  # 跳过不计尝试
    assert len(replay_service.list_replay_events(dl_id)) == 1  # 只一次实际执行
    assert store.get_checkpoint("users") == 1  # 位点不变
    logger.info("duplicate replay idempotent: rows=%s events=1 checkpoint=%d",
                rows_after_first, store.get_checkpoint("users"))


def test_replay_insert_does_not_overwrite_existing(store, schema, pipeline,
                                                   replay_service):
    """重放与正常接入同写入语义：目标主键已存在时不得悄悄覆盖。"""
    schema.ensure_source("users", {"id": "int", "name": "str"})
    # 正常落地 id=1 name="real"（位点 1）
    pipeline.ingest("users", [ChangeRecord(
        "users", 1, Op.INSERT, {"id": 1}, {"id": 1, "name": "real"})])
    # 位点 2：另一条同主键 INSERT，因 name 类型错误被拒（记录了坏值 "stale"）
    pipeline.ingest("users", [ChangeRecord(
        "users", 2, Op.INSERT, {"id": 1}, {"id": 1, "name": 9})])
    dl_id = _dead_ids(replay_service, "users")[0]
    # 把坏值改成类型正确、但仍是同主键 INSERT 的陈旧内容：
    # 直接改写死信保存的原始记录（等价于源端修正了类型但主键冲突仍在）
    conn = store.connect()
    import json
    conn.execute(
        "UPDATE dead_letters SET record = ? WHERE id = ?",
        (json.dumps({"position": 2, "op": "insert", "pk": {"id": 1},
                     "payload": {"id": 1, "name": "stale"}}), dl_id))

    result = replay_service.replay([dl_id])
    assert result.items[0].result is ReplayResult.REJECTED
    assert result.items[0].reason is FailureReason.PRIMARY_KEY_CONFLICT
    assert store.get_rows("users") == [{"id": 1, "name": "real"}]  # 未被覆盖
    # 位点保持前进到 2，未被回退
    assert store.get_checkpoint("users") == 2
    logger.info("existing row protected: %s, reason=%s",
                store.get_rows("users"), result.items[0].reason)


def test_replay_and_normal_ingest_are_mutex(store, schema, pipeline,
                                            replay_service, monkeypatch):
    """同源重放与正常接入互斥：重放持锁期间接入必须等待，状态不串。"""
    schema.ensure_source("users", {"id": "int", "name": "str"})
    pipeline.ingest("users", [ChangeRecord(
        "users", 1, Op.INSERT, {"id": 1}, {"id": 1})])  # 缺 name -> 死信
    dl_id = _dead_ids(replay_service, "users")[0]
    store.set_columns("users", {"id": "int"})  # 源端修好，使重放能走 apply

    inside_replay, proceed = threading.Event(), threading.Event()
    original = store.replay_apply

    def slow_replay_apply(dl, rec):
        inside_replay.set()
        proceed.wait(timeout=2)
        return original(dl, rec)

    monkeypatch.setattr(store, "replay_apply", slow_replay_apply)

    done_ingest = threading.Event()

    def do_replay():
        replay_service.replay([dl_id])

    def do_ingest():
        pipeline.ingest("users", [ChangeRecord(
            "users", 2, Op.INSERT, {"id": 2}, {"id": 2})])
        done_ingest.set()

    tr = threading.Thread(target=do_replay)
    ti = threading.Thread(target=do_ingest)
    tr.start()
    inside_replay.wait(timeout=2)
    ti.start()
    try:
        assert not done_ingest.wait(timeout=0.3)  # 同源接入被重放挡住
        logger.info("ingest blocked while replay holds the source lock")
    finally:
        proceed.set()
        tr.join(); ti.join(timeout=2)
    assert done_ingest.is_set()

    # 两条数据各写一次，无重复；重放不动位点，正常接入把位点推进到 2
    assert sorted(store.get_rows("users"), key=lambda r: r["id"]) == [
        {"id": 1}, {"id": 2}]
    assert store.get_checkpoint("users") == 2
    assert replay_service.get_dead_letter(dl_id).disposition is Disposition.RESOLVED
    logger.info("mutex ok: rows=%s checkpoint=%d",
                store.get_rows("users"), store.get_checkpoint("users"))


def test_replay_retry_bounded_and_source_isolation(store, schema, locks):
    """重放可恢复错误有界重试；一个源重放失败不阻塞另一个源。"""
    from ingestion.pipeline import IngestionPipeline, PipelineConfig
    schema.ensure_source("a", {"id": "int", "v": "str"})
    schema.ensure_source("b", {"id": "int", "v": "str"})
    cfg = PipelineConfig(max_retries=2, backoff_seconds=0.01)
    pipe = IngestionPipeline(store, schema, cfg, locks)
    svc = ReplayService(store, schema, cfg, locks)

    # 两源各一条缺字段死信
    for src in ("a", "b"):
        pipe.ingest(src, [ChangeRecord(src, 1, Op.INSERT, {"id": 1}, {"id": 1})])
    a_id = _dead_ids(svc, "a")[0]
    b_id = _dead_ids(svc, "b")[0]
    store.set_columns("a", {"id": "int"})  # 结构修好，使重放能走到写入

    # a 源写入始终遇可恢复错误：有界重试后抛出，死信保持 pending
    calls = {"n": 0}

    def always_locked(dl, rec):
        calls["n"] += 1
        raise sqlite3.OperationalError("database is locked")

    store.replay_apply = always_locked  # type: ignore[assignment]
    with pytest.raises(sqlite3.OperationalError):
        svc.replay([a_id])
    assert calls["n"] == 1 + cfg.max_retries
    assert svc.get_dead_letter(a_id).disposition is Disposition.PENDING
    logger.info("a replay bounded attempts=%d, still pending, checkpoint=%d",
                calls["n"], store.get_checkpoint("a"))

    # 恢复后 b 源重放不受 a 源失败影响：修好结构，成功应用
    del store.replay_apply
    store.set_columns("b", {"id": "int"})
    result = svc.replay([b_id])
    assert result.applied == 1
    assert svc.get_dead_letter(b_id).disposition is Disposition.RESOLVED
    assert store.get_rows("b") == [{"id": 1}]
    logger.info("b replay applied despite a's failure: rows=%s",
                store.get_rows("b"))


def test_unrecoverable_replay_error_not_retried(store, schema, pipeline,
                                                replay_service, monkeypatch):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    pipeline.ingest("users", [ChangeRecord(
        "users", 1, Op.INSERT, {"id": 1}, {"id": 1})])  # 缺 name
    dl_id = _dead_ids(replay_service, "users")[0]
    store.set_columns("users", {"id": "int"})

    calls = {"n": 0}

    def boom(dl, rec):
        calls["n"] += 1
        raise ValueError("corrupt")

    monkeypatch.setattr(store, "replay_apply", boom)
    with pytest.raises(ValueError):
        replay_service.replay([dl_id])
    assert calls["n"] == 1  # 不可恢复错误不重试，死信仍 pending
    assert replay_service.get_dead_letter(dl_id).disposition is Disposition.PENDING


def test_schema_change_dead_letter_replay(store, schema, pipeline,
                                          replay_service):
    """不兼容结构变更进死信；结构路径修好后可重放，删列/改名永远不放行。"""
    schema.ensure_source("users", {"id": "int", "name": "str"})
    pipeline.ingest("users", [ChangeRecord(
        "users", 1, Op.INSERT, {"id": 1}, {"id": 1, "name": "a"})])
    # 删列：不兼容 -> 死信，位点推进到 2
    pipeline.apply_schema_change(SchemaChange(
        "users", 2, to_version=2, drop_columns=("name",)))
    dl_id = _dead_ids(replay_service, "users")[0]
    dl = replay_service.get_dead_letter(dl_id)
    assert dl.reason is FailureReason.INCOMPATIBLE_SCHEMA_CHANGE
    assert store.get_columns("users") == {"id": "int", "name": "str"}

    # 没修就重放：仍按不兼容明确拒
    result = replay_service.replay([dl_id])
    assert result.items[0].result is ReplayResult.REJECTED
    assert result.items[0].reason is FailureReason.INCOMPATIBLE_SCHEMA_CHANGE
    assert store.get_columns("users") == {"id": "int", "name": "str"}
    logger.info("incompatible schema replay still rejected, checkpoint=%d",
                store.get_checkpoint("users"))

    # 运维改为只新增列（结构路径修复）：把死信记录改成纯 add_columns 后重放成功
    import json
    conn = store.connect()
    conn.execute(
        "UPDATE dead_letters SET record = ? WHERE id = ?",
        (json.dumps({"position": 2, "to_version": 3,
                     "add_columns": {"age": "int"},
                     "drop_columns": [], "rename_columns": {}}), dl_id))
    result = replay_service.replay([dl_id])
    assert result.items[0].result is ReplayResult.APPLIED
    # 走与正常结构变更相同的 apply_change：注册列并回填存量行
    assert store.get_columns("users")["age"] == "int"
    assert store.get_rows("users") == [{"id": 1, "name": "a", "age": 0}]
    refreshed = replay_service.get_dead_letter(dl_id)
    assert refreshed.disposition is Disposition.RESOLVED
    assert refreshed.replay_state.value == "applied"
    # 位点不被重放触碰
    assert store.get_checkpoint("users") == 2
    logger.info("schema replay applied: columns=%s rows=%s",
                store.get_columns("users"), store.get_rows("users"))
