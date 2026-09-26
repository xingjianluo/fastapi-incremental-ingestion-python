"""同一数据源并发触发：互斥、有序、不重复；数据源间互不阻塞；有界重试。"""
import logging
import sqlite3
import threading

import pytest

from ingestion.models import ChangeRecord, Op

logger = logging.getLogger("test.concurrency")


def _rec(source, pos, pk_id):
    return ChangeRecord(source, pos, Op.INSERT, {"id": pk_id},
                        {"id": pk_id, "name": f"n{pk_id}"})


def test_concurrent_triggers_same_source(store, schema, pipeline):
    """多线程并发触发同一数据源：无重复写入、位点一致、结果可解释。"""
    schema.ensure_source("users", {"id": "int", "name": "str"})
    threads_n, batch_n = 8, 25
    results, errors = [], []

    def trigger(positions):
        try:
            batch = [_rec("users", p, p) for p in positions]
            results.append(pipeline.ingest("users", batch))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    # 每个线程都投递全量位点 1..batch_n，模拟并发重复消费
    threads = [threading.Thread(target=trigger, args=(range(1, batch_n + 1),))
               for _ in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    applied = sum(r.applied for r in results)
    duplicates = sum(r.duplicates for r in results)
    logger.info("threads=%d applied=%d duplicates=%d checkpoint=%d",
                threads_n, applied, duplicates, store.get_checkpoint("users"))
    assert applied == batch_n                          # 每条位点恰好应用一次
    assert duplicates == threads_n * batch_n - batch_n  # 其余全部幂等跳过
    assert len(store.get_rows("users")) == batch_n     # 无重复写入
    assert store.get_checkpoint("users") == batch_n


def test_source_failure_does_not_block_others(store, schema, pipeline):
    """一个数据源的拒绝/失败不影响其他数据源推进。"""
    schema.ensure_source("good", {"id": "int", "name": "str"})
    schema.ensure_source("bad", {"id": "int", "name": "str"})
    barrier = threading.Barrier(2)
    outcomes = {}

    def run(source, records):
        barrier.wait()
        outcomes[source] = pipeline.ingest(source, records)

    bad_batch = [_rec("bad", p, p) if p % 2 else
                 ChangeRecord("bad", p, Op.INSERT, {"id": p}, {"id": p, "name": p})
                 for p in range(1, 11)]
    t1 = threading.Thread(target=run, args=("good", [_rec("good", p, p)
                                                     for p in range(1, 11)]))
    t2 = threading.Thread(target=run, args=("bad", bad_batch))
    t1.start(); t2.start(); t1.join(); t2.join()

    logger.info("good: applied=%d; bad: applied=%d rejected=%d",
                outcomes["good"].applied, outcomes["bad"].applied,
                len(outcomes["bad"].rejected))
    assert outcomes["good"].applied == 10               # good 源完全不受阻塞
    assert outcomes["bad"].applied == 5
    assert len(outcomes["bad"].rejected) == 5
    assert store.get_checkpoint("good") == 10
    assert store.get_checkpoint("bad") == 10


def test_recoverable_error_retried_then_succeeds(store, schema, pipeline,
                                                 monkeypatch):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    calls = {"n": 0}
    original = store.apply_batch

    def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise sqlite3.OperationalError("database is locked")
        return original(*args, **kwargs)

    monkeypatch.setattr(store, "apply_batch", flaky)
    r = pipeline.ingest("users", [_rec("users", 1, 1)])
    logger.info("recovered after attempts=%d", r.attempts)
    assert r.applied == 1 and r.attempts == 3
    assert store.get_checkpoint("users") == 1


def test_recoverable_error_retry_is_bounded(store, schema, pipeline,
                                            monkeypatch):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    calls = {"n": 0}

    def always_locked(*args, **kwargs):
        calls["n"] += 1
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(store, "apply_batch", always_locked)
    with pytest.raises(sqlite3.OperationalError):
        pipeline.ingest("users", [_rec("users", 1, 1)])
    # 1 次首发 + max_retries(=2) 次重试，有界
    assert calls["n"] == 1 + pipeline._config.max_retries
    assert store.get_checkpoint("users") == 0  # 状态未错乱


def test_unrecoverable_error_not_retried(store, schema, pipeline, monkeypatch):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    calls = {"n": 0}

    def broken(*args, **kwargs):
        calls["n"] += 1
        raise ValueError("corrupt payload")

    monkeypatch.setattr(store, "apply_batch", broken)
    with pytest.raises(ValueError):
        pipeline.ingest("users", [_rec("users", 1, 1)])
    assert calls["n"] == 1  # 不可恢复错误不重试
