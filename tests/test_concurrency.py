"""并发触发：同一数据源互斥有序推进；不同数据源互不阻塞；重试有界。"""

from __future__ import annotations

import threading
import time

import pytest

from app.config import Settings
from app.errors import SourceLockTimeout, TransientStorageError
from app.locker import SourceLocker
from app.models import IngestBatch
from app.pipeline import IngestionPipeline
from app.storage import Storage
from tests.conftest import make_event


def test_concurrent_triggers_same_source_are_serialized(db_path, settings):
    """同一数据源被 8 个线程同时触发同一批次（重复投递场景）：

    互斥 + 幂等保证恰好一次生效——最终行数精确、无重复写、无状态错乱。
    """
    storage = Storage(db_path)
    storage.register_source("s", {"name": "str", "age": "int"})
    locker = SourceLocker(db_path, timeout_seconds=30.0)
    pipeline = IngestionPipeline(storage, locker, settings)

    n_threads, n_events = 8, 5
    batch = IngestBatch(source="s", events=[
        make_event(f"e{i}", i + 1, data={"name": f"n{i}", "age": i})
        for i in range(n_events)
    ])
    results: list = []
    errors: list[Exception] = []

    def trigger():
        try:
            results.append(pipeline.ingest(batch))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=trigger) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"并发触发不应产生异常: {errors}"
    rows = storage.list_rows("s")
    assert len(rows) == n_events, "并发重复触发不得重复写或丢行"
    assert len({r["pk"] for r in rows}) == n_events
    assert storage.get_watermark("s") == n_events
    # 恰好一个触发真正应用，其余全部识别为重复消费
    assert sum(r.applied for r in results) == n_events
    assert sum(r.duplicates for r in results) == (n_threads - 1) * n_events
    assert len(storage.list_dead_letters("s")) == 0
    storage.close()


def test_failing_source_does_not_block_others(db_path, settings):
    """slow 数据源持续触发可恢复错误并重试（退避在锁外），
    fast 数据源应在其重试期间正常完成，不被阻塞。"""
    storage = Storage(db_path)
    storage.register_source("slow", {"name": "str", "age": "int"})
    storage.register_source("fast", {"name": "str", "age": "int"})
    locker = SourceLocker(db_path, timeout_seconds=30.0)
    slow_pipeline = IngestionPipeline(
        storage, locker,
        Settings(db_path=db_path, max_retries=5, retry_backoff_seconds=0.1),
    )
    fast_pipeline = IngestionPipeline(storage, locker, settings)

    original = storage.apply_event
    def flaky(source, *args, **kwargs):
        if source == "slow":
            raise TransientStorageError("simulated SQLITE_BUSY")
        return original(source, *args, **kwargs)
    storage.apply_event = flaky  # type: ignore[method-assign]

    done: dict[str, float] = {}
    def run_slow():
        try:
            slow_pipeline.ingest(IngestBatch(
                source="slow", events=[make_event("slow-e1", 1)]))
        except TransientStorageError:
            pass
        done["slow"] = time.monotonic()

    def run_fast():
        fast_pipeline.ingest(IngestBatch(
            source="fast", events=[make_event("fast-e1", 1)]))
        done["fast"] = time.monotonic()

    start = time.monotonic()
    t_slow = threading.Thread(target=run_slow)
    t_fast = threading.Thread(target=run_fast)
    t_slow.start()
    time.sleep(0.05)  # 确保 slow 先进入重试循环
    t_fast.start()
    t_slow.join()
    t_fast.join()

    assert done["fast"] - start < 0.3, "fast 数据源不应被 slow 的重试阻塞"
    assert done["slow"] - start >= 0.4, "slow 应经历了多次退避重试"
    assert storage.get_watermark("fast") == 1
    assert storage.get_watermark("slow") == 0  # 全部重试失败，无副作用
    storage.close()


def test_lock_timeout_is_recoverable_error(db_path, settings):
    """锁被长期占用时，等待方以 LOCK_TIMEOUT（可恢复）失败而非死等。"""
    locker = SourceLocker(db_path, timeout_seconds=0.2)
    with locker.acquire("s"):
        with pytest.raises(SourceLockTimeout) as exc_info:
            with locker.acquire("s"):
                pass
    assert exc_info.value.recoverable is True


def test_transient_errors_are_retried_with_bound(db_path, settings):
    """可恢复错误按 max_retries 有界重试，成功为止；超过上限则放弃。"""
    storage = Storage(db_path)
    storage.register_source("s", {"name": "str", "age": "int"})
    pipeline = IngestionPipeline(
        storage, SourceLocker(db_path, 1.0),
        Settings(db_path=db_path, max_retries=3, retry_backoff_seconds=0.001),
    )
    batch = IngestBatch(source="s", events=[make_event("e1", 1)])

    # 前 2 次注入可恢复错误，第 3 次成功
    calls = {"n": 0}
    original = storage.apply_event

    def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise TransientStorageError("simulated SQLITE_BUSY")
        return original(*args, **kwargs)

    storage.apply_event = flaky  # type: ignore[method-assign]
    result = pipeline.ingest(batch)
    assert result.applied == 1 and result.attempts == 3
    assert storage.get_watermark("s") == 1

    # 持续失败：重试次数有界，最终抛错而非无界重试
    storage.apply_event = lambda *a, **k: (_ for _ in ()).throw(  # type: ignore[method-assign]
        TransientStorageError("always busy")
    )
    with pytest.raises(TransientStorageError):
        pipeline.ingest(IngestBatch(source="s", events=[make_event("e2", 2)]))
    storage.close()
