"""同一数据源的互斥与有序推进：进程内锁 + SQLite 持久租约（跨进程）。

语义：
- 同一数据源的任意触发在全局串行化，先到先得；拿不到锁且等待超时则抛
  SourceLockTimeout（可恢复错误，调用方可有限重试）。
- 不同数据源使用不同锁，互不阻塞。
- 跨进程利用 SQLite 单文件事务的 BEGIN IMMEDIATE 抢占 leases 表中的行；
  租约带过期时间，持有者崩溃后租约可被其他进程接管，避免永久死锁。
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Iterator

from .errors import SourceLockTimeout


class SourceLocker:
    def __init__(
        self,
        db_path: str,
        timeout_seconds: float = 5.0,
        lease_seconds: float = 30.0,
    ) -> None:
        self._db_path = db_path
        self._timeout = timeout_seconds
        self._lease = lease_seconds
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def _thread_lock(self, source: str) -> threading.Lock:
        with self._guard:
            lock = self._locks.get(source)
            if lock is None:
                lock = threading.Lock()
                self._locks[source] = lock
            return lock

    @contextmanager
    def acquire(self, source: str) -> Iterator[None]:
        # 1) 进程内先串行（非阻塞，快速失败由等待循环决定）
        lock = self._thread_lock(source)
        deadline = time.monotonic() + self._timeout
        while not lock.acquire(blocking=False):
            if time.monotonic() >= deadline:
                raise SourceLockTimeout(
                    f"数据源 {source!r} 正被其他触发占用，等待超时"
                )
            time.sleep(0.01)
        token = uuid.uuid4().hex
        conn: sqlite3.Connection | None = None
        try:
            # 2) 跨进程持久租约
            conn = self._open_conn()
            self._acquire_lease(conn, source, token, deadline)
            try:
                yield
            finally:
                # 异常路径也必须释放租约，否则后续触发会被自己的残租阻塞
                self._release_lease(conn, source, token)
        finally:
            if conn is not None:
                conn.close()
            lock.release()

    def _open_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=self._timeout, isolation_level=None)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ingestion_leases ("
            "source TEXT PRIMARY KEY, token TEXT NOT NULL, expires_at REAL NOT NULL)"
        )
        return conn

    def _acquire_lease(
        self, conn: sqlite3.Connection, source: str, token: str, deadline: float
    ) -> None:
        while True:
            now = time.time()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO ingestion_leases(source, token, expires_at) VALUES(?,?,?)",
                    (source, token, now + self._lease),
                )
                conn.execute("COMMIT")
                return
            except sqlite3.IntegrityError:
                # 行已存在：若租约过期则接管，否则释放事务稍后重试
                conn.execute("ROLLBACK")
                row = conn.execute(
                    "SELECT token, expires_at FROM ingestion_leases WHERE source=?",
                    (source,),
                ).fetchone()
                if row is not None and row[1] < now:
                    cur = conn.execute(
                        "UPDATE ingestion_leases SET token=?, expires_at=? "
                        "WHERE source=? AND token=? AND expires_at<?",
                        (token, now + self._lease, source, row[0], now),
                    )
                    if cur.rowcount == 1:
                        return
            except sqlite3.OperationalError:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
            if time.monotonic() >= deadline:
                raise SourceLockTimeout(
                    f"数据源 {source!r} 跨进程锁等待超时"
                )
            time.sleep(0.01)

    def _release_lease(
        self, conn: sqlite3.Connection, source: str, token: str
    ) -> None:
        try:
            conn.execute(
                "DELETE FROM ingestion_leases WHERE source=? AND token=?",
                (source, token),
            )
        except sqlite3.Error:
            # 释放失败不掩盖业务异常；租约到期后会被其他触发接管
            pass
