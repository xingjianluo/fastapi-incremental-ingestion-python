"""本地 SQLite 存储：目标行、位点、schema 注册表与死信表。

设计要点：
- 单连接 + WAL + busy_timeout；连接被多线程共享（不同数据源并发触发），
  用可重入锁串行化所有访问，apply_batch 整个事务持锁，保证事务完整性。
- apply_batch 在一个 IMMEDIATE 事务内完成：写目标行、记位点流水、
  写死信、推进 checkpoint，要么全部成功要么全部回滚，保证重启后不丢不重。
"""
from __future__ import annotations

import functools
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from .models import ChangeRecord, Op, Rejection

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS checkpoints (
    source TEXT PRIMARY KEY,
    last_position INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS applied_positions (
    source TEXT NOT NULL,
    position INTEGER NOT NULL,
    PRIMARY KEY (source, position)
);
CREATE TABLE IF NOT EXISTS schema_registry (
    source TEXT NOT NULL,
    column_name TEXT NOT NULL,
    type_name TEXT NOT NULL,
    PRIMARY KEY (source, column_name)
);
CREATE TABLE IF NOT EXISTS target_rows (
    source TEXT NOT NULL,
    pk TEXT NOT NULL,
    row TEXT NOT NULL,
    PRIMARY KEY (source, pk)
);
CREATE TABLE IF NOT EXISTS dead_letters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    position INTEGER NOT NULL,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    record TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def canonical_pk(pk: dict[str, Any]) -> str:
    """主键规范化：键排序后的 JSON，保证同一主键映射到同一字符串。"""
    return json.dumps(pk, sort_keys=True, ensure_ascii=False)


def _locked(fn):
    """串行化对共享连接的访问（可重入）。"""

    @functools.wraps(fn)
    def wrapper(self: "Store", *args, **kwargs):
        with self._mu:
            return fn(self, *args, **kwargs)

    return wrapper


class Store:
    """基于 SQLite 的本地持久层。"""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self._conn: sqlite3.Connection | None = None
        self._mu = threading.RLock()

    def connect(self) -> sqlite3.Connection:
        with self._mu:
            if self._conn is None:
                self._conn = sqlite3.connect(
                    self.db_path, check_same_thread=False, isolation_level=None
                )
                self._conn.execute("PRAGMA journal_mode=WAL")
                self._conn.execute("PRAGMA busy_timeout=5000")
                self._conn.executescript(_SCHEMA_SQL)
            return self._conn

    def close(self) -> None:
        with self._mu:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # ---- 位点 ----
    @_locked
    def get_checkpoint(self, source: str) -> int:
        row = self.connect().execute(
            "SELECT last_position FROM checkpoints WHERE source = ?", (source,)
        ).fetchone()
        return int(row[0]) if row else 0

    @_locked
    def is_position_applied(self, source: str, position: int) -> bool:
        row = self.connect().execute(
            "SELECT 1 FROM applied_positions WHERE source = ? AND position = ?",
            (source, position),
        ).fetchone()
        return row is not None

    @_locked
    def row_exists(self, source: str, pk: dict[str, Any]) -> bool:
        row = self.connect().execute(
            "SELECT 1 FROM target_rows WHERE source = ? AND pk = ?",
            (source, canonical_pk(pk)),
        ).fetchone()
        return row is not None

    # ---- schema 注册表 ----
    @_locked
    def get_columns(self, source: str) -> dict[str, str]:
        rows = self.connect().execute(
            "SELECT column_name, type_name FROM schema_registry WHERE source = ?"
            " ORDER BY column_name",
            (source,),
        ).fetchall()
        return {name: type_name for name, type_name in rows}

    @_locked
    def set_columns(self, source: str, columns: dict[str, str]) -> None:
        conn = self.connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM schema_registry WHERE source = ?", (source,))
            conn.executemany(
                "INSERT INTO schema_registry (source, column_name, type_name)"
                " VALUES (?, ?, ?)",
                [(source, name, t) for name, t in sorted(columns.items())],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    @_locked
    def add_column(self, source: str, name: str, type_name: str, default: Any) -> None:
        """新增列：注册列并为存量行回填稳定缺省值。"""
        conn = self.connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO schema_registry (source, column_name, type_name)"
                " VALUES (?, ?, ?)",
                (source, name, type_name),
            )
            for (pk, row_json) in conn.execute(
                "SELECT pk, row FROM target_rows WHERE source = ?", (source,)
            ).fetchall():
                row = json.loads(row_json)
                row[name] = default
                conn.execute(
                    "UPDATE target_rows SET row = ? WHERE source = ? AND pk = ?",
                    (json.dumps(row, ensure_ascii=False), source, pk),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # ---- 变更应用（事务） ----
    @_locked
    def apply_batch(
        self,
        source: str,
        records: list[ChangeRecord],
        rejections: list[Rejection],
        last_position: int,
    ) -> None:
        """单事务应用一批已校验的变更并推进位点。"""
        conn = self.connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            for rec in records:
                pk = canonical_pk(rec.pk)
                if rec.op is Op.DELETE:
                    conn.execute(
                        "DELETE FROM target_rows WHERE source = ? AND pk = ?",
                        (source, pk),
                    )
                else:
                    conn.execute(
                        "INSERT INTO target_rows (source, pk, row) VALUES (?, ?, ?)"
                        " ON CONFLICT (source, pk) DO UPDATE SET row = excluded.row",
                        (source, pk, json.dumps(rec.payload, ensure_ascii=False)),
                    )
                conn.execute(
                    "INSERT OR IGNORE INTO applied_positions (source, position)"
                    " VALUES (?, ?)",
                    (source, rec.position),
                )
            for rej in rejections:
                conn.execute(
                    "INSERT INTO dead_letters (source, position, reason, detail, record)"
                    " VALUES (?, ?, ?, ?, ?)",
                    (source, rej.position, rej.reason.value, rej.detail,
                     json.dumps(rej.record, ensure_ascii=False)),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO applied_positions (source, position)"
                    " VALUES (?, ?)",
                    (source, rej.position),
                )
            conn.execute(
                "INSERT INTO checkpoints (source, last_position) VALUES (?, ?)"
                " ON CONFLICT (source) DO UPDATE SET last_position = excluded.last_position",
                (source, last_position),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # ---- 查询 ----
    @_locked
    def list_rejections(self, source: str) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            "SELECT position, reason, detail, record FROM dead_letters"
            " WHERE source = ? ORDER BY id",
            (source,),
        ).fetchall()
        return [
            {"position": p, "reason": r, "detail": d, "record": json.loads(rec)}
            for p, r, d, rec in rows
        ]

    @_locked
    def get_rows(self, source: str) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            "SELECT row FROM target_rows WHERE source = ? ORDER BY pk", (source,)
        ).fetchall()
        return [json.loads(r[0]) for r in rows]
