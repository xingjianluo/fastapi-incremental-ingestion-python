"""SQLite 本地存储：目标表、位点、事件流水、结构注册表、死信队列。

设计要点：
- 每个数据源一张目标表，列为真实 DDL 列：结构新增列 = ALTER TABLE ADD COLUMN
  带稳定 DEFAULT，删列/改类型在 schema_evolution 层直接拒绝，杜绝静默错列。
- apply_event 在单事务内完成“行写入 + 事件流水 + 水位推进”，
  崩溃恢复后依靠 (source,event_id) 唯一约束与水位实现不重不丢。
- 死信独立提交：数据事务回滚不影响失败记录的可追溯性。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .errors import FailureReason, RecordRejected, TransientStorageError
from .schema_evolution import SUPPORTED_TYPES

_SQL_TYPES = {"int": "INTEGER", "float": "REAL", "str": "TEXT", "bool": "INTEGER"}
_SQL_DEFAULTS = {"int": "0", "float": "0.0", "str": "''", "bool": "0"}


def qident(name: str) -> str:
    """安全引用 SQL 标识符。"""
    return '"' + name.replace('"', '""') + '"'


class Storage:
    """线程安全：每个线程使用自己的 SQLite 连接（thread-local）。

    跨线程一致性由文件级事务（BEGIN IMMEDIATE）与同一数据源的互斥锁共同保证；
    不同线程的连接在 WAL 模式下写串行、读不阻塞。
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._local = threading.local()
        self._all_conns: set[sqlite3.Connection] = set()
        self._conns_guard = threading.Lock()
        self._closed = False
        self._init_meta_tables()

    def _new_conn(self) -> sqlite3.Connection:
        # check_same_thread=False 是安全的：每个连接经 thread-local 只被
        # 创建它的线程使用；close() 在线程 join 之后跨线程关闭无并发风险。
        conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None,
            timeout=10.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        with self._conns_guard:
            if self._closed:
                conn.close()
                raise RuntimeError("Storage 已关闭")
            self._all_conns.add(conn)
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._new_conn()
            self._local.conn = conn
        return conn

    def close(self) -> None:
        with self._conns_guard:
            self._closed = True
            conns = list(self._all_conns)
            self._all_conns.clear()
        for conn in conns:
            conn.close()
        self._local.conn = None

    @property
    def is_closed(self) -> bool:
        return self._closed

    def _init_meta_tables(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sources (
                source         TEXT PRIMARY KEY,
                watermark      INTEGER NOT NULL DEFAULT 0,
                schema_version INTEGER NOT NULL DEFAULT 1,
                columns_json   TEXT NOT NULL DEFAULT '{}',
                required_json  TEXT NOT NULL DEFAULT '[]'
            );
            CREATE TABLE IF NOT EXISTS applied_events (
                source     TEXT NOT NULL,
                event_id   TEXT NOT NULL,
                seq        INTEGER NOT NULL,
                op         TEXT NOT NULL,
                pk         TEXT,
                data_json  TEXT,
                applied_at REAL NOT NULL,
                PRIMARY KEY (source, event_id)
            );
            CREATE TABLE IF NOT EXISTS dead_letters (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                source       TEXT NOT NULL,
                event_id     TEXT NOT NULL,
                seq          INTEGER NOT NULL,
                reason       TEXT NOT NULL,
                detail       TEXT NOT NULL,
                payload_json TEXT,
                created_at   REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_dead_source ON dead_letters(source);
            CREATE INDEX IF NOT EXISTS idx_applied_source_seq ON applied_events(source, seq);
            """
        )

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            # SQLITE_BUSY/LOCKED 属于可恢复错误
            conn.execute("ROLLBACK")
            raise TransientStorageError(str(exc)) from exc
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # ---- 数据源注册与位点 ----
    def register_source(self, source: str, columns: dict[str, str]) -> None:
        unknown = sorted(set(columns.values()) - SUPPORTED_TYPES)
        if unknown:
            raise RecordRejected(
                FailureReason.SCHEMA_INCOMPATIBLE, f"不支持的列类型: {unknown}"
            )
        table = f"t_{source}"
        col_defs = ["id TEXT PRIMARY KEY"] + [
            f"{qident(c)} {_SQL_TYPES[t]} NOT NULL DEFAULT {_SQL_DEFAULTS[t]}"
            for c, t in columns.items()
        ]
        with self._transaction() as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            conn.execute(f"CREATE TABLE IF NOT EXISTS {qident(table)} ({', '.join(col_defs)})")
            if exists is None:
                conn.execute(
                    "INSERT INTO sources(source, watermark, schema_version, columns_json, "
                    "required_json) VALUES(?, 0, 1, ?, ?)",
                    (source, json.dumps(columns), json.dumps(list(columns.keys()))),
                )

    def source_exists(self, source: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM sources WHERE source=?", (source,)
        ).fetchone() is not None

    def _meta(self, source: str) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM sources WHERE source=?", (source,)
        ).fetchone()
        if row is None:
            raise RecordRejected(
                FailureReason.UNKNOWN_SOURCE, f"数据源未注册: {source!r}"
            )
        return row

    def get_watermark(self, source: str) -> int:
        return int(self._meta(source)["watermark"])

    def get_columns(self, source: str) -> dict[str, str]:
        return json.loads(self._meta(source)["columns_json"])

    def get_required_columns(self, source: str) -> list[str]:
        """注册时确定的必填列；演进新增列不在其列（缺失时填确定缺省值）。"""
        return json.loads(self._meta(source)["required_json"])

    def get_schema_version(self, source: str) -> int:
        return int(self._meta(source)["schema_version"])

    # ---- 结构变更 ----
    def evolve_schema(
        self, source: str, columns: dict[str, str], event_id: str, seq: int
    ) -> int:
        """新增列走 ALTER TABLE ADD COLUMN（带确定 DEFAULT）；返回新版本号。

        结构变更同样记入 applied_events（op='schema'），使跨重启幂等
        去重与数据事件共用同一套位点流水。
        """
        old = self.get_columns(source)
        table = qident(f"t_{source}")
        with self._transaction() as conn:
            version = self.get_schema_version(source) + 1
            for name, type_name in columns.items():
                if name not in old:
                    conn.execute(
                        f"ALTER TABLE {table} ADD COLUMN {qident(name)} "
                        f"{_SQL_TYPES[type_name]} NOT NULL DEFAULT {_SQL_DEFAULTS[type_name]}"
                    )
            conn.execute(
                "UPDATE sources SET columns_json=?, schema_version=? WHERE source=?",
                (json.dumps(columns), version, source),
            )
            conn.execute(
                "INSERT INTO applied_events(source, event_id, seq, op, pk, data_json, applied_at)"
                " VALUES(?,?,?,?,?,?,?)",
                (source, event_id, seq, "schema", None, json.dumps(columns, ensure_ascii=False),
                 time.time()),
            )
            conn.execute(
                "UPDATE sources SET watermark=? WHERE source=? AND watermark<?",
                (seq, source, seq),
            )
        return version

    # ---- 数据应用 ----
    def event_seen(self, source: str, event_id: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM applied_events WHERE source=? AND event_id=?",
            (source, event_id),
        ).fetchone() is not None

    def row_exists(self, source: str, pk: Any) -> bool:
        return self.conn.execute(
            f"SELECT 1 FROM {qident(f't_{source}')} WHERE id=?", (self._pk(pk),)
        ).fetchone() is not None

    @staticmethod
    def _pk(pk: Any) -> str:
        if isinstance(pk, str):
            return pk
        return json.dumps(pk, separators=(",", ":"), ensure_ascii=False)

    def apply_event(
        self,
        source: str,
        event_id: str,
        seq: int,
        op: str,
        pk: Any,
        row: dict[str, Any] | None,
    ) -> None:
        table = qident(f"t_{source}")
        pk_s = self._pk(pk)
        with self._transaction() as conn:
            if op == "insert":
                cols = list(row.keys())  # type: ignore[union-attr]
                col_sql = ", ".join(["id"] + [qident(c) for c in cols])
                val_sql = ", ".join(["?"] * (len(cols) + 1))
                params = [pk_s] + [self._encode(row[c]) for c in cols]  # type: ignore[index]
                try:
                    conn.execute(f"INSERT INTO {table} ({col_sql}) VALUES ({val_sql})", params)
                except sqlite3.IntegrityError as exc:
                    raise RecordRejected(
                        FailureReason.PK_CONFLICT, f"主键冲突: {pk_s!r} ({exc})"
                    ) from exc
            elif op == "update":
                cols = [c for c in (row or {})]
                if cols:
                    set_sql = ", ".join(f"{qident(c)}=?" for c in cols)
                    params = [self._encode(row[c]) for c in cols] + [pk_s]  # type: ignore[index]
                    cur = conn.execute(
                        f"UPDATE {table} SET {set_sql} WHERE id=?", params
                    )
                    if cur.rowcount == 0:
                        raise RecordRejected(
                            FailureReason.UNKNOWN_PK, f"目标主键不存在: {pk_s!r}"
                        )
            elif op == "delete":
                conn.execute(f"DELETE FROM {table} WHERE id=?", (pk_s,))
            else:
                raise RecordRejected(FailureReason.TYPE_MISMATCH, f"未知操作: {op!r}")

            conn.execute(
                "INSERT INTO applied_events(source, event_id, seq, op, pk, data_json, applied_at)"
                " VALUES(?,?,?,?,?,?,?)",
                (
                    source,
                    event_id,
                    seq,
                    op,
                    pk_s,
                    json.dumps(row, ensure_ascii=False) if row is not None else None,
                    time.time(),
                ),
            )
            conn.execute(
                "UPDATE sources SET watermark=? WHERE source=? AND watermark<?",
                (seq, source, seq),
            )

    @staticmethod
    def _encode(value: Any) -> Any:
        return int(value) if isinstance(value, bool) else value

    # ---- 死信与查询 ----
    def record_dead_letter(
        self,
        source: str,
        event_id: str,
        seq: int,
        reason: str,
        detail: str,
        payload: str,
    ) -> None:
        """死信与水位推进在同一事务提交：失败记录可追溯且不会被无限重投。"""
        with self._transaction() as conn:
            conn.execute(
                "INSERT INTO dead_letters(source, event_id, seq, reason, detail, payload_json, created_at)"
                " VALUES(?,?,?,?,?,?,?)",
                (source, event_id, seq, reason, detail, payload, time.time()),
            )
            conn.execute(
                "UPDATE sources SET watermark=? WHERE source=? AND watermark<?",
                (seq, source, seq),
            )

    def list_dead_letters(self, source: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT event_id, seq, reason, detail, payload_json FROM dead_letters"
            " WHERE source=? ORDER BY id",
            (source,),
        ).fetchall()
        return [dict(r) for r in rows]

    def list_rows(self, source: str) -> list[dict[str, Any]]:
        columns = self.get_columns(source)
        cur = self.conn.execute(f"SELECT * FROM {qident(f't_{source}')} ORDER BY id")
        result: list[dict[str, Any]] = []
        for r in cur.fetchall():
            row: dict[str, Any] = {"pk": r["id"]}
            for name, type_name in columns.items():
                value = r[name]
                if type_name == "bool" and value is not None:
                    value = bool(value)
                row[name] = value
            result.append(row)
        return result

    def list_events(self, source: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT event_id, seq, op, pk, data_json FROM applied_events"
            " WHERE source=? ORDER BY seq, event_id",
            (source,),
        ).fetchall()
        return [dict(r) for r in rows]

    def source_state(self, source: str) -> dict[str, Any]:
        meta = self._meta(source)
        dl = self.conn.execute(
            "SELECT COUNT(*) AS n FROM dead_letters WHERE source=?", (source,)
        ).fetchone()["n"]
        applied = self.conn.execute(
            "SELECT COUNT(*) AS n FROM applied_events WHERE source=?", (source,)
        ).fetchone()["n"]
        return {
            "source": source,
            "watermark": int(meta["watermark"]),
            "schema_version": int(meta["schema_version"]),
            "applied_events": int(applied),
            "dead_letters": int(dl),
        }

    def list_sources(self) -> list[str]:
        return [
            r["source"]
            for r in self.conn.execute("SELECT source FROM sources ORDER BY source")
        ]
