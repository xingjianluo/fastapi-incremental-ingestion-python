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

from .models import (
    ChangeRecord,
    DeadLetter,
    Disposition,
    FailureReason,
    Op,
    Rejection,
    ReplayState,
)

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
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    disposition TEXT NOT NULL DEFAULT 'pending',
    handled_at TEXT,
    note TEXT NOT NULL DEFAULT '',
    replay_state TEXT NOT NULL DEFAULT 'none',
    last_replayed_at TEXT,
    replay_reason TEXT,
    replay_detail TEXT,
    replay_attempts INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS replay_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dead_letter_id INTEGER NOT NULL,
    source TEXT NOT NULL,
    position INTEGER NOT NULL,
    result TEXT NOT NULL,
    reason TEXT,
    detail TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (dead_letter_id) REFERENCES dead_letters(id)
);
"""

# 旧版 dead_letters 表（仅 6 列）到处置/重放结构的增量迁移
_MIGRATION_COLUMNS: dict[str, str] = {
    "disposition": "TEXT NOT NULL DEFAULT 'pending'",
    "handled_at": "TEXT",
    "note": "TEXT NOT NULL DEFAULT ''",
    "replay_state": "TEXT NOT NULL DEFAULT 'none'",
    "last_replayed_at": "TEXT",
    "replay_reason": "TEXT",
    "replay_detail": "TEXT",
    "replay_attempts": "INTEGER NOT NULL DEFAULT 0",
}


def canonical_pk(pk: dict[str, Any]) -> str:
    """主键规范化：键排序后的 JSON，保证同一主键映射到同一字符串。"""
    return json.dumps(pk, sort_keys=True, ensure_ascii=False)


class DeadLetterNotPending(RuntimeError):
    """重放落库时发现该死信已不是 pending（被并发标注/重放处理过）。

    事务已回滚，未产生任何写入；调用方据此幂等跳过即可。
    """


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
                self._migrate_dead_letters()
            return self._conn

    def close(self) -> None:
        with self._mu:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _migrate_dead_letters(self) -> None:
        """为旧版 dead_letters 表补齐处置/重放列（幂等）。"""
        conn = self._conn
        assert conn is not None
        existing = {
            row[1]
            for row in conn.execute("PRAGMA table_info(dead_letters)").fetchall()
        }
        for name, decl in _MIGRATION_COLUMNS.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE dead_letters ADD COLUMN {name} {decl}")
        # 依赖处置/重放列的索引：建表与迁移完成后再创建（幂等）
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dead_letters_filter"
            " ON dead_letters (source, disposition, reason, id)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_replay_events_dl"
            " ON replay_events (dead_letter_id, id)")

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

    # ---- 失败记录：定位 / 筛选 / 分页 ----
    _DL_COLUMNS = (
        "id, source, position, reason, detail, record, created_at,"
        " disposition, handled_at, note, replay_state, last_replayed_at,"
        " replay_reason, replay_detail, replay_attempts"
    )

    def _row_to_dead_letter(self, row: tuple) -> DeadLetter:
        (dl_id, source, position, reason, detail, record, created_at,
         disposition, handled_at, note, replay_state, last_replayed_at,
         replay_reason, replay_detail, replay_attempts) = row
        return DeadLetter(
            id=dl_id, source=source, position=position,
            reason=FailureReason(reason), detail=detail,
            record=json.loads(record), created_at=created_at,
            disposition=Disposition(disposition), handled_at=handled_at,
            note=note or "", replay_state=ReplayState(replay_state),
            last_replayed_at=last_replayed_at, replay_reason=replay_reason,
            replay_detail=replay_detail, replay_attempts=replay_attempts,
        )

    @_locked
    def get_dead_letter(self, dead_letter_id: int) -> DeadLetter | None:
        """按 id 单独定位一条失败记录。"""
        row = self.connect().execute(
            f"SELECT {self._DL_COLUMNS} FROM dead_letters WHERE id = ?",
            (dead_letter_id,),
        ).fetchone()
        return self._row_to_dead_letter(row) if row else None

    @_locked
    def query_dead_letters(
        self,
        source: str | None = None,
        disposition: str | None = None,
        reason: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[DeadLetter], int]:
        """按数据源 / 处置状态 / 失败原因筛选并分页，返回 (本页记录, 总数)。

        只读取 limit 指定的一页，不把全部失败记录载入内存。
        """
        if limit <= 0:
            raise ValueError("limit must be > 0")
        if offset < 0:
            raise ValueError("offset must be >= 0")
        where, params = [], []
        if source is not None:
            where.append("source = ?")
            params.append(source)
        if disposition is not None:
            where.append("disposition = ?")
            params.append(disposition)
        if reason is not None:
            where.append("reason = ?")
            params.append(reason)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        conn = self.connect()
        total = int(conn.execute(
            f"SELECT COUNT(*) FROM dead_letters{clause}", params
        ).fetchone()[0])
        rows = conn.execute(
            f"SELECT {self._DL_COLUMNS} FROM dead_letters{clause}"
            " ORDER BY id LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchall()
        return [self._row_to_dead_letter(r) for r in rows], total

    @_locked
    def mark_disposition(
        self,
        dead_letter_id: int,
        disposition: Disposition,
        note: str = "",
    ) -> DeadLetter | None:
        """标注处置状态（待处理/已解决/已忽略），记录处置时间与说明。"""
        conn = self.connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                f"SELECT {self._DL_COLUMNS} FROM dead_letters WHERE id = ?",
                (dead_letter_id,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                return None
            conn.execute(
                "UPDATE dead_letters SET disposition = ?, note = ?,"
                " handled_at = datetime('now') WHERE id = ?",
                (disposition.value, note, dead_letter_id),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        updated = self.get_dead_letter(dead_letter_id)
        assert updated is not None
        return updated

    # ---- 受控重放落库（单条事务；不触碰 checkpoint / applied_positions） ----
    @_locked
    def replay_apply(
        self, dl: DeadLetter, rec: ChangeRecord
    ) -> None:
        """重放成功：走 upsert 写入目标行；原死信流转为 resolved 并追加流水。

        - 不写 applied_positions、不推进 checkpoint：重放是位点之外的受控补偿。
        - upsert 语义保证重复重放不产生重复行（同主键覆盖为同一内容，
          而正常接入的重复重放会在服务层直接幂等跳过，不会走到这里）。
        """
        conn = self.connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            pk = canonical_pk(rec.pk)
            if rec.op is Op.DELETE:
                conn.execute(
                    "DELETE FROM target_rows WHERE source = ? AND pk = ?",
                    (rec.source, pk),
                )
            else:
                conn.execute(
                    "INSERT INTO target_rows (source, pk, row) VALUES (?, ?, ?)"
                    " ON CONFLICT (source, pk) DO UPDATE SET row = excluded.row",
                    (rec.source, pk, json.dumps(rec.payload, ensure_ascii=False)),
                )
            cur = conn.execute(
                "UPDATE dead_letters SET disposition = 'resolved',"
                " handled_at = datetime('now'), replay_state = 'applied',"
                " last_replayed_at = datetime('now'), replay_reason = NULL,"
                " replay_detail = '', replay_attempts = replay_attempts + 1"
                " WHERE id = ? AND disposition = 'pending'",
                (dl.id,),
            )
            if cur.rowcount != 1:
                raise DeadLetterNotPending(
                    f"dead letter {dl.id} is not pending during replay apply")
            conn.execute(
                "INSERT INTO replay_events"
                " (dead_letter_id, source, position, result, reason, detail)"
                " VALUES (?, ?, ?, 'applied', NULL, '')",
                (dl.id, dl.source, dl.position),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    @_locked
    def replay_schema_apply(self, dl: DeadLetter) -> None:
        """结构变更重放成功：原死信流转 resolved 并追加流水（schema 已另行落库）。"""
        conn = self.connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "UPDATE dead_letters SET disposition = 'resolved',"
                " handled_at = datetime('now'), replay_state = 'applied',"
                " last_replayed_at = datetime('now'), replay_reason = NULL,"
                " replay_detail = '', replay_attempts = replay_attempts + 1"
                " WHERE id = ? AND disposition = 'pending'",
                (dl.id,),
            )
            if cur.rowcount != 1:
                raise DeadLetterNotPending(
                    f"dead letter {dl.id} is not pending during schema replay")
            conn.execute(
                "INSERT INTO replay_events"
                " (dead_letter_id, source, position, result, reason, detail)"
                " VALUES (?, ?, ?, 'applied', NULL, '')",
                (dl.id, dl.source, dl.position),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    @_locked
    def replay_reject(
        self,
        dl: DeadLetter,
        reason: str,
        detail: str,
    ) -> None:
        """重放仍被明确拒绝：保持 pending，记录可区分原因并追加流水。"""
        conn = self.connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "UPDATE dead_letters SET replay_state = 'rejected',"
                " last_replayed_at = datetime('now'), replay_reason = ?,"
                " replay_detail = ?,"
                " replay_attempts = replay_attempts + 1"
                " WHERE id = ? AND disposition = 'pending'",
                (reason, detail, dl.id),
            )
            if cur.rowcount != 1:
                raise DeadLetterNotPending(
                    f"dead letter {dl.id} is not pending during replay reject")
            conn.execute(
                "INSERT INTO replay_events"
                " (dead_letter_id, source, position, result, reason, detail)"
                " VALUES (?, ?, ?, 'rejected', ?, ?)",
                (dl.id, dl.source, dl.position, reason, detail),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    @_locked
    def list_replay_events(self, dead_letter_id: int) -> list[dict[str, Any]]:
        """查看一条死信的完整重放流水（可追溯）。"""
        rows = self.connect().execute(
            "SELECT result, reason, detail, created_at FROM replay_events"
            " WHERE dead_letter_id = ? ORDER BY id",
            (dead_letter_id,),
        ).fetchall()
        return [
            {"result": r, "reason": reason, "detail": detail, "at": at}
            for r, reason, detail, at in rows
        ]
