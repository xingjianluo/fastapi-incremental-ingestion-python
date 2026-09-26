"""增量接入管线：校验、去重、按数据源互斥、有界重试。

语义：
- 同一数据源的 ingest 通过 per-source 锁互斥，批内按位点升序应用；
  不同数据源互不阻塞。
- 位点 <= checkpoint：已应用过 => 幂等跳过（计 duplicate）；
  未应用过 => 位点回退，明确拒绝。
- 校验失败（主键缺失/冲突、未知列、类型不匹配）逐条拒绝并写入死信，
  不静默跳过；拒绝记录同样推进位点，避免无界重试。
- 存储层异常按可恢复/不可恢复分类，仅可恢复错误做有界退避重试。
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from typing import Any

from .models import (
    ChangeRecord,
    ErrorKind,
    FailureReason,
    IngestResult,
    Op,
    Rejection,
    SchemaChange,
)
from .schema import SchemaManager, value_matches
from .store import Store

logger = logging.getLogger("ingestion.pipeline")


class PipelineConfig:
    """可配置项。"""

    def __init__(self, max_retries: int = 3, backoff_seconds: float = 0.05) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        self.max_retries = max_retries          # 可恢复错误的最大重试次数（有界）
        self.backoff_seconds = backoff_seconds  # 退避基数，按尝试次数线性增长


def classify_error(exc: Exception) -> ErrorKind:
    """区分可恢复与不可恢复错误。"""
    if isinstance(exc, sqlite3.OperationalError):
        return ErrorKind.RECOVERABLE  # 如数据库瞬时锁定
    return ErrorKind.UNRECOVERABLE


class IngestionPipeline:
    """按数据源互斥、幂等地应用变更。"""

    def __init__(self, store: Store, schema: SchemaManager, config: PipelineConfig) -> None:
        self._store = store
        self._schema = schema
        self._config = config
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _lock_for(self, source: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(source, threading.Lock())

    # ---- 变更数据接入 ----
    def ingest(self, source: str, records: list[ChangeRecord]) -> IngestResult:
        with self._lock_for(source):  # 同一数据源互斥
            return self._with_retry(source, records)

    def _with_retry(self, source: str, records: list[ChangeRecord]) -> IngestResult:
        attempt = 0
        while True:
            try:
                result = self._ingest_once(source, records)
                result.attempts = attempt + 1
                return result
            except Exception as exc:
                if classify_error(exc) is ErrorKind.UNRECOVERABLE:
                    logger.error("source=%s unrecoverable error: %r", source, exc)
                    raise
                attempt += 1
                if attempt > self._config.max_retries:
                    logger.error("source=%s retries exhausted after %d attempts",
                                 source, attempt)
                    raise
                delay = self._config.backoff_seconds * attempt
                logger.warning("source=%s recoverable error %r, retry %d in %.3fs",
                               source, exc, attempt, delay)
                time.sleep(delay)

    def _ingest_once(self, source: str, records: list[ChangeRecord]) -> IngestResult:
        checkpoint = self._store.get_checkpoint(source)
        columns = self._store.get_columns(source)
        result = IngestResult(source=source, last_position=checkpoint)
        applied: list[ChangeRecord] = []
        seen: set[int] = set()
        max_position = checkpoint

        for rec in sorted(records, key=lambda r: r.position):  # 有序推进
            max_position = max(max_position, rec.position)
            if rec.position in seen or (
                rec.position <= checkpoint
                and self._store.is_position_applied(source, rec.position)
            ):
                result.duplicates += 1  # 重复消费：幂等跳过
                logger.info("source=%s position=%d duplicate, skipped",
                            source, rec.position)
                continue
            seen.add(rec.position)
            if rec.position <= checkpoint:
                result.rejected.append(self._reject(
                    rec, FailureReason.POSITION_REGRESSION,
                    f"position {rec.position} regresses below checkpoint {checkpoint}",
                ))
                continue
            rejection = self._validate(rec, columns)
            if rejection is not None:
                result.rejected.append(rejection)
                continue
            applied.append(rec)
            logger.info("source=%s position=%d op=%s applied",
                        source, rec.position, rec.op.value)

        for rej in result.rejected:
            logger.warning("source=%s position=%d rejected reason=%s detail=%s",
                           source, rej.position, rej.reason.value, rej.detail)

        self._store.apply_batch(source, applied, result.rejected, max_position)
        result.applied = len(applied)
        result.last_position = max_position
        return result

    def _validate(self, rec: ChangeRecord, columns: dict[str, str]) -> Rejection | None:
        if not rec.pk:
            return self._reject(rec, FailureReason.MISSING_PRIMARY_KEY,
                                "primary key is empty")
        if rec.op is Op.INSERT and self._store.row_exists(rec.source, rec.pk):
            return self._reject(rec, FailureReason.PRIMARY_KEY_CONFLICT,
                                f"primary key {rec.pk} already exists")
        if rec.op is not Op.DELETE:
            for name in columns:
                if name not in rec.payload:
                    return self._reject(rec, FailureReason.MISSING_FIELD,
                                        f"column {name!r} missing in payload")
            for name, value in rec.payload.items():
                if name not in columns:
                    return self._reject(rec, FailureReason.UNKNOWN_COLUMN,
                                        f"column {name!r} not in target schema")
                if not value_matches(columns[name], value):
                    return self._reject(
                        rec, FailureReason.TYPE_MISMATCH,
                        f"column {name!r} expects {columns[name]}, got {value!r}")
        return None

    @staticmethod
    def _reject(rec: ChangeRecord, reason: FailureReason, detail: str) -> Rejection:
        return Rejection(
            source=rec.source,
            position=rec.position,
            reason=reason,
            detail=detail,
            record={"position": rec.position, "op": rec.op.value,
                    "pk": rec.pk, "payload": rec.payload},
        )

    # ---- 结构变更接入 ----
    def apply_schema_change(self, change: SchemaChange) -> None:
        with self._lock_for(change.source):
            checkpoint = self._store.get_checkpoint(change.source)
            if change.position <= checkpoint and self._store.is_position_applied(
                change.source, change.position
            ):
                logger.info("source=%s schema change position=%d duplicate, skipped",
                            change.source, change.position)
                return
            if self._schema.is_compatible(change):
                self._schema.apply_change(change)
                self._store.apply_batch(change.source, [], [], change.position)
                logger.info("source=%s schema change position=%d applied: +%s",
                            change.source, change.position,
                            sorted(change.add_columns))
            else:
                # 不兼容变更：保留原始记录并标记为可追溯的失败
                rejection = Rejection(
                    source=change.source,
                    position=change.position,
                    reason=FailureReason.INCOMPATIBLE_SCHEMA_CHANGE,
                    detail=f"drop={list(change.drop_columns)} "
                           f"rename={change.rename_columns}",
                    record={"position": change.position,
                            "to_version": change.to_version,
                            "add_columns": change.add_columns,
                            "drop_columns": list(change.drop_columns),
                            "rename_columns": change.rename_columns},
                )
                self._store.apply_batch(change.source, [], [rejection], change.position)
                logger.warning("source=%s schema change position=%d rejected: %s",
                               change.source, change.position, rejection.detail)
