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

from .models import (
    ChangeRecord,
    ErrorKind,
    FailureReason,
    IngestResult,
    Op,
    Rejection,
    ReplayItemResult,
    ReplayOutcome,
    ReplayResult,
    SchemaChange,
)
from .schema import SchemaManager, value_matches
from .store import Store

logger = logging.getLogger("ingestion.pipeline")


class PipelineConfig:
    """可配置项。"""

    def __init__(self, max_retries: int = 3, backoff_seconds: float = 0.05,
                 max_replay_batch: int = 100) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if max_replay_batch < 1:
            raise ValueError("max_replay_batch must be >= 1")
        self.max_retries = max_retries          # 可恢复错误的最大重试次数（有界）
        self.backoff_seconds = backoff_seconds  # 退避基数，按尝试次数线性增长
        self.max_replay_batch = max_replay_batch  # 单次重放请求允许的最大条数


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
            return self._with_retry(source, lambda: self._ingest_once(source, records))

    def _with_retry(self, source: str, fn):
        """对可恢复错误做有界退避重试；不可恢复错误直接抛出。"""
        attempt = 0
        while True:
            try:
                result = fn()
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

    # ---- 失败记录受控重放 ----
    def replay(self, dead_letter_ids: list[int]) -> ReplayResult:
        """重放一小批死信记录。

        与位点的关系：死信位点在首次拒绝时已推进并落账，重放不查位点、
        不推进 checkpoint、不改 applied_positions，只补写数据；重放幂等性
        由死信自身的 replay_outcome 保证（已成功重放的再次重放幂等跳过）。
        并发：按数据源分组后复用 per-source 锁，与正常接入互斥、按位点有序；
        不同数据源的重放与接入互不阻塞。可恢复错误沿用有界重试。
        """
        if not dead_letter_ids:
            return ReplayResult()
        if len(dead_letter_ids) > self._config.max_replay_batch:
            raise ValueError(
                f"replay batch size {len(dead_letter_ids)} exceeds"
                f" max_replay_batch {self._config.max_replay_batch}")
        result = ReplayResult()
        groups: dict[str, list[int]] = {}
        for dl_id in dict.fromkeys(dead_letter_ids):  # 去重并保持顺序
            dl = self._store.get_dead_letter(dl_id)
            if dl is None:
                result.results.append(ReplayItemResult(
                    dead_letter_id=dl_id, outcome=ReplayOutcome.NOT_FOUND))
                logger.warning("replay dead_letter=%d not found", dl_id)
                continue
            groups.setdefault(dl.source, []).append(dl_id)
        for source, ids in groups.items():
            with self._lock_for(source):  # 与该数据源的正常接入互斥
                sub = self._with_retry(
                    source,
                    lambda source=source, ids=ids: self._replay_source_once(
                        source, ids))
            result.results.extend(sub.results)
            result.attempts = max(result.attempts, sub.attempts)
        return result

    def _replay_source_once(self, source: str, ids: list[int]) -> ReplayResult:
        result = ReplayResult()
        columns = self._store.get_columns(source)
        checkpoint = self._store.get_checkpoint(source)
        letters = [self._store.get_dead_letter(i) for i in ids]
        pending = [dl for dl in letters if dl is not None]
        for dl in sorted(pending, key=lambda d: d.position):  # 按位点有序
            if dl.replay_outcome is ReplayOutcome.APPLIED:
                # 重试或重复重放：此前已成功应用，幂等跳过，不产生重复数据
                result.results.append(ReplayItemResult(
                    dead_letter_id=dl.id, source=source, position=dl.position,
                    outcome=ReplayOutcome.ALREADY_APPLIED))
                logger.info("source=%s dead_letter=%d position=%d replay"
                            " already applied, skipped (checkpoint=%d)",
                            source, dl.id, dl.position, checkpoint)
                continue
            record = ChangeRecord(
                source=dl.source, position=dl.position,
                op=Op(dl.record["op"]), pk=dl.record["pk"],
                payload=dl.record.get("payload", {}))
            rejection = self._validate(record, columns)  # 与正常接入同一套校验
            if rejection is None:
                self._store.apply_replay(dl, record)
                result.results.append(ReplayItemResult(
                    dead_letter_id=dl.id, source=source, position=dl.position,
                    outcome=ReplayOutcome.APPLIED))
                logger.info("source=%s dead_letter=%d position=%d replay applied"
                            " (checkpoint=%d unchanged)",
                            source, dl.id, dl.position, checkpoint)
            else:
                self._store.record_replay_rejection(
                    dl, rejection.reason, rejection.detail)
                result.results.append(ReplayItemResult(
                    dead_letter_id=dl.id, source=source, position=dl.position,
                    outcome=ReplayOutcome.REJECTED, reason=rejection.reason.value,
                    detail=rejection.detail))
                logger.warning("source=%s dead_letter=%d position=%d replay"
                               " rejected reason=%s detail=%s",
                               source, dl.id, dl.position,
                               rejection.reason.value, rejection.detail)
        return result
