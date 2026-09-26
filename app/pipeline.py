"""接入管线核心：位点校验、幂等去重、记录校验、结构演进、有限重试。

处理规则（全部在数据源互斥锁内按 seq 升序执行）：
- seq <= 当前水位 且 event_id 已应用 -> 重复消费：幂等跳过，计入 duplicates；
- seq <= 当前水位 且 event_id 未见过 -> 位点回退：拒绝（WATERMARK_REGRESSION）；
- 记录校验失败（主键冲突/字段缺失/类型不匹配）-> 拒绝并写死信，水位随死信推进，
  保证同一坏记录不会被无限重投；
- 不兼容结构变更 -> 整批中止，原始变更记录写死信（SCHEMA_INCOMPATIBLE），
  已应用部分保持提交，水位停在最后一个成功位点；
- 可恢复错误（TRANSIENT/锁竞争）-> 指数退避有限重试，次数受 max_retries 限制。
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from .config import Settings
from .errors import (
    FailureReason,
    RecordRejected,
    SchemaIncompatibleError,
    TransientStorageError,
)
from .locker import SourceLocker
from .models import ChangeEvent, IngestBatch, IngestResult, RejectedRecord, SchemaChange
from .schema_evolution import default_for_type, ensure_compatible
from .storage import Storage
from .validation import coerce_row, validate_event

logger = logging.getLogger("ingestion.pipeline")


class IngestionPipeline:
    def __init__(self, storage: Storage, locker: SourceLocker, settings: Settings) -> None:
        self._storage = storage
        self._locker = locker
        self._settings = settings

    # ---- 对外入口 ----
    def ingest(self, batch: IngestBatch) -> IngestResult:
        """消费一个批次：互斥、有序、幂等、失败可区分。"""
        attempt = 0
        while True:
            attempt += 1
            try:
                with self._locker.acquire(batch.source):
                    # 存在性检查在锁内进行：避免并发下无锁访问存储
                    if not self._storage.source_exists(batch.source):
                        raise RecordRejected(
                            FailureReason.UNKNOWN_SOURCE,
                            f"数据源未注册: {batch.source!r}",
                        )
                    result = self._ingest_once(batch)
                result.attempts = attempt
                return result
            except TransientStorageError:
                if attempt > self._settings.max_retries:
                    logger.error(
                        "source=%s 可恢复错误重试 %d 次后仍失败，放弃（避免无界重试）",
                        batch.source, self._settings.max_retries,
                    )
                    raise
                backoff = self._settings.retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "source=%s 第 %d 次尝试遇到可恢复错误，%.3fs 后重试",
                    batch.source, attempt, backoff,
                )
                time.sleep(backoff)

    # ---- 单次执行（已持有互斥锁）----
    def _ingest_once(self, batch: IngestBatch) -> IngestResult:
        items = self._ordered_items(batch)
        result = IngestResult(source=batch.source)
        for seq, kind, item in items:
            if kind == "schema":
                stop = self._handle_schema_change(batch.source, item, result)
            else:
                stop = self._handle_event(batch.source, item, result)
            if stop:
                result.status = "failed"
                break
        result.watermark = self._storage.get_watermark(batch.source)
        if result.status != "failed":
            result.status = "ok" if not result.rejected else "partial"
        logger.info(
            "source=%s status=%s applied=%d duplicates=%d rejected=%d watermark=%d",
            batch.source, result.status, result.applied,
            result.duplicates, len(result.rejected), result.watermark,
        )
        return result

    @staticmethod
    def _ordered_items(batch: IngestBatch) -> list[tuple[int, str, Any]]:
        """数据事件与结构变更按 seq 升序混排；同 seq 时结构变更先行。"""
        items: list[tuple[int, str, Any]] = [
            (sc.seq, "schema", sc) for sc in batch.schema_changes
        ] + [(ev.seq, "event", ev) for ev in batch.events]
        items.sort(key=lambda t: (t[0], 0 if t[1] == "schema" else 1))
        return items

    def _check_position(self, source: str, event_id: str, seq: int) -> str:
        """返回 'apply' / 'duplicate'；位点回退抛 RecordRejected。"""
        watermark = self._storage.get_watermark(source)
        if self._storage.event_seen(source, event_id):
            return "duplicate"
        if seq <= watermark:
            raise RecordRejected(
                FailureReason.WATERMARK_REGRESSION,
                f"位点回退: seq={seq} <= 当前水位 {watermark}",
            )
        return "apply"

    def _handle_event(self, source: str, event: ChangeEvent, result: IngestResult) -> bool:
        """处理数据事件；返回 True 表示整批中止。"""
        try:
            action = self._check_position(source, event.event_id, event.seq)
        except RecordRejected as exc:
            exc.event = event.model_dump(mode="json")
            self._reject(source, event.event_id, event.seq, exc, result)
            return False
        if action == "duplicate":
            result.duplicates += 1
            logger.info(
                "source=%s 重复消费 event_id=%s seq=%d，幂等跳过",
                source, event.event_id, event.seq,
            )
            return False
        try:
            columns = self._storage.get_columns(source)
            required = self._storage.get_required_columns(source)
            exists = self._storage.row_exists(source, event.pk)
            validate_event(event, columns, exists, required)
            if event.op.value == "delete":
                row = None
            else:
                row = coerce_row(event.data, columns)
                if event.op.value == "insert":
                    # 演进新增列缺失时填确定缺省值（稳定缺省语义）
                    for name, type_name in columns.items():
                        if name not in row:
                            row[name] = default_for_type(type_name)
            self._storage.apply_event(
                source, event.event_id, event.seq, event.op.value, event.pk, row
            )
            result.applied += 1
        except RecordRejected as exc:
            self._reject(source, event.event_id, event.seq, exc, result)
        return False

    def _handle_schema_change(
        self, source: str, change: SchemaChange, result: IngestResult
    ) -> bool:
        """处理结构变更；不兼容时写死信并中止整批。"""
        try:
            action = self._check_position(source, change.event_id, change.seq)
        except RecordRejected as exc:
            exc.event = change.model_dump(mode="json")
            self._reject(source, change.event_id, change.seq, exc, result)
            return False
        if action == "duplicate":
            result.duplicates += 1
            return False
        current = self._storage.get_columns(source)
        try:
            ensure_compatible(current, change.columns)
        except SchemaIncompatibleError as exc:
            exc.raw_change = change.model_dump(mode="json")
            self._reject(source, change.event_id, change.seq, exc, result)
            logger.error(
                "source=%s 不兼容结构变更 seq=%d 已写入死信并中止批次: %s",
                source, change.seq, exc.detail,
            )
            return True
        version = self._storage.evolve_schema(
            source, change.columns, change.event_id, change.seq
        )
        result.applied += 1
        logger.info(
            "source=%s 结构演进至 v%d（新增列缺省值按类型确定）",
            source, version,
        )
        return False

    def _reject(
        self,
        source: str,
        event_id: str,
        seq: int,
        exc: RecordRejected | SchemaIncompatibleError,
        result: IngestResult,
    ) -> None:
        payload = json.dumps(
            getattr(exc, "event", None) or getattr(exc, "raw_change", None),
            ensure_ascii=False,
        )
        self._storage.record_dead_letter(
            source, event_id, seq, exc.reason.value, exc.detail, payload
        )
        result.rejected.append(
            RejectedRecord(
                event_id=event_id, seq=seq, reason=exc.reason, detail=exc.detail
            )
        )
        logger.warning(
            "source=%s 拒绝 event_id=%s seq=%d reason=%s detail=%s",
            source, event_id, seq, exc.reason.value, exc.detail,
        )
