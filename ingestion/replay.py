"""失败记录的受控重放。

与位点、并发的自洽规则：
- 被拒记录当初已经推进过 checkpoint（位点只进不退）。重放是**位点之外的受控
  补偿通道**：不读取/不校验/不回退 checkpoint，也不写 applied_positions 流水，
  因此既不会被"重复消费"判定拦掉，也不会倒回或污染已经落地的位点状态。
- 重放走与正常接入**完全相同**的校验与写入语义（复用 validate_record 与
  target_rows 的 upsert）：能过的过，该拒的仍按可区分原因拒掉；INSERT 命中
  已存在主键会得到 primary_key_conflict，绝不因走重放而覆盖已落地数据。
- 同一数据源的重放与正常接入共用同一把 per-source 锁，按序推进；不同数据源
  互不阻塞。重放内的可恢复错误沿用现有有界重试，不可恢复错误直接失败。
- 幂等：只有 disposition=pending 的死信可重放；重复重放同一条（已 resolved/
  ignored）直接跳过，不产生重复数据、不改状态。每次重放都追加 replay_events
  流水（实际执行才记），原死信上保留最近一次结果与累计尝试次数，全程可追溯。
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable

from .locks import SourceLockRegistry
from .models import (
    ChangeRecord,
    DeadLetter,
    Disposition,
    ErrorKind,
    FailureReason,
    Op,
    ReplayBatchResult,
    ReplayItemResult,
    ReplayResult,
    SchemaChange,
)
from .pipeline import PipelineConfig, classify_error, validate_record
from .schema import SchemaManager
from .store import DeadLetterNotPending, Store

logger = logging.getLogger("ingestion.replay")


class DeadLetterNotFound(KeyError):
    """请求重放/查看的死信不存在。"""


class ReplayService:
    """失败记录处置状态查询/标注与受控重放。"""

    def __init__(
        self,
        store: Store,
        schema: SchemaManager,
        config: PipelineConfig,
        locks: SourceLockRegistry,
    ) -> None:
        self._store = store
        self._schema = schema
        self._config = config
        self._locks = locks

    # ---- 定位 / 筛选 / 处置标注 ----
    def get_dead_letter(self, dead_letter_id: int) -> DeadLetter:
        dl = self._store.get_dead_letter(dead_letter_id)
        if dl is None:
            raise DeadLetterNotFound(dead_letter_id)
        return dl

    def list_dead_letters(
        self,
        source: str | None = None,
        disposition: str | None = None,
        reason: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[DeadLetter], int]:
        return self._store.query_dead_letters(
            source=source, disposition=disposition, reason=reason,
            limit=limit, offset=offset)

    def list_replay_events(self, dead_letter_id: int) -> list[dict]:
        self.get_dead_letter(dead_letter_id)  # 不存在直接报错
        return self._store.list_replay_events(dead_letter_id)

    def mark(
        self, dead_letter_id: int, disposition: Disposition, note: str = ""
    ) -> DeadLetter:
        dl = self._store.mark_disposition(dead_letter_id, disposition, note)
        if dl is None:
            raise DeadLetterNotFound(dead_letter_id)
        logger.info("dead_letter=%d source=%s marked disposition=%s note=%r",
                    dead_letter_id, dl.source, disposition.value, note)
        return dl

    # ---- 受控重放 ----
    def replay(self, dead_letter_ids: list[int]) -> ReplayBatchResult:
        """对一条或一小批死信发起受控重放。

        同一数据源串行、按 id 升序推进；不同数据源的条目互不阻塞。
        """
        if not dead_letter_ids:
            raise ValueError("dead_letter_ids must not be empty")
        ids = sorted(set(dead_letter_ids))
        if len(ids) > self._config.max_replay_batch_size:
            raise ValueError(
                f"replay batch size {len(ids)} exceeds"
                f" max_replay_batch_size={self._config.max_replay_batch_size}")

        # 先解析并校验存在性，同时按数据源分组
        grouped: dict[str, list[DeadLetter]] = {}
        for dl_id in ids:
            dl = self.get_dead_letter(dl_id)
            grouped.setdefault(dl.source, []).append(dl)

        batch = ReplayBatchResult(source=next(iter(grouped))
                                  if len(grouped) == 1 else "")
        for source, dls in grouped.items():
            with self._locks.get(source):  # 与该源正常接入互斥
                for dl in sorted(dls, key=lambda d: d.id):
                    batch.items.append(self._replay_one(dl))
        logger.info(
            "replay batch size=%d applied=%d rejected=%d skipped=%d",
            len(batch.items), batch.applied, batch.rejected, batch.skipped)
        return batch

    def _replay_one(self, dl: DeadLetter) -> ReplayItemResult:
        # 锁内重新读取最新状态：非 pending（已解决/已忽略/已重放）一律幂等跳过
        current = self._store.get_dead_letter(dl.id)
        assert current is not None
        if current.disposition is not Disposition.PENDING:
            logger.info("dead_letter=%d source=%s disposition=%s, replay skipped",
                        dl.id, dl.source, current.disposition.value)
            return ReplayItemResult(dl.id, ReplayResult.SKIPPED)

        is_schema_change = "op" not in current.record
        action: Callable[[], None]
        if is_schema_change:
            action = self._schema_action(current)
        else:
            action = self._data_action(current)
        return self._with_retry(current, action)

    def _data_action(self, dl: DeadLetter) -> Callable[[], None]:
        rec_dict = dl.record
        rec = ChangeRecord(
            source=dl.source, position=dl.position, op=Op(rec_dict["op"]),
            pk=rec_dict.get("pk", {}), payload=rec_dict.get("payload", {}),
            schema_version=rec_dict.get("schema_version", 1),
        )

        def action() -> None:
            # 与正常接入完全相同的校验（用当前最新 schema 重新判定）
            columns = self._store.get_columns(dl.source)
            rejection = validate_record(self._store, rec, columns)
            if rejection is not None:
                self._store.replay_reject(
                    dl, rejection.reason.value, rejection.detail)
                logger.warning(
                    "replay dead_letter=%d source=%s position=%d rejected"
                    " reason=%s detail=%s",
                    dl.id, dl.source, dl.position,
                    rejection.reason.value, rejection.detail)
                return
            # 与正常接入相同的写入语义；upsert 保证重复重放不产生重复行
            self._store.replay_apply(dl, rec)
            logger.info(
                "replay dead_letter=%d source=%s position=%d applied op=%s",
                dl.id, dl.source, dl.position, rec.op.value)

        return action

    def _schema_action(self, dl: DeadLetter) -> Callable[[], None]:
        rec = dl.record
        change = SchemaChange(
            source=dl.source, position=dl.position,
            to_version=rec.get("to_version", 1),
            add_columns=rec.get("add_columns", {}),
            drop_columns=tuple(rec.get("drop_columns", [])),
            rename_columns=rec.get("rename_columns", {}),
        )

        def action() -> None:
            # 用当前结构重新评估兼容性：删列/改名在任何时候都不会被放行
            if not self._schema.is_compatible(change):
                from .models import Rejection
                rejection = Rejection(
                    source=dl.source, position=dl.position,
                    reason=FailureReason.INCOMPATIBLE_SCHEMA_CHANGE,
                    detail=f"drop={list(change.drop_columns)} "
                           f"rename={change.rename_columns}",
                    record=dl.record,
                )
                self._store.replay_reject(
                    dl, rejection.reason.value, rejection.detail)
                logger.warning(
                    "replay schema dead_letter=%d source=%s position=%d"
                    " still incompatible: %s",
                    dl.id, dl.source, dl.position, rejection.detail)
                return
            self._schema.apply_change(change)  # 含列注册与存量回填
            self._store.replay_schema_apply(dl)
            logger.info(
                "replay schema dead_letter=%d source=%s position=%d applied",
                dl.id, dl.source, dl.position)

        return action

    def _with_retry(
        self, dl: DeadLetter, action: Callable[[], None]
    ) -> ReplayItemResult:
        """对单条重放走有界重试；返回最终结果。

        拒绝/成功是 action 内部正常提交的业务结果（不重试）；只有存储层抛出
        可恢复错误时才退避重试，不可恢复错误直接向上抛，绝不无界重试。
        """
        attempt = 0
        while True:
            try:
                action()
                break
            except DeadLetterNotPending:
                # 锁内仍可能与显式标注并发：按幂等跳过处理
                logger.info("dead_letter=%d not pending at commit, skipped", dl.id)
                return ReplayItemResult(dl.id, ReplayResult.SKIPPED,
                                        attempts=attempt + 1)
            except Exception as exc:
                if classify_error(exc) is ErrorKind.UNRECOVERABLE:
                    logger.error("replay dead_letter=%d unrecoverable: %r",
                                 dl.id, exc)
                    raise
                attempt += 1
                if attempt > self._config.max_retries:
                    logger.error(
                        "replay dead_letter=%d retries exhausted after %d",
                        dl.id, attempt)
                    raise
                delay = self._config.backoff_seconds * attempt
                logger.warning(
                    "replay dead_letter=%d recoverable %r, retry %d in %.3fs",
                    dl.id, exc, attempt, delay)
                time.sleep(delay)

        refreshed = self._store.get_dead_letter(dl.id)
        assert refreshed is not None
        result = (ReplayResult.APPLIED if refreshed.disposition is Disposition.RESOLVED
                  else ReplayResult.REJECTED)
        reason = (FailureReason(refreshed.replay_reason)
                  if refreshed.replay_reason and result is ReplayResult.REJECTED
                  else None)
        return ReplayItemResult(dl.id, result, reason=reason,
                                detail=refreshed.replay_detail or "",
                                attempts=attempt + 1)
