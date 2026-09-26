"""可区分的失败原因与异常类型。

所有记录级 / 批次级失败都必须落入 FailureReason 中的一种，
禁止静默跳过。
"""

from __future__ import annotations

import enum
from typing import Any


class FailureReason(str, enum.Enum):
    """可区分的失败原因。"""

    WATERMARK_REGRESSION = "WATERMARK_REGRESSION"  # 位点回退：seq 小于当前水位
    PK_CONFLICT = "PK_CONFLICT"                    # 主键冲突：insert 命中已存在主键
    MISSING_FIELD = "MISSING_FIELD"                # 必填字段缺失
    TYPE_MISMATCH = "TYPE_MISMATCH"                # 字段类型不匹配且不可安全转换
    SCHEMA_INCOMPATIBLE = "SCHEMA_INCOMPATIBLE"    # 不兼容的结构变更（删列/改名/改类型）
    UNKNOWN_SOURCE = "UNKNOWN_SOURCE"              # 数据源未注册
    UNKNOWN_PK = "UNKNOWN_PK"                      # update/delete 目标主键不存在
    LOCK_TIMEOUT = "LOCK_TIMEOUT"                  # 并发触发时等待互斥锁超时
    TRANSIENT = "TRANSIENT"                        # 可恢复的临时错误（可重试）


# 可恢复错误：允许有限次重试
RECOVERABLE_REASONS: frozenset[FailureReason] = frozenset(
    {FailureReason.LOCK_TIMEOUT, FailureReason.TRANSIENT}
)


class IngestError(Exception):
    """接入管线基础异常。"""

    reason: FailureReason = FailureReason.TRANSIENT
    recoverable: bool = False


class RecordRejected(IngestError):
    """单条记录被拒绝（不可恢复，需进入失败流水）。"""

    recoverable = False

    def __init__(self, reason: FailureReason, detail: str, event: dict[str, Any] | None = None):
        super().__init__(f"{reason.value}: {detail}")
        self.reason = reason
        self.detail = detail
        self.event = event


class SchemaIncompatibleError(IngestError):
    """不兼容结构变更（不可恢复，原始变更记录需保留）。"""

    reason = FailureReason.SCHEMA_INCOMPATIBLE
    recoverable = False

    def __init__(self, detail: str, raw_change: dict[str, Any] | None = None):
        super().__init__(f"{self.reason.value}: {detail}")
        self.detail = detail
        self.raw_change = raw_change


class SourceLockTimeout(IngestError):
    """同一数据源并发触发时等待互斥锁超时（可恢复）。"""

    reason = FailureReason.LOCK_TIMEOUT
    recoverable = True


class TransientStorageError(IngestError):
    """存储层临时错误（可恢复，有限重试）。"""

    reason = FailureReason.TRANSIENT
    recoverable = True
