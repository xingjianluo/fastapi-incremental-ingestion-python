"""核心数据模型：变更记录、位点、失败原因与结果类型。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class Op(str, enum.Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class FailureReason(str, enum.Enum):
    """可区分的失败原因。"""

    POSITION_REGRESSION = "position_regression"      # 位点回退
    MISSING_PRIMARY_KEY = "missing_primary_key"      # 主键缺失
    MISSING_FIELD = "missing_field"                  # 必填字段缺失
    PRIMARY_KEY_CONFLICT = "primary_key_conflict"    # INSERT 命中已存在主键
    TYPE_MISMATCH = "type_mismatch"                  # 字段类型不匹配
    UNKNOWN_COLUMN = "unknown_column"                # 目标 schema 中不存在的列
    INCOMPATIBLE_SCHEMA_CHANGE = "incompatible_schema_change"  # 不兼容结构变更
    VALIDATION = "validation"                        # 其它校验失败


class ErrorKind(str, enum.Enum):
    RECOVERABLE = "recoverable"      # 可恢复：允许有界重试
    UNRECOVERABLE = "unrecoverable"  # 不可恢复：直接判失败


class Disposition(str, enum.Enum):
    """失败记录的人工/自动处置状态。"""

    PENDING = "pending"      # 待处理
    RESOLVED = "resolved"    # 已解决（重放成功会自动流转到此状态）
    IGNORED = "ignored"      # 已忽略（人工确认不再处理）


class ReplayState(str, enum.Enum):
    """最近一次受控重放的结果。"""

    NONE = "none"          # 从未重放
    APPLIED = "applied"    # 最近一次重放成功应用
    REJECTED = "rejected"  # 最近一次重放仍被明确拒绝


class ReplayResult(str, enum.Enum):
    """单条失败记录本次重放的处置结果。"""

    APPLIED = "applied"    # 通过与正常接入完全相同的校验并写入
    REJECTED = "rejected"  # 仍被明确拒绝（原因可区分）
    SKIPPED = "skipped"    # 非 pending（已解决/已忽略）：幂等跳过，不做任何写入


@dataclass(frozen=True)
class ChangeRecord:
    """一条源端变更。"""

    source: str
    position: int
    op: Op
    pk: dict[str, Any]
    payload: dict[str, Any] = field(default_factory=dict)
    schema_version: int = 1


@dataclass(frozen=True)
class SchemaChange:
    """一次源端结构变更。"""

    source: str
    position: int
    to_version: int
    add_columns: dict[str, str] = field(default_factory=dict)   # name -> 类型名
    drop_columns: tuple[str, ...] = ()
    rename_columns: dict[str, str] = field(default_factory=dict)  # old -> new


@dataclass(frozen=True)
class Rejection:
    """一条被明确拒绝的记录及其原因。"""

    source: str
    position: int
    reason: FailureReason
    detail: str
    record: dict[str, Any]


@dataclass
class IngestResult:
    """一批变更的应用结果。"""

    source: str
    applied: int = 0
    duplicates: int = 0
    rejected: list[Rejection] = field(default_factory=list)
    last_position: int = 0
    attempts: int = 1


@dataclass
class DeadLetter:
    """一条被明确拒绝的记录及其处置/重放状态。"""

    id: int
    source: str
    position: int
    reason: FailureReason
    detail: str
    record: dict[str, Any]
    created_at: str
    disposition: Disposition = Disposition.PENDING
    handled_at: str | None = None
    note: str = ""
    replay_state: ReplayState = ReplayState.NONE
    last_replayed_at: str | None = None
    replay_reason: str | None = None
    replay_detail: str | None = None
    replay_attempts: int = 0


@dataclass
class ReplayItemResult:
    """单条死信的重放结果。"""

    dead_letter_id: int
    result: ReplayResult
    reason: FailureReason | None = None  # 再次被拒时的可区分原因
    detail: str = ""
    attempts: int = 1                    # 本条经历的有界重试次数


@dataclass
class ReplayBatchResult:
    """一批受控重放的汇总结果。"""

    source: str
    items: list[ReplayItemResult] = field(default_factory=list)

    @property
    def applied(self) -> int:
        return sum(i.result is ReplayResult.APPLIED for i in self.items)

    @property
    def rejected(self) -> int:
        return sum(i.result is ReplayResult.REJECTED for i in self.items)

    @property
    def skipped(self) -> int:
        return sum(i.result is ReplayResult.SKIPPED for i in self.items)

    @property
    def attempts(self) -> int:
        return max((i.attempts for i in self.items), default=1)
