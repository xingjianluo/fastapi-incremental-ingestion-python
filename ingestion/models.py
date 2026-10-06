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


class DeadLetterStatus(str, enum.Enum):
    """失败记录的处置状态。"""

    PENDING = "pending"    # 待处理
    RESOLVED = "resolved"  # 已解决（重放成功或人工确认）
    IGNORED = "ignored"    # 已忽略（确认不再处理）


class ReplayOutcome(str, enum.Enum):
    """单次重放的结果。"""

    APPLIED = "applied"                  # 重放校验通过并成功应用
    REJECTED = "rejected"                # 重放后仍被明确拒绝（原因可区分）
    ALREADY_APPLIED = "already_applied"  # 此前已重放成功，幂等跳过
    NOT_FOUND = "not_found"              # 死信记录不存在


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
    """一条可单独定位、可处置、可重放的失败记录。"""

    id: int
    source: str
    position: int
    reason: FailureReason
    detail: str
    record: dict[str, Any]
    status: DeadLetterStatus = DeadLetterStatus.PENDING
    created_at: str = ""
    updated_at: str = ""
    resolution_note: str = ""
    replay_outcome: ReplayOutcome | None = None  # 最近一次重放的结果
    replay_reason: str = ""    # 重放仍被拒时的可区分原因
    replay_detail: str = ""
    replayed_at: str = ""
    replay_attempts: int = 0


@dataclass
class ReplayItemResult:
    """单条死信的重放结果。"""

    dead_letter_id: int
    source: str = ""
    position: int = 0
    outcome: ReplayOutcome = ReplayOutcome.NOT_FOUND
    reason: str = ""    # 重放被拒时的 FailureReason 值
    detail: str = ""


@dataclass
class ReplayResult:
    """一小批死信的重放汇总。"""

    results: list[ReplayItemResult] = field(default_factory=list)
    attempts: int = 1

    @property
    def applied(self) -> int:
        return sum(1 for r in self.results if r.outcome is ReplayOutcome.APPLIED)

    @property
    def rejected(self) -> int:
        return sum(1 for r in self.results if r.outcome is ReplayOutcome.REJECTED)
