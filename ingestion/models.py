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
