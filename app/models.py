"""接入协议的数据模型：变更事件、批次、结果与数据源状态。"""

from __future__ import annotations

import enum
from typing import Any, Literal

from pydantic import BaseModel, Field

from .errors import FailureReason


class OpType(str, enum.Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class ChangeEvent(BaseModel):
    """单条变更事件（接入协议的基本单元）。

    - seq: 数据源内单调递增的位点；重复投递同一 seq 视为重复消费（幂等跳过），
      seq 小于当前水位视为位点回退（拒绝）。
    - event_id: 全局唯一事件标识，用于跨重启去重。
    """

    event_id: str
    seq: int
    op: OpType
    pk: Any
    data: dict[str, Any] = Field(default_factory=dict)


class SchemaChange(BaseModel):
    """源端表结构变更通知。"""

    event_id: str
    seq: int
    columns: dict[str, str]  # 变更后的完整列定义：列名 -> 类型名


class IngestBatch(BaseModel):
    """一次触发投递的内容：数据事件与结构变更按 seq 混排。"""

    source: str
    events: list[ChangeEvent] = Field(default_factory=list)
    schema_changes: list[SchemaChange] = Field(default_factory=list)


class RejectedRecord(BaseModel):
    """被明确拒绝的记录（含可区分原因）。"""

    event_id: str
    seq: int
    reason: FailureReason
    detail: str


class IngestResult(BaseModel):
    """一次接入的结果汇总。"""

    source: str
    applied: int = 0
    duplicates: int = 0
    rejected: list[RejectedRecord] = Field(default_factory=list)
    watermark: int = 0
    attempts: int = 1
    status: Literal["ok", "partial", "failed"] = "ok"


class SourceState(BaseModel):
    """数据源当前状态（位点、结构版本、统计）。"""

    source: str
    watermark: int
    schema_version: int
    applied_events: int
    dead_letters: int
