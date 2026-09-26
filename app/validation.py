"""记录级校验：主键、必填字段、类型匹配。

任何不合法记录都必须以明确原因被拒绝（RecordRejected），禁止静默跳过：
- pk 缺失/为 None           -> MISSING_FIELD
- 必填列缺失（insert 场景） -> MISSING_FIELD
- 字段类型不可安全转换       -> TYPE_MISMATCH
- insert 命中已存在主键      -> PK_CONFLICT
- update/delete 目标不存在   -> UNKNOWN_PK
"""

from __future__ import annotations

from typing import Any

from .errors import FailureReason, RecordRejected
from .models import ChangeEvent, OpType

# 允许的安全转换（目标类型 -> 可接受源类型）
_SAFE_CONVERSIONS: dict[str, type | tuple[type, ...]] = {
    "int": (int,),
    "float": (int, float),
    "str": (str,),
    "bool": (bool,),
}


def _reject(reason: FailureReason, detail: str, event: ChangeEvent) -> RecordRejected:
    return RecordRejected(reason, detail, event.model_dump(mode="json"))


def validate_event(
    event: ChangeEvent,
    columns: dict[str, str],
    row_exists: bool,
    required: list[str] | None = None,
) -> None:
    """校验单条事件；不合法时抛出 RecordRejected。

    required 为注册时确定的必填列；演进新增列不在其中，缺失时由调用方
    按类型填确定缺省值（见 schema_evolution.default_for_type）。
    """
    if event.pk is None or event.pk == "":
        raise _reject(FailureReason.MISSING_FIELD, "主键 pk 缺失或为空", event)

    if event.op is OpType.INSERT:
        if row_exists:
            raise _reject(FailureReason.PK_CONFLICT, f"主键冲突: {event.pk!r} 已存在", event)
        # insert 必须提供全部必填列（演进新增列允许缺省）
        missing = [c for c in (required if required is not None else columns)
                   if c not in event.data]
        if missing:
            raise _reject(FailureReason.MISSING_FIELD, f"insert 缺少必填列: {missing}", event)
    elif event.op in (OpType.UPDATE, OpType.DELETE):
        if not row_exists:
            raise _reject(
                FailureReason.UNKNOWN_PK,
                f"{event.op.value} 目标主键不存在: {event.pk!r}",
                event,
            )
        # update 可只提供部分列，但提供的字段仍需类型匹配（delete 无 data）
        if event.op is OpType.UPDATE:
            unknown = [c for c in event.data if c not in columns]
            if unknown:
                raise _reject(
                    FailureReason.MISSING_FIELD,
                    f"update 引用了目标结构中不存在的列（可能源端已改名）: {unknown}",
                    event,
                )

    if event.op is not OpType.DELETE:
        coerce_row(event.data, columns)  # 类型校验；失败抛 TYPE_MISMATCH


def coerce_row(data: dict[str, Any], columns: dict[str, str]) -> dict[str, Any]:
    """按列定义校验字段类型（只接受安全转换），返回规范化行。"""
    coerced: dict[str, Any] = {}
    for name, type_name in columns.items():
        if name not in data:
            continue
        value = data[name]
        allowed = _SAFE_CONVERSIONS.get(type_name)
        if allowed is None:
            raise RecordRejected(
                FailureReason.TYPE_MISMATCH,
                f"未知目标类型: {type_name!r}",
            )
        # bool 是 int 的子类，int 列不接受 bool 以保持语义确定
        if type_name == "int" and isinstance(value, bool):
            raise RecordRejected(
                FailureReason.TYPE_MISMATCH,
                f"列 {name!r} 期望 int，收到 bool: {value!r}",
            )
        if not isinstance(value, allowed):
            raise RecordRejected(
                FailureReason.TYPE_MISMATCH,
                f"列 {name!r} 期望 {type_name}，收到 {type(value).__name__}: {value!r}",
            )
        coerced[name] = value
    return coerced
