"""源端结构变更的兼容性判定与目标端结构演进。

兼容规则：
- 新增列：兼容。缺省语义稳定确定——按列类型填充固定默认值（见 default_for_type），
  存量行与缺失该字段的新事件都使用同一缺省值。
- 删除列：不兼容。目标端已有数据列不允许静默丢弃。
- 列类型变更：不兼容（含改名场景：改名在协议上表现为“删旧列+增新列”，
  删列部分即判定不兼容，避免目标端静默错列）。
不兼容变更由调用方把原始变更记录写入死信队列，标记为可追溯失败。
"""

from __future__ import annotations

from .errors import SchemaIncompatibleError

# 类型名 -> 新增列的确定缺省值
_DEFAULTS: dict[str, object] = {
    "int": 0,
    "float": 0.0,
    "str": "",
    "bool": False,
}

SUPPORTED_TYPES = frozenset(_DEFAULTS)


def default_for_type(type_name: str) -> object:
    """新增列的稳定确定缺省值：按类型给固定默认值。"""
    if type_name not in _DEFAULTS:
        raise SchemaIncompatibleError(f"不支持的列类型: {type_name!r}")
    return _DEFAULTS[type_name]


def diff_columns(old: dict[str, str], new: dict[str, str]) -> dict[str, object]:
    """比较新旧列定义，返回 added/removed/type_changed 三类差异。"""
    added = {k: v for k, v in new.items() if k not in old}
    removed = {k: v for k, v in old.items() if k not in new}
    type_changed = {
        k: {"from": old[k], "to": new[k]}
        for k in old.keys() & new.keys()
        if old[k] != new[k]
    }
    return {"added": added, "removed": removed, "type_changed": type_changed}


def ensure_compatible(old: dict[str, str], new: dict[str, str]) -> None:
    """校验结构变更是否兼容；不兼容抛 SchemaIncompatibleError。"""
    for name, t in new.items():
        if t not in SUPPORTED_TYPES:
            raise SchemaIncompatibleError(f"列 {name!r} 使用不支持的类型 {t!r}")
    diff = diff_columns(old, new)
    removed: dict[str, str] = diff["removed"]  # type: ignore[assignment]
    type_changed: dict[str, object] = diff["type_changed"]  # type: ignore[assignment]
    if removed:
        raise SchemaIncompatibleError(
            f"删除列 {sorted(removed)} 不兼容：目标端已有数据列不允许静默丢弃"
        )
    if type_changed:
        raise SchemaIncompatibleError(
            f"列类型变更 {type_changed} 不兼容：禁止目标端静默错列"
        )
