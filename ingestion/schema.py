"""源端结构变更的兼容处理。

兼容规则：
- 新增列：兼容。按类型映射到稳定确定的缺省值，并为存量行回填。
- 删列 / 改名：不兼容。不修改目标 schema，由管线把原始变更记录
  写入死信表并标记为 INCOMPATIBLE_SCHEMA_CHANGE，保证可追溯。
"""
from __future__ import annotations

from typing import Any

from .models import SchemaChange
from .store import Store

# 类型名 -> 稳定确定的缺省值
DEFAULTS: dict[str, Any] = {
    "int": 0,
    "float": 0.0,
    "str": "",
    "bool": False,
}


def value_matches(type_name: str, value: Any) -> bool:
    """校验值是否与注册类型匹配（bool 不视为 int）。"""
    if type_name == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if type_name == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if type_name == "str":
        return isinstance(value, str)
    if type_name == "bool":
        return isinstance(value, bool)
    return False


class SchemaManager:
    """按数据源维护目标 schema 并评估结构变更兼容性。"""

    def __init__(self, store: Store) -> None:
        self._store = store

    def ensure_source(self, source: str, columns: dict[str, str]) -> None:
        """首次接入时注册初始 schema；已存在则保持不变。"""
        if not self._store.get_columns(source):
            self._store.set_columns(source, columns)

    def is_compatible(self, change: SchemaChange) -> bool:
        """仅包含新增列的变更为兼容；删列/改名一律不兼容。"""
        return not change.drop_columns and not change.rename_columns

    def apply_change(self, change: SchemaChange) -> None:
        """应用兼容变更：逐列注册并回填缺省值。"""
        if not self.is_compatible(change):
            raise ValueError(f"incompatible schema change: {change}")
        existing = self._store.get_columns(change.source)
        for name, type_name in sorted(change.add_columns.items()):
            if name not in existing:
                self._store.add_column(
                    change.source, name, type_name, self.default_for(type_name)
                )

    def default_for(self, type_name: str) -> Any:
        if type_name not in DEFAULTS:
            raise ValueError(f"unknown column type: {type_name}")
        return DEFAULTS[type_name]
