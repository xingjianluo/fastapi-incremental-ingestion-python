"""按数据源的锁注册表：正常接入与受控重放共用同一把 per-source 锁。"""
from __future__ import annotations

import threading


class SourceLockRegistry:
    """为每个数据源提供一把互斥锁；不同数据源互不阻塞。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def get(self, source: str) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(source, threading.Lock())
