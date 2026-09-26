"""可配置项：从环境变量读取，带本地默认值。"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """管线配置。

    - db_path: SQLite 存储路径（本地文件，":memory:" 仅用于测试）
    - lock_timeout_seconds: 同一数据源互斥锁的等待上限
    - max_retries: 可恢复错误的最大重试次数（避免无界重试）
    - retry_backoff_seconds: 重试退避基数（指数退避）
    """

    db_path: str = "ingestion.db"
    lock_timeout_seconds: float = 5.0
    max_retries: int = 3
    retry_backoff_seconds: float = 0.05

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=os.environ.get("INGEST_DB_PATH", "ingestion.db"),
            lock_timeout_seconds=float(os.environ.get("INGEST_LOCK_TIMEOUT", "5.0")),
            max_retries=int(os.environ.get("INGEST_MAX_RETRIES", "3")),
            retry_backoff_seconds=float(os.environ.get("INGEST_RETRY_BACKOFF", "0.05")),
        )
