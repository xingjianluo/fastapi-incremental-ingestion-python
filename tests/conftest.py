"""测试夹具：每个用例使用独立临时库；用例结束自动打印位点与变更流水。"""

from __future__ import annotations

import json
import logging

import pytest

from app.config import Settings
from app.locker import SourceLocker
from app.models import ChangeEvent
from app.pipeline import IngestionPipeline
from app.storage import Storage

logger = logging.getLogger("ingestion.test")


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture()
def settings(db_path):
    return Settings(db_path=db_path, lock_timeout_seconds=1.0, max_retries=3,
                    retry_backoff_seconds=0.01)


@pytest.fixture()
def storage(db_path):
    st = Storage(db_path)
    yield st
    st.close()


@pytest.fixture()
def pipeline(storage, settings, db_path):
    return IngestionPipeline(storage, SourceLocker(db_path, settings.lock_timeout_seconds),
                             settings)


def make_event(event_id: str, seq: int, op: str = "insert", pk=None, data=None) -> ChangeEvent:
    return ChangeEvent(
        event_id=event_id,
        seq=seq,
        op=op,
        pk=pk if pk is not None else event_id,
        data=data if data is not None else {"name": "n", "age": 1},
    )


@pytest.fixture(autouse=True)
def dump_state_after_test(request, storage):
    """用例结束后打印各数据源的位点与变更流水，保证失败可解释。"""
    yield
    if storage.is_closed:
        return
    for source in storage.list_sources():
        state = storage.source_state(source)
        events = storage.list_events(source)
        dead = storage.list_dead_letters(source)
        logger.info(
            "[state-dump] test=%s source=%s watermark=%d schema_version=%d\n"
            "  applied_events=%s\n  dead_letters=%s",
            request.node.name, source, state["watermark"], state["schema_version"],
            json.dumps(events, ensure_ascii=False),
            json.dumps(dead, ensure_ascii=False),
        )
        print(
            f"\n[state-dump] test={request.node.name} source={source} "
            f"watermark={state['watermark']} schema_version={state['schema_version']}"
        )
        for e in events:
            print(f"  applied: seq={e['seq']} op={e['op']} event_id={e['event_id']} pk={e['pk']}")
        for d in dead:
            print(f"  dead:    seq={d['seq']} event_id={d['event_id']} "
                  f"reason={d['reason']} detail={d['detail']}")
