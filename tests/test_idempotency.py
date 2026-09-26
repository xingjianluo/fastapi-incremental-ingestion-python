"""重复消费与进程重启场景：目标数据不得重复或丢失。"""
import logging

from ingestion.models import ChangeRecord, Op
from ingestion.store import Store

logger = logging.getLogger("test.idempotency")


def _rec(pos, pk_id, name="a"):
    return ChangeRecord("users", pos, Op.INSERT, {"id": pk_id},
                        {"id": pk_id, "name": name})


def test_duplicate_consumption_is_idempotent(store, schema, pipeline):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    batch = [_rec(1, 1), _rec(2, 2), _rec(3, 3)]
    r1 = pipeline.ingest("users", batch)
    logger.info("first ingest: applied=%d last_position=%d", r1.applied,
                r1.last_position)
    assert (r1.applied, r1.duplicates) == (3, 0)

    # 整批重复消费 + 单条重复消费
    r2 = pipeline.ingest("users", batch)
    r3 = pipeline.ingest("users", [_rec(2, 2)])
    logger.info("reconsume: duplicates=%d/%d", r2.duplicates, r3.duplicates)
    assert (r2.applied, r2.duplicates) == (0, 3)
    assert (r3.applied, r3.duplicates) == (0, 1)
    assert store.get_rows("users") == [
        {"id": 1, "name": "a"}, {"id": 2, "name": "a"}, {"id": 3, "name": "a"},
    ]
    assert store.get_checkpoint("users") == 3


def test_restart_preserves_state(store, schema, pipeline, tmp_path):
    schema.ensure_source("users", {"id": "int", "name": "str"})
    pipeline.ingest("users", [_rec(1, 1), _rec(2, 2)])
    db_path = store.db_path
    store.close()

    # 模拟进程重启：同一 db 文件重建全部组件
    store2 = Store(db_path)
    schema2 = type(schema)(store2)
    pipeline2 = type(pipeline)(store2, schema2, pipeline._config)
    assert store2.get_checkpoint("users") == 2
    assert len(store2.get_rows("users")) == 2

    # 重启后重复消费旧位点仍幂等，新位点正常推进
    r = pipeline2.ingest("users", [_rec(2, 2), _rec(3, 3)])
    logger.info("after restart: applied=%d duplicates=%d last_position=%d",
                r.applied, r.duplicates, r.last_position)
    assert (r.applied, r.duplicates) == (1, 1)
    assert store2.get_checkpoint("users") == 3
    assert len(store2.get_rows("users")) == 3
    store2.close()
