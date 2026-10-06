"""HTTP 接入协议端到端：init -> ingest -> checkpoint/rejections 可查询。"""
import logging

import pytest
from fastapi.testclient import TestClient

from main import create_app

logger = logging.getLogger("test.api")


@pytest.fixture()
def client(tmp_path):
    return TestClient(create_app(str(tmp_path / "api.db")))


def test_ingest_protocol_over_http(client):
    r = client.post("/sources/users/init",
                    json={"columns": {"id": "int", "name": "str"}})
    assert r.status_code == 200

    r = client.post("/sources/users/ingest", json={"records": [
        {"position": 1, "op": "insert", "pk": {"id": 1},
         "payload": {"id": 1, "name": "a"}},
        {"position": 2, "op": "insert", "pk": {"id": 2},
         "payload": {"id": 2, "name": 7}},          # 类型不匹配
        {"position": 1, "op": "insert", "pk": {"id": 1},
         "payload": {"id": 1, "name": "a"}},        # 批内重复
    ]})
    body = r.json()
    logger.info("ingest response: %s", body)
    assert body["applied"] == 1
    assert body["duplicates"] == 1
    assert body["rejected"] == [
        {"position": 2, "reason": "type_mismatch",
         "detail": "column 'name' expects str, got 7"},
    ]
    assert body["last_position"] == 2

    cp = client.get("/sources/users/checkpoint").json()
    assert cp["last_position"] == 2
    rej = client.get("/sources/users/rejections").json()["rejections"]
    assert rej[0]["reason"] == "type_mismatch"
    rows = client.get("/sources/users/rows").json()["rows"]
    assert rows == [{"id": 1, "name": "a"}]


def test_schema_change_over_http(client):
    client.post("/sources/users/init",
                json={"columns": {"id": "int", "name": "str"}})
    client.post("/sources/users/ingest", json={"records": [
        {"position": 1, "op": "insert", "pk": {"id": 1},
         "payload": {"id": 1, "name": "a"}}]})
    r = client.post("/sources/users/schema-changes", json={
        "position": 2, "to_version": 2, "add_columns": {"age": "int"}})
    assert r.json()["columns"]["age"] == "int"
    assert client.get("/sources/users/rows").json()["rows"][0]["age"] == 0

    client.post("/sources/users/schema-changes", json={
        "position": 3, "to_version": 3, "drop_columns": ["name"]})
    rej = client.get("/sources/users/rejections").json()["rejections"]
    logger.info("schema rejections: %s", rej)
    assert rej[0]["reason"] == "incompatible_schema_change"
    assert rej[0]["record"]["drop_columns"] == ["name"]


def test_dead_letter_triage_and_replay_over_http(client):
    """HTTP 端到端：死信筛选/标注/分页 + 受控重放。"""
    client.post("/sources/users/init",
                json={"columns": {"id": "int", "name": "str"}})
    client.post("/sources/users/ingest", json={"records": [
        {"position": p, "op": "insert", "pk": {"id": p},
         "payload": {"id": p, "name": p}} for p in range(1, 4)]})  # 3 条全拒

    # 分页 + 按原因筛选
    r = client.get("/sources/users/dead-letters",
                   params={"reason": "type_mismatch", "limit": 2, "offset": 0})
    body = r.json()
    logger.info("dead-letters page: total=%d items=%d", body["total"],
                len(body["items"]))
    assert body["total"] == 3 and len(body["items"]) == 2
    dl_id = body["items"][0]["id"]
    assert body["items"][0]["status"] == "pending"

    # 单条定位 + 标注处置状态
    r = client.post(f"/dead-letters/{dl_id}/status",
                    json={"status": "ignored", "note": "确认作废"})
    assert r.json()["dead_letter"]["status"] == "ignored"
    r = client.get("/sources/users/dead-letters",
                   params={"status": "pending"})
    assert r.json()["total"] == 2

    # 重放未修复的记录：仍被明确拒绝
    r = client.post("/sources/users/replay", json={"ids": [dl_id]})
    item = r.json()["results"][0]
    logger.info("http replay: %s", item)
    assert item["outcome"] == "rejected"
    assert item["reason"] == "type_mismatch"

    # 单条详情含重放流水
    r = client.get(f"/dead-letters/{dl_id}")
    assert r.json()["replay_history"][0]["outcome"] == "rejected"
    assert r.json()["dead_letter"]["replay_attempts"] == 1

    assert client.get("/dead-letters/9999").status_code == 404
