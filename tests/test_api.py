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


def test_disposition_and_replay_protocol_over_http(client, tmp_path):
    """死信分页/标注/受控重放的 HTTP 端到端。"""
    import sqlite3
    client.post("/sources/users/init",
                json={"columns": {"id": "int", "name": "str"}})
    client.post("/sources/users/ingest", json={"records": [
        {"position": 1, "op": "insert", "pk": {"id": 1},
         "payload": {"id": 1}}]})  # 缺 name -> 死信

    # 分页筛选
    page = client.get("/dead-letters",
                      params={"source": "users", "reason": "missing_field",
                              "limit": 10, "offset": 0}).json()
    assert page["total"] == 1
    dl_id = page["items"][0]["id"]
    assert page["items"][0]["disposition"] == "pending"
    logger.info("listed dead letter: %s", page["items"][0])

    # 单条定位与 404
    assert client.get(f"/dead-letters/{dl_id}").json()["id"] == dl_id
    assert client.get("/dead-letters/999").status_code == 404

    # 没修就重放：仍拒
    r = client.post("/replay", json={"dead_letter_ids": [dl_id]}).json()
    assert (r["rejected"], r["applied"], r["skipped"]) == (1, 0, 0)
    assert r["items"][0]["reason"] == "missing_field"

    # 源端结构修复（目标不再要求 name）后重放成功
    db = str(tmp_path / "api.db")
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM schema_registry WHERE source='users'")
    conn.execute("INSERT INTO schema_registry (source, column_name, type_name)"
                 " VALUES ('users', 'id', 'int')")
    conn.commit(); conn.close()
    r = client.post("/replay", json={"dead_letter_ids": [dl_id]}).json()
    logger.info("replay after fix: %s", r)
    assert (r["rejected"], r["applied"]) == (0, 1)
    assert client.get("/sources/users/rows").json()["rows"] == [{"id": 1}]
    assert client.get("/sources/users/checkpoint").json()["last_position"] == 1

    # 重复重放幂等跳过；流水可追溯；状态已 resolved
    r = client.post("/replay", json={"dead_letter_ids": [dl_id]}).json()
    assert r["skipped"] == 1 and r["applied"] == 0
    events = client.get(f"/dead-letters/{dl_id}/replays").json()["events"]
    assert [e["result"] for e in events] == ["rejected", "applied"]

    # 标注忽略 / 非法请求
    marked = client.post(f"/dead-letters/{dl_id}/disposition",
                         json={"disposition": "ignored", "note": "ok"}).json()
    assert marked["disposition"] == "ignored" and marked["note"] == "ok"
    assert client.post("/replay", json={"dead_letter_ids": []}).status_code == 400
