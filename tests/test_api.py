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
