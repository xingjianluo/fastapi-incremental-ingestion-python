"""HTTP 层端到端：注册、接入、状态/数据/流水/死信查询。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.service import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(Settings(db_path=str(tmp_path / "api.db")))
    return TestClient(app)


def test_full_flow_over_http(client):
    assert client.post("/sources/users/register", json={
        "columns": {"name": "str", "age": "int"}
    }).status_code == 200

    batch = {"source": "users", "events": [
        {"event_id": "e1", "seq": 1, "op": "insert", "pk": "u1",
         "data": {"name": "a", "age": 1}},
        {"event_id": "e2", "seq": 2, "op": "insert", "pk": "u2",
         "data": {"name": "b", "age": 2}},
    ]}
    r = client.post("/sources/users/ingest", json=batch)
    assert r.status_code == 200 and r.json()["applied"] == 2

    # 重复消费
    r = client.post("/sources/users/ingest", json=batch)
    assert r.json()["duplicates"] == 2

    # 结构演进：新增列
    r = client.post("/sources/users/ingest", json={
        "source": "users",
        "schema_changes": [{"event_id": "sc1", "seq": 3,
                            "columns": {"name": "str", "age": "int", "city": "str"}}],
    })
    assert r.json()["applied"] == 1

    state = client.get("/sources/users/state").json()
    assert state["watermark"] == 3 and state["schema_version"] == 2

    rows = client.get("/sources/users/rows").json()["rows"]
    assert len(rows) == 2 and rows[0]["city"] == ""

    events = client.get("/sources/users/events").json()["events"]
    assert [e["seq"] for e in events] == [1, 2, 3]

    # 坏记录 -> 死信可查
    r = client.post("/sources/users/ingest", json={"source": "users", "events": [
        {"event_id": "bad", "seq": 4, "op": "insert", "pk": "u3",
         "data": {"name": "x", "age": "old"}},
    ]})
    assert r.json()["rejected"][0]["reason"] == "TYPE_MISMATCH"
    dead = client.get("/sources/users/dead-letters").json()["dead_letters"]
    assert dead[0]["reason"] == "TYPE_MISMATCH" and dead[0]["event_id"] == "bad"

    assert "users" in client.get("/sources").json()["sources"]


def test_unknown_source_returns_distinguishable_error(client):
    r = client.post("/sources/ghost/ingest", json={"source": "ghost", "events": []})
    assert r.status_code == 422
    assert r.json()["reason"] == "UNKNOWN_SOURCE"


def test_source_mismatch_rejected(client):
    client.post("/sources/a/register", json={"columns": {"name": "str"}})
    r = client.post("/sources/a/ingest", json={"source": "b", "events": []})
    assert r.status_code == 400
