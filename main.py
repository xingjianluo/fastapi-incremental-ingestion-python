"""本地增量数据接入服务：HTTP 接入协议入口。"""
from __future__ import annotations

import os
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from ingestion.locks import SourceLockRegistry
from ingestion.models import (
    ChangeRecord,
    Disposition,
    Op,
    SchemaChange,
)
from ingestion.pipeline import IngestionPipeline, PipelineConfig
from ingestion.replay import DeadLetterNotFound, ReplayService
from ingestion.schema import SchemaManager
from ingestion.store import Store


class InitSourceRequest(BaseModel):
    columns: dict[str, str]


class ChangeRecordIn(BaseModel):
    position: int
    op: Op
    pk: dict[str, Any]
    payload: dict[str, Any] = Field(default_factory=dict)
    schema_version: int = 1


class IngestRequest(BaseModel):
    records: list[ChangeRecordIn]


class SchemaChangeIn(BaseModel):
    position: int
    to_version: int
    add_columns: dict[str, str] = Field(default_factory=dict)
    drop_columns: list[str] = Field(default_factory=list)
    rename_columns: dict[str, str] = Field(default_factory=dict)


class MarkDispositionRequest(BaseModel):
    disposition: Disposition
    note: str = ""


class ReplayRequest(BaseModel):
    dead_letter_ids: list[int]


def _dead_letter_dict(dl) -> dict[str, Any]:
    return {
        "id": dl.id,
        "source": dl.source,
        "position": dl.position,
        "reason": dl.reason.value,
        "detail": dl.detail,
        "record": dl.record,
        "created_at": dl.created_at,
        "disposition": dl.disposition.value,
        "handled_at": dl.handled_at,
        "note": dl.note,
        "replay_state": dl.replay_state.value,
        "last_replayed_at": dl.last_replayed_at,
        "replay_reason": dl.replay_reason,
        "replay_detail": dl.replay_detail,
        "replay_attempts": dl.replay_attempts,
    }


def create_app(db_path: str | None = None) -> FastAPI:
    store = Store(db_path or os.environ.get("INGESTION_DB", "ingestion.db"))
    store.connect()
    schema = SchemaManager(store)
    config = PipelineConfig()
    locks = SourceLockRegistry()
    pipeline = IngestionPipeline(store, schema, config, locks)
    replay_service = ReplayService(store, schema, config, locks)
    app = FastAPI(title="local-incremental-ingestion")

    @app.on_event("shutdown")
    def _shutdown() -> None:
        store.close()

    @app.post("/sources/{source}/init")
    def init_source(source: str, req: InitSourceRequest) -> dict[str, Any]:
        schema.ensure_source(source, req.columns)
        return {"source": source, "columns": store.get_columns(source)}

    @app.post("/sources/{source}/ingest")
    def ingest(source: str, req: IngestRequest) -> dict[str, Any]:
        records = [
            ChangeRecord(source=source, position=r.position, op=r.op,
                         pk=r.pk, payload=r.payload,
                         schema_version=r.schema_version)
            for r in req.records
        ]
        result = pipeline.ingest(source, records)
        return {
            "source": result.source,
            "applied": result.applied,
            "duplicates": result.duplicates,
            "rejected": [
                {"position": r.position, "reason": r.reason.value,
                 "detail": r.detail}
                for r in result.rejected
            ],
            "last_position": result.last_position,
            "attempts": result.attempts,
        }

    @app.post("/sources/{source}/schema-changes")
    def apply_schema_change(source: str, req: SchemaChangeIn) -> dict[str, Any]:
        change = SchemaChange(
            source=source, position=req.position, to_version=req.to_version,
            add_columns=req.add_columns, drop_columns=tuple(req.drop_columns),
            rename_columns=req.rename_columns,
        )
        pipeline.apply_schema_change(change)
        return {"source": source, "columns": store.get_columns(source)}

    @app.get("/sources/{source}/checkpoint")
    def get_checkpoint(source: str) -> dict[str, Any]:
        return {"source": source, "last_position": store.get_checkpoint(source)}

    @app.get("/sources/{source}/schema")
    def get_schema(source: str) -> dict[str, Any]:
        return {"source": source, "columns": store.get_columns(source)}

    @app.get("/sources/{source}/rows")
    def get_rows(source: str) -> dict[str, Any]:
        return {"source": source, "rows": store.get_rows(source)}

    @app.get("/sources/{source}/rejections")
    def get_rejections(source: str) -> dict[str, Any]:
        return {"source": source, "rejections": store.list_rejections(source)}

    # ---- 失败记录隔离处置与受控重放 ----
    @app.get("/dead-letters")
    def list_dead_letters(
        source: str | None = None,
        disposition: str | None = None,
        reason: str | None = None,
        limit: int = Query(default=50, gt=0, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        page, total = replay_service.list_dead_letters(
            source=source, disposition=disposition, reason=reason,
            limit=limit, offset=offset)
        return {
            "items": [_dead_letter_dict(dl) for dl in page],
            "total": total, "limit": limit, "offset": offset,
        }

    @app.get("/dead-letters/{dead_letter_id}")
    def get_dead_letter(dead_letter_id: int) -> dict[str, Any]:
        try:
            dl = replay_service.get_dead_letter(dead_letter_id)
        except DeadLetterNotFound:
            raise HTTPException(status_code=404,
                                detail=f"dead letter {dead_letter_id} not found")
        return _dead_letter_dict(dl)

    @app.post("/dead-letters/{dead_letter_id}/disposition")
    def mark_disposition(dead_letter_id: int,
                         req: MarkDispositionRequest) -> dict[str, Any]:
        try:
            dl = replay_service.mark(dead_letter_id, req.disposition, req.note)
        except DeadLetterNotFound:
            raise HTTPException(status_code=404,
                                detail=f"dead letter {dead_letter_id} not found")
        return _dead_letter_dict(dl)

    @app.post("/replay")
    def replay(req: ReplayRequest) -> dict[str, Any]:
        try:
            result = replay_service.replay(req.dead_letter_ids)
        except DeadLetterNotFound as exc:
            raise HTTPException(status_code=404,
                                detail=f"dead letter {exc.args[0]} not found")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {
            "source": result.source,
            "total": len(result.items),
            "applied": result.applied,
            "rejected": result.rejected,
            "skipped": result.skipped,
            "attempts": result.attempts,
            "items": [
                {"dead_letter_id": i.dead_letter_id,
                 "result": i.result.value,
                 "reason": i.reason.value if i.reason else None,
                 "detail": i.detail,
                 "attempts": i.attempts}
                for i in result.items
            ],
        }

    @app.get("/dead-letters/{dead_letter_id}/replays")
    def get_replay_events(dead_letter_id: int) -> dict[str, Any]:
        try:
            events = replay_service.list_replay_events(dead_letter_id)
        except DeadLetterNotFound:
            raise HTTPException(status_code=404,
                                detail=f"dead letter {dead_letter_id} not found")
        return {"dead_letter_id": dead_letter_id, "events": events}

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
