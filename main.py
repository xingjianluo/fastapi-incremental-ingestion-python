"""本地增量数据接入服务：HTTP 接入协议入口。"""
from __future__ import annotations

import os
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ingestion.models import (
    ChangeRecord,
    DeadLetter,
    DeadLetterStatus,
    FailureReason,
    Op,
    SchemaChange,
)
from ingestion.pipeline import IngestionPipeline, PipelineConfig
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


class DeadLetterStatusIn(BaseModel):
    status: DeadLetterStatus
    note: str = ""


class ReplayRequest(BaseModel):
    ids: list[int] = Field(min_length=1)


def dead_letter_to_dict(dl: DeadLetter) -> dict[str, Any]:
    return {
        "id": dl.id,
        "source": dl.source,
        "position": dl.position,
        "reason": dl.reason.value,
        "detail": dl.detail,
        "record": dl.record,
        "status": dl.status.value,
        "created_at": dl.created_at,
        "updated_at": dl.updated_at,
        "resolution_note": dl.resolution_note,
        "replay_outcome": dl.replay_outcome.value if dl.replay_outcome else None,
        "replay_reason": dl.replay_reason,
        "replay_detail": dl.replay_detail,
        "replayed_at": dl.replayed_at,
        "replay_attempts": dl.replay_attempts,
    }


def create_app(db_path: str | None = None) -> FastAPI:
    store = Store(db_path or os.environ.get("INGESTION_DB", "ingestion.db"))
    store.connect()
    schema = SchemaManager(store)
    pipeline = IngestionPipeline(store, schema, PipelineConfig())
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

    # ---- 失败记录处置与受控重放 ----
    @app.get("/sources/{source}/dead-letters")
    def list_dead_letters(
        source: str,
        reason: FailureReason | None = None,
        status: DeadLetterStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        limit = max(1, min(limit, 500))  # 分页上限，避免一次性全量读入
        offset = max(0, offset)
        items, total = store.list_dead_letters(
            source=source, reason=reason, status=status,
            limit=limit, offset=offset)
        return {
            "source": source,
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": [dead_letter_to_dict(dl) for dl in items],
        }

    @app.get("/dead-letters/{dead_letter_id}")
    def get_dead_letter(dead_letter_id: int) -> dict[str, Any]:
        dl = store.get_dead_letter(dead_letter_id)
        if dl is None:
            raise HTTPException(status_code=404, detail="dead letter not found")
        return {
            "dead_letter": dead_letter_to_dict(dl),
            "replay_history": store.list_replay_history(dead_letter_id),
        }

    @app.post("/dead-letters/{dead_letter_id}/status")
    def mark_dead_letter(dead_letter_id: int, req: DeadLetterStatusIn
                         ) -> dict[str, Any]:
        dl = store.update_dead_letter_status(
            dead_letter_id, req.status, req.note)
        if dl is None:
            raise HTTPException(status_code=404, detail="dead letter not found")
        return {"dead_letter": dead_letter_to_dict(dl)}

    @app.post("/sources/{source}/replay")
    def replay_dead_letters(source: str, req: ReplayRequest) -> dict[str, Any]:
        try:
            result = pipeline.replay(req.ids)
        except ValueError as exc:  # 超过 max_replay_batch
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "results": [
                {"dead_letter_id": r.dead_letter_id, "source": r.source,
                 "position": r.position, "outcome": r.outcome.value,
                 "reason": r.reason, "detail": r.detail}
                for r in result.results
            ],
            "applied": result.applied,
            "rejected": result.rejected,
            "attempts": result.attempts,
        }

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
