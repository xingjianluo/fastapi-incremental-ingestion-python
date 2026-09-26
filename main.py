"""本地增量数据接入服务：HTTP 接入协议入口。"""
from __future__ import annotations

import os
from typing import Any

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel, Field

from ingestion.models import ChangeRecord, Op, SchemaChange
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

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
