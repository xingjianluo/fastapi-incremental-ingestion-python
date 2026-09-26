"""FastAPI 服务层：注册数据源、触发接入、查询状态/数据/流水/死信。"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .config import Settings
from .errors import IngestError
from .locker import SourceLocker
from .models import IngestBatch
from .pipeline import IngestionPipeline
from .storage import Storage


class RegisterRequest(BaseModel):
    columns: dict[str, str]


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    storage = Storage(settings.db_path)
    locker = SourceLocker(settings.db_path, settings.lock_timeout_seconds)
    pipeline = IngestionPipeline(storage, locker, settings)

    app = FastAPI(title="incremental-ingestion")
    app.state.storage = storage
    app.state.pipeline = pipeline

    @app.exception_handler(IngestError)
    def _ingest_error(request, exc: IngestError):  # type: ignore[no-untyped-def]
        from fastapi.responses import JSONResponse

        status = 409 if exc.recoverable else 422
        return JSONResponse(
            status_code=status,
            content={"reason": exc.reason.value, "detail": str(exc)},
        )

    @app.post("/sources/{source}/register")
    def register(source: str, req: RegisterRequest):
        storage.register_source(source, req.columns)
        return {"source": source, "columns": req.columns}

    @app.post("/sources/{source}/ingest")
    def ingest(source: str, batch: IngestBatch):
        if batch.source != source:
            raise HTTPException(400, "路径与批次中的数据源不一致")
        return pipeline.ingest(batch)

    @app.get("/sources")
    def list_sources():
        return {"sources": storage.list_sources()}

    @app.get("/sources/{source}/state")
    def state(source: str):
        return storage.source_state(source)

    @app.get("/sources/{source}/rows")
    def rows(source: str):
        return {"rows": storage.list_rows(source)}

    @app.get("/sources/{source}/events")
    def events(source: str):
        return {"events": storage.list_events(source)}

    @app.get("/sources/{source}/dead-letters")
    def dead_letters(source: str):
        return {"dead_letters": storage.list_dead_letters(source)}

    return app
