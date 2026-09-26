import logging

import pytest

from ingestion.pipeline import IngestionPipeline, PipelineConfig
from ingestion.schema import SchemaManager
from ingestion.store import Store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@pytest.fixture()
def store(tmp_path):
    s = Store(tmp_path / "test.db")
    yield s
    s.close()


@pytest.fixture()
def schema(store):
    return SchemaManager(store)


@pytest.fixture()
def pipeline(store, schema):
    return IngestionPipeline(store, schema, PipelineConfig(max_retries=2,
                                                           backoff_seconds=0.01))
