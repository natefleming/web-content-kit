"""Step 2: manage the chunking pipeline and the Vector Search index with the Databricks SDK."""

import logging
import time
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.pipelines import FileLibrary, PipelineLibrary, PipelinesEnvironment, UpdateInfoState
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    EndpointStatusState,
    EndpointType,
    PipelineType,
    VectorIndexType,
)

log = logging.getLogger("web_content_kit.indexing")

CHUNK_TABLE_NAME: str = "document_chunks"
SYNC_COLUMNS: list[str] = ["chunk_id", "chunk_text", "title", "section", "source_url", "chunk_index", "file_path"]
POLL_SECONDS: int = 15


@dataclass(frozen=True)
class IndexSettings:
    """Everything step 2 needs to know for one run."""

    catalog: str
    schema: str
    volume: str
    pipeline_name: str
    vs_endpoint: str
    index_name: str
    embedding_model: str = "databricks-gte-large-en"
    chunk_size_tokens: int = 512
    chunk_overlap_tokens: int = 64
    full_refresh: bool = False

    @property
    def content_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def chunk_table(self) -> str:
        return f"{self.catalog}.{self.schema}.{CHUNK_TABLE_NAME}"

    @property
    def full_index_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.index_name}"


# ---------------------------------------------------------------------------
# Chunking pipeline (Lakeflow Spark Declarative Pipelines)
# ---------------------------------------------------------------------------
def create_or_update_pipeline(
    w: WorkspaceClient, settings: IndexSettings, pipeline_file: str, project_root: str
) -> str:
    """Create the serverless chunking pipeline, or update its settings if it exists. Returns its id.

    ``project_root`` is the workspace folder holding ``pyproject.toml``. The pipeline installs this
    project so its code can ``import web_content_kit``. (A plain path is used on purpose: pipeline
    environments turn ``path[extra]`` into an unquoted ``name[extra] @ file:...`` that pip rejects.)
    """
    spec = {
        "name": settings.pipeline_name,
        "catalog": settings.catalog,
        "schema": settings.schema,
        "serverless": True,
        "continuous": False,
        "development": False,
        "channel": "CURRENT",
        "libraries": [PipelineLibrary(file=FileLibrary(path=pipeline_file))],
        "configuration": {
            "web_content.source_path": settings.content_path,
            "web_content.chunk_size_tokens": str(settings.chunk_size_tokens),
            "web_content.chunk_overlap_tokens": str(settings.chunk_overlap_tokens),
        },
        "environment": PipelinesEnvironment(dependencies=[project_root]),
    }

    existing = [
        p
        for p in w.pipelines.list_pipelines(filter=f"name LIKE '{settings.pipeline_name}'")
        if p.name == settings.pipeline_name
    ]
    if existing:
        pipeline_id = existing[0].pipeline_id
        w.pipelines.update(pipeline_id=pipeline_id, **spec)
        log.info("Updated existing pipeline '%s' (%s).", settings.pipeline_name, pipeline_id)
    else:
        pipeline_id = w.pipelines.create(**spec).pipeline_id
        log.info("Created pipeline '%s' (%s).", settings.pipeline_name, pipeline_id)
    return pipeline_id


def run_pipeline(w: WorkspaceClient, pipeline_id: str, full_refresh: bool, timeout_minutes: int = 60) -> None:
    """Start a pipeline update and wait for it to finish, logging each state change."""
    update_id = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh).update_id
    url = f"{w.config.host.rstrip('/')}/pipelines/{pipeline_id}/updates/{update_id}"
    log.info("Pipeline update started%s: %s", " (FULL REFRESH)" if full_refresh else "", url)
    deadline = time.monotonic() + timeout_minutes * 60
    last_state: UpdateInfoState | None = None
    while time.monotonic() < deadline:
        state = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update.state
        if state != last_state:
            log.info("Pipeline state: %s", state.value if state else "UNKNOWN")
            last_state = state
        if state == UpdateInfoState.COMPLETED:
            return
        if state in (UpdateInfoState.FAILED, UpdateInfoState.CANCELED):
            errors = [
                e.message
                for e in w.pipelines.list_pipeline_events(
                    pipeline_id=pipeline_id, filter="level='ERROR'", max_results=5
                )
            ]
            raise RuntimeError(f"Pipeline update {state.value}. Details: {url}\n" + "\n".join(errors[:5]))
        time.sleep(POLL_SECONDS)
    raise TimeoutError(f"Pipeline did not finish within {timeout_minutes} minutes: {url}")


# ---------------------------------------------------------------------------
# Vector Search
# ---------------------------------------------------------------------------
def ensure_endpoint(w: WorkspaceClient, name: str, timeout_minutes: int = 30) -> None:
    """Create the Vector Search endpoint if needed and wait until it is ONLINE."""
    try:
        w.vector_search_endpoints.get_endpoint(name)
        log.info("Using existing Vector Search endpoint '%s'.", name)
    except NotFound:
        log.info("Creating Vector Search endpoint '%s' (this can take several minutes)...", name)
        w.vector_search_endpoints.create_endpoint(name=name, endpoint_type=EndpointType.STANDARD)
    deadline = time.monotonic() + timeout_minutes * 60
    while time.monotonic() < deadline:
        status = w.vector_search_endpoints.get_endpoint(name).endpoint_status
        state = status.state if status else None
        if state == EndpointStatusState.ONLINE:
            log.info("Endpoint '%s' is ONLINE.", name)
            return
        if state in (EndpointStatusState.OFFLINE, EndpointStatusState.RED_STATE):
            raise RuntimeError(f"Endpoint '{name}' is {state.value}: {status.message}")
        log.info("Endpoint state: %s ...", state.value if state else "UNKNOWN")
        time.sleep(POLL_SECONDS * 2)
    raise TimeoutError(f"Endpoint '{name}' was not ONLINE after {timeout_minutes} minutes.")


def ensure_index(w: WorkspaceClient, settings: IndexSettings) -> None:
    """Create the Delta Sync index on the chunk table, or trigger a sync if it already exists."""
    try:
        w.vector_search_indexes.get_index(settings.full_index_name)
        log.info("Index exists. Syncing it with the latest chunks...")
        w.vector_search_indexes.sync_index(settings.full_index_name)
    except NotFound:
        log.info("Creating index %s ...", settings.full_index_name)
        w.vector_search_indexes.create_index(
            name=settings.full_index_name,
            endpoint_name=settings.vs_endpoint,
            primary_key="chunk_id",
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=settings.chunk_table,
                pipeline_type=PipelineType.TRIGGERED,
                columns_to_sync=SYNC_COLUMNS,
                embedding_source_columns=[
                    EmbeddingSourceColumn(name="chunk_text", embedding_model_endpoint_name=settings.embedding_model)
                ],
            ),
        )


def wait_for_index(w: WorkspaceClient, name: str, expected_rows: int, timeout_minutes: int = 60) -> None:
    """Wait until the index is ready and has caught up with the chunk table."""
    deadline = time.monotonic() + timeout_minutes * 60
    while time.monotonic() < deadline:
        status = w.vector_search_indexes.get_index(name).status
        indexed = (status.indexed_row_count or 0) if status else 0
        if status and status.ready and indexed >= expected_rows:
            log.info("Index is ready with %d chunks.", indexed)
            return
        message = (status.message or "").split(".")[0] if status else ""
        log.info("Index syncing: %d / %d chunks indexed (%s)", indexed, expected_rows, message)
        time.sleep(POLL_SECONDS * 2)
    raise TimeoutError(f"Index {name} was not ready after {timeout_minutes} minutes. Check it in Catalog Explorer.")


def search(w: WorkspaceClient, index_name: str, question: str, num_results: int = 5) -> list[dict[str, object]]:
    """Hybrid (keyword + meaning) search. Returns one dict per matching chunk."""
    response = w.vector_search_indexes.query_index(
        index_name=index_name,
        columns=["title", "section", "source_url", "chunk_text"],
        query_text=question,
        query_type="HYBRID",
        num_results=num_results,
    )
    columns = [c.name for c in response.manifest.columns]
    return [dict(zip(columns, row)) for row in (response.result.data_array or [])]
