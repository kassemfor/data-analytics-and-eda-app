from __future__ import annotations

from contextlib import asynccontextmanager
import os
from typing import Any, Literal

import duckdb
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .analysis import json_ready
from .batch import BatchManager
from .ingestion import ingest_csv_bytes
from .storage import get_parquet_path, get_report, list_datasets


batch_manager: BatchManager | None = None


def get_batch_manager() -> BatchManager:
    global batch_manager
    if batch_manager is None:
        batch_manager = BatchManager()
    return batch_manager


@asynccontextmanager
async def app_lifespan(_: FastAPI):
    manager = get_batch_manager()
    manager.start()
    try:
        yield
    finally:
        manager.stop()


app = FastAPI(
    title="Modern Auto Analytics Engine",
    version="1.0.0",
    lifespan=app_lifespan,
    description=(
        "Automates EDA data-quality fixes, stores clean data as Parquet, and enables "
        "DuckDB/MotherDuck analytics queries."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    sql: str = Field(min_length=1)
    engine: Literal["duckdb", "motherduck"] = "duckdb"
    motherduck_token: str | None = None


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    engine: str


class BatchJobCreateRequest(BaseModel):
    watch_dir: str = Field(min_length=1)
    poll_seconds: int = Field(default=300, ge=10, le=86400)
    auto_fix: bool = True
    enabled: bool = True
    name: str | None = None
    run_on_create: bool = False


class BatchJobUpdateRequest(BaseModel):
    name: str | None = None
    watch_dir: str | None = None
    poll_seconds: int | None = Field(default=None, ge=10, le=86400)
    auto_fix: bool | None = None
    enabled: bool | None = None


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "auto-analytics-engine"}


@app.post("/api/upload")
async def upload_dataset(
    file: UploadFile = File(...),
    auto_fix: bool = Form(default=True),
) -> dict[str, Any]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="A file name is required.")

    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    raw_bytes = await file.read()
    try:
        return ingest_csv_bytes(raw_bytes, file.filename, auto_fix=auto_fix, ingestion_mode="upload")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/datasets")
def datasets() -> dict[str, list[dict[str, Any]]]:
    return {"datasets": list_datasets()}


@app.get("/api/datasets/{dataset_id}")
def dataset_report(dataset_id: str) -> dict[str, Any]:
    try:
        payload = get_report(dataset_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Dataset not found.") from exc

    return json_ready(payload)


def _query_local_duckdb(parquet_path: str, sql: str) -> QueryResponse:
    con = duckdb.connect(database=":memory:")
    con.execute(
        "CREATE OR REPLACE TEMP TABLE dataset AS SELECT * FROM read_parquet(?)",
        [parquet_path],
    )
    result = con.execute(sql).fetchdf()
    con.close()

    rows = result.head(500).values.tolist()
    return QueryResponse(
        columns=[str(col) for col in result.columns.tolist()],
        rows=json_ready(rows),
        row_count=int(len(result)),
        engine="duckdb",
    )


def _query_motherduck(parquet_path: str, sql: str, token: str) -> QueryResponse:
    con = duckdb.connect(f"md:?motherduck_token={token}")
    con.execute(
        "CREATE OR REPLACE TEMP TABLE dataset AS SELECT * FROM read_parquet(?)",
        [parquet_path],
    )
    result = con.execute(sql).fetchdf()
    con.close()

    rows = result.head(500).values.tolist()
    return QueryResponse(
        columns=[str(col) for col in result.columns.tolist()],
        rows=json_ready(rows),
        row_count=int(len(result)),
        engine="motherduck",
    )


@app.post("/api/datasets/{dataset_id}/query")
def query_dataset(dataset_id: str, payload: QueryRequest) -> QueryResponse:
    normalized_sql = payload.sql.strip().lower()
    if not normalized_sql.startswith(("select", "with")):
        raise HTTPException(status_code=400, detail="Only SELECT/WITH queries are allowed.")

    try:
        parquet_path = str(get_parquet_path(dataset_id))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Dataset not found.") from exc

    try:
        if payload.engine == "motherduck":
            token = payload.motherduck_token or os.getenv("MOTHERDUCK_TOKEN")
            if not token:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "MotherDuck token missing. Set MOTHERDUCK_TOKEN env var or send motherduck_token."
                    ),
                )
            return _query_motherduck(parquet_path, payload.sql, token)

        return _query_local_duckdb(parquet_path, payload.sql)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Query failed: {exc}") from exc


@app.get("/api/batch/jobs")
def batch_jobs() -> dict[str, list[dict[str, Any]]]:
    manager = get_batch_manager()
    return {"jobs": manager.list_jobs()}


@app.post("/api/batch/jobs")
def create_batch_job(payload: BatchJobCreateRequest) -> dict[str, Any]:
    manager = get_batch_manager()
    try:
        job = manager.create_job(
            watch_dir=payload.watch_dir,
            poll_seconds=payload.poll_seconds,
            auto_fix=payload.auto_fix,
            enabled=payload.enabled,
            name=payload.name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    run = None
    if payload.run_on_create:
        try:
            run = manager.run_job(job["job_id"], triggered_by="create")
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Batch job not found.") from exc

    return {"job": job, "run": run}


@app.patch("/api/batch/jobs/{job_id}")
def update_batch_job(job_id: str, payload: BatchJobUpdateRequest) -> dict[str, Any]:
    manager = get_batch_manager()
    try:
        job = manager.update_job(
            job_id,
            name=payload.name,
            watch_dir=payload.watch_dir,
            poll_seconds=payload.poll_seconds,
            auto_fix=payload.auto_fix,
            enabled=payload.enabled,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Batch job not found.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"job": job}


@app.delete("/api/batch/jobs/{job_id}")
def delete_batch_job(job_id: str) -> dict[str, bool]:
    manager = get_batch_manager()
    try:
        manager.delete_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Batch job not found.") from exc
    return {"deleted": True}


@app.post("/api/batch/jobs/{job_id}/run")
def run_batch_job(job_id: str) -> dict[str, Any]:
    manager = get_batch_manager()
    try:
        run = manager.run_job(job_id, triggered_by="manual")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Batch job not found.") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"run": run}


@app.get("/api/batch/runs")
def batch_runs(limit: int = Query(default=30, ge=1, le=200)) -> dict[str, list[dict[str, Any]]]:
    manager = get_batch_manager()
    return {"runs": manager.list_runs(limit=limit)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
