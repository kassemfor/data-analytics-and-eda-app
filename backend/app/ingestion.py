from __future__ import annotations

import io
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from .analysis import (
    benchmark_pandas_vs_duckdb,
    build_query_suggestions,
    json_ready,
    run_automated_eda_pipeline,
)
from .storage import save_dataset, update_report


def ingest_csv_bytes(
    raw_bytes: bytes,
    source_file: str,
    *,
    auto_fix: bool = True,
    ingestion_mode: str = "upload",
    source_path: str | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    if not source_file:
        raise ValueError("A file name is required.")

    if not raw_bytes:
        raise ValueError("Uploaded CSV is empty.")

    try:
        df = pd.read_csv(io.BytesIO(raw_bytes))
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"CSV parsing failed: {exc}") from exc

    if df.empty:
        raise ValueError("CSV has no rows.")

    if auto_fix:
        cleaned_df, report = run_automated_eda_pipeline(df)
    else:
        cleaned_df, report = df.copy(), {
            "before": {},
            "after": {},
            "fixes_applied": [],
            "tricks_covered": [],
            "type_conversions": [],
            "quality_delta": {},
        }

    report["created_at"] = datetime.now(UTC).isoformat()
    report["source_file"] = source_file
    report["ingestion"] = {
        "mode": ingestion_mode,
        "auto_fix": auto_fix,
        "source_path": source_path,
        "job_id": job_id,
    }

    metadata = save_dataset(raw_bytes, cleaned_df, report)
    parquet_path = metadata["parquet_path"]

    benchmark = benchmark_pandas_vs_duckdb(cleaned_df, parquet_path)
    suggestions = build_query_suggestions(cleaned_df)

    report["benchmark"] = benchmark
    report["query_suggestions"] = suggestions

    update_report(metadata["dataset_id"], report)

    return json_ready(
        {
            "dataset_id": metadata["dataset_id"],
            "report": report,
            "benchmark": benchmark,
            "query_suggestions": suggestions,
            "parquet_path": parquet_path,
        }
    )
