from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

import pandas as pd


def storage_root() -> Path:
    default_root = Path(__file__).resolve().parent.parent / "storage"
    root = Path(os.getenv("EDA_STORAGE_DIR", str(default_root)))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _dataset_dir(dataset_id: str) -> Path:
    path = storage_root() / dataset_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_dataset(
    raw_csv: bytes,
    cleaned_df: pd.DataFrame,
    report: dict[str, Any],
) -> dict[str, Any]:
    dataset_id = str(uuid.uuid4())
    target_dir = _dataset_dir(dataset_id)

    raw_path = target_dir / "original.csv"
    parquet_path = target_dir / "cleaned.parquet"
    report_path = target_dir / "report.json"

    raw_path.write_bytes(raw_csv)
    cleaned_df.to_parquet(parquet_path, index=False)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    metadata = {
        "dataset_id": dataset_id,
        "raw_csv_path": str(raw_path),
        "parquet_path": str(parquet_path),
        "report_path": str(report_path),
    }
    return metadata


def update_report(dataset_id: str, report: dict[str, Any]) -> None:
    report_path = storage_root() / dataset_id / "report.json"
    if not report_path.exists():
        raise FileNotFoundError(dataset_id)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def list_datasets() -> list[dict[str, Any]]:
    datasets: list[dict[str, Any]] = []
    for child in sorted(storage_root().iterdir(), key=lambda p: p.name, reverse=True):
        if not child.is_dir():
            continue
        report_path = child / "report.json"
        if not report_path.exists():
            continue

        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            datasets.append(
                {
                    "dataset_id": child.name,
                    "rows": payload.get("after", {}).get("rows"),
                    "columns": payload.get("after", {}).get("columns"),
                    "created_at": payload.get("created_at"),
                }
            )
        except json.JSONDecodeError:
            continue

    return datasets


def get_report(dataset_id: str) -> dict[str, Any]:
    path = storage_root() / dataset_id / "report.json"
    if not path.exists():
        raise FileNotFoundError(dataset_id)
    return json.loads(path.read_text(encoding="utf-8"))


def get_parquet_path(dataset_id: str) -> Path:
    path = storage_root() / dataset_id / "cleaned.parquet"
    if not path.exists():
        raise FileNotFoundError(dataset_id)
    return path
