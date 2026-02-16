from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def _sample_csv() -> bytes:
    return (
        "id,city,revenue,cost,date\n"
        "1, New York ,100,60,2024-01-01\n"
        "2,new york,110,65,2024-01-02\n"
        "3,LOS ANGELES,5000,100,2024-01-03\n"
        "3,LOS ANGELES,5000,100,2024-01-03\n"
        "4,,120,,2024-01-04\n"
    ).encode("utf-8")


def test_upload_runs_eda_pipeline(monkeypatch, tmp_path):
    monkeypatch.setenv("EDA_STORAGE_DIR", str(tmp_path))
    client = TestClient(app)

    response = client.post(
        "/api/upload",
        data={"auto_fix": "true"},
        files={"file": ("sample.csv", _sample_csv(), "text/csv")},
    )

    assert response.status_code == 200
    payload = response.json()

    assert payload["dataset_id"]
    assert payload["report"]["quality_delta"]["duplicate_rows_after"] == 0
    assert payload["report"]["quality_delta"]["missing_cells_after"] == 0
    assert payload["benchmark"]["duckdb_ms"] >= 0


def test_query_endpoint_returns_rows(monkeypatch, tmp_path):
    monkeypatch.setenv("EDA_STORAGE_DIR", str(tmp_path))
    client = TestClient(app)

    upload = client.post(
        "/api/upload",
        data={"auto_fix": "true"},
        files={"file": ("sample.csv", _sample_csv(), "text/csv")},
    )
    assert upload.status_code == 200

    dataset_id = upload.json()["dataset_id"]
    query = client.post(
        f"/api/datasets/{dataset_id}/query",
        json={"sql": "SELECT COUNT(*) AS row_count FROM dataset", "engine": "duckdb"},
    )

    assert query.status_code == 200
    payload = query.json()

    assert payload["columns"] == ["row_count"]
    assert payload["rows"][0][0] >= 1
    assert payload["engine"] == "duckdb"
