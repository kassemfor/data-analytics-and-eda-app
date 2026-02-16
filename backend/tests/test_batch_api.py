from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app


def _write_sample_csv(path: Path, *, include_extra_row: bool = False) -> None:
    payload = (
        "id,category,revenue,cost,date\n"
        "1,retail,100,60,2024-01-01\n"
        "2,retail,120,70,2024-01-02\n"
    )
    if include_extra_row:
        payload += "3,wholesale,400,130,2024-01-03\n"

    path.write_text(payload, encoding="utf-8")


def test_batch_job_ingests_folder(monkeypatch, tmp_path):
    monkeypatch.setenv("EDA_STORAGE_DIR", str(tmp_path / "storage"))

    inbox = tmp_path / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    csv_path = inbox / "sales.csv"
    _write_sample_csv(csv_path)

    with TestClient(app) as client:
        create_response = client.post(
            "/api/batch/jobs",
            json={
                "name": "Folder Job",
                "watch_dir": str(inbox),
                "poll_seconds": 3600,
                "auto_fix": True,
                "enabled": True,
                "run_on_create": False,
            },
        )
        assert create_response.status_code == 200

        job_id = create_response.json()["job"]["job_id"]

        first_run = client.post(f"/api/batch/jobs/{job_id}/run")
        assert first_run.status_code == 200
        first_payload = first_run.json()["run"]
        assert first_payload["status"] == "success"
        assert first_payload["files_seen"] == 1
        assert first_payload["files_processed"] == 1
        assert len(first_payload["datasets_created"]) == 1

        jobs_response = client.get("/api/batch/jobs")
        assert jobs_response.status_code == 200
        jobs = jobs_response.json()["jobs"]
        assert jobs[0]["processed_files"] == 1

        _write_sample_csv(csv_path, include_extra_row=True)

        second_run = client.post(f"/api/batch/jobs/{job_id}/run")
        assert second_run.status_code == 200
        second_payload = second_run.json()["run"]
        assert second_payload["files_processed"] == 1
        assert len(second_payload["datasets_created"]) == 1

        delete_response = client.delete(f"/api/batch/jobs/{job_id}")
        assert delete_response.status_code == 200
        assert delete_response.json()["deleted"] is True


def test_batch_job_rejects_invalid_folder(monkeypatch, tmp_path):
    monkeypatch.setenv("EDA_STORAGE_DIR", str(tmp_path / "storage"))

    with TestClient(app) as client:
        create_response = client.post(
            "/api/batch/jobs",
            json={
                "watch_dir": str(tmp_path / "does-not-exist"),
                "poll_seconds": 60,
                "auto_fix": True,
                "enabled": True,
            },
        )
        assert create_response.status_code == 400
