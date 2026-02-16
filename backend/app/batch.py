from __future__ import annotations

import json
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .ingestion import ingest_csv_bytes
from .storage import storage_root


MAX_RUN_HISTORY = 300


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _file_signature(path: Path) -> str:
    stat = path.stat()
    return f"{stat.st_mtime_ns}:{stat.st_size}"


class BatchManager:
    def __init__(self, state_path: Path | None = None) -> None:
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._running_jobs: set[str] = set()
        self._next_run_ts: dict[str, float] = {}

        self._state_path = state_path or (storage_root() / "batch_state.json")
        self._state: dict[str, Any] = {"jobs": [], "runs": []}
        self._load_state()

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, daemon=True, name="batch-scheduler")
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            thread = self._thread
            self._thread = None
            self._stop_event.set()

        if thread:
            thread.join(timeout=5)

    def _load_state(self) -> None:
        if not self._state_path.exists():
            self._state = {"jobs": [], "runs": []}
            return

        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {"jobs": [], "runs": []}

        jobs = payload.get("jobs", [])
        runs = payload.get("runs", [])

        normalized_jobs: list[dict[str, Any]] = []
        now = time.time()
        for job in jobs:
            job.setdefault("job_id", str(uuid.uuid4()))
            job.setdefault("name", f"Batch Job {job['job_id'][:8]}")
            job.setdefault("watch_dir", "")
            job.setdefault("poll_seconds", 300)
            job.setdefault("auto_fix", True)
            job.setdefault("enabled", True)
            job.setdefault("created_at", _now_iso())
            job.setdefault("last_run_at", None)
            job.setdefault("last_status", "idle")
            job.setdefault("last_error", None)
            job.setdefault("processed_signatures", {})
            normalized_jobs.append(job)

            if job["enabled"]:
                self._next_run_ts[job["job_id"]] = now + int(job["poll_seconds"])

        self._state = {
            "jobs": normalized_jobs,
            "runs": runs[-MAX_RUN_HISTORY:],
        }

    def _save_state(self) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._state, indent=2), encoding="utf-8")
        tmp.replace(self._state_path)

    def _run_loop(self) -> None:
        while not self._stop_event.wait(1):
            with self._lock:
                now = time.time()
                due_jobs: list[str] = []
                for job in self._state["jobs"]:
                    if not job.get("enabled", True):
                        continue

                    job_id = str(job["job_id"])
                    next_ts = self._next_run_ts.get(job_id, now + int(job["poll_seconds"]))
                    if now >= next_ts and job_id not in self._running_jobs:
                        due_jobs.append(job_id)
                        self._next_run_ts[job_id] = now + int(job["poll_seconds"])

            for job_id in due_jobs:
                self.run_job(job_id, triggered_by="scheduler")

    def _find_job(self, job_id: str) -> dict[str, Any] | None:
        for job in self._state["jobs"]:
            if job["job_id"] == job_id:
                return job
        return None

    def _serialize_job(self, job: dict[str, Any]) -> dict[str, Any]:
        next_run = self._next_run_ts.get(job["job_id"])
        return {
            "job_id": job["job_id"],
            "name": job["name"],
            "watch_dir": job["watch_dir"],
            "poll_seconds": job["poll_seconds"],
            "auto_fix": job["auto_fix"],
            "enabled": job["enabled"],
            "created_at": job["created_at"],
            "last_run_at": job["last_run_at"],
            "last_status": job["last_status"],
            "last_error": job["last_error"],
            "processed_files": len(job.get("processed_signatures", {})),
            "next_run_at": datetime.fromtimestamp(next_run, tz=UTC).isoformat() if next_run else None,
            "running": job["job_id"] in self._running_jobs,
        }

    def list_jobs(self) -> list[dict[str, Any]]:
        with self._lock:
            jobs = [self._serialize_job(job) for job in self._state["jobs"]]
        jobs.sort(key=lambda item: item["created_at"], reverse=True)
        return jobs

    def list_runs(self, limit: int = 30) -> list[dict[str, Any]]:
        with self._lock:
            runs = list(self._state["runs"])
        runs.sort(key=lambda item: item.get("started_at", ""), reverse=True)
        return runs[:limit]

    def create_job(
        self,
        *,
        watch_dir: str,
        poll_seconds: int = 300,
        auto_fix: bool = True,
        enabled: bool = True,
        name: str | None = None,
    ) -> dict[str, Any]:
        resolved_dir = Path(watch_dir).expanduser().resolve()
        if not resolved_dir.exists() or not resolved_dir.is_dir():
            raise ValueError(f"Watch directory does not exist: {resolved_dir}")

        interval = int(poll_seconds)
        if interval < 10:
            raise ValueError("poll_seconds must be >= 10")

        job_id = str(uuid.uuid4())
        now_iso = _now_iso()
        job = {
            "job_id": job_id,
            "name": name or f"Batch Job {job_id[:8]}",
            "watch_dir": str(resolved_dir),
            "poll_seconds": interval,
            "auto_fix": bool(auto_fix),
            "enabled": bool(enabled),
            "created_at": now_iso,
            "last_run_at": None,
            "last_status": "idle",
            "last_error": None,
            "processed_signatures": {},
        }

        with self._lock:
            self._state["jobs"].append(job)
            if enabled:
                self._next_run_ts[job_id] = time.time() + interval
            self._save_state()
            return self._serialize_job(job)

    def update_job(
        self,
        job_id: str,
        *,
        name: str | None = None,
        watch_dir: str | None = None,
        poll_seconds: int | None = None,
        auto_fix: bool | None = None,
        enabled: bool | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            job = self._find_job(job_id)
            if not job:
                raise KeyError(job_id)

            if name is not None:
                stripped = name.strip()
                if not stripped:
                    raise ValueError("name cannot be empty")
                job["name"] = stripped

            if watch_dir is not None:
                resolved_dir = Path(watch_dir).expanduser().resolve()
                if not resolved_dir.exists() or not resolved_dir.is_dir():
                    raise ValueError(f"Watch directory does not exist: {resolved_dir}")
                job["watch_dir"] = str(resolved_dir)

            if poll_seconds is not None:
                interval = int(poll_seconds)
                if interval < 10:
                    raise ValueError("poll_seconds must be >= 10")
                job["poll_seconds"] = interval
                self._next_run_ts[job_id] = time.time() + interval

            if auto_fix is not None:
                job["auto_fix"] = bool(auto_fix)

            if enabled is not None:
                job["enabled"] = bool(enabled)
                if enabled:
                    self._next_run_ts[job_id] = time.time() + int(job["poll_seconds"])
                else:
                    self._next_run_ts.pop(job_id, None)

            self._save_state()
            return self._serialize_job(job)

    def delete_job(self, job_id: str) -> None:
        with self._lock:
            initial_count = len(self._state["jobs"])
            self._state["jobs"] = [job for job in self._state["jobs"] if job["job_id"] != job_id]
            if len(self._state["jobs"]) == initial_count:
                raise KeyError(job_id)

            self._next_run_ts.pop(job_id, None)
            self._save_state()

    def run_job(self, job_id: str, *, triggered_by: str = "manual") -> dict[str, Any]:
        with self._lock:
            job = self._find_job(job_id)
            if not job:
                raise KeyError(job_id)
            if job_id in self._running_jobs:
                raise RuntimeError("Job is already running")

            self._running_jobs.add(job_id)
            job_snapshot = {
                "job_id": job["job_id"],
                "name": job["name"],
                "watch_dir": job["watch_dir"],
                "auto_fix": bool(job["auto_fix"]),
                "poll_seconds": int(job["poll_seconds"]),
            }

        started_at = _now_iso()
        run_id = str(uuid.uuid4())
        errors: list[str] = []
        created: list[dict[str, Any]] = []
        files_seen = 0
        files_processed = 0

        try:
            watch_dir = Path(job_snapshot["watch_dir"])
            if not watch_dir.exists() or not watch_dir.is_dir():
                raise ValueError(f"Watch directory is missing: {watch_dir}")

            csv_files = sorted(watch_dir.rglob("*.csv"))
            files_seen = len(csv_files)

            for csv_file in csv_files:
                source_path = str(csv_file.resolve())
                signature = _file_signature(csv_file)

                with self._lock:
                    live_job = self._find_job(job_id)
                    if not live_job:
                        raise KeyError(job_id)
                    processed = live_job.setdefault("processed_signatures", {})
                    known_signature = processed.get(source_path)
                    auto_fix = bool(live_job.get("auto_fix", True))

                if known_signature == signature:
                    continue

                try:
                    raw_bytes = csv_file.read_bytes()
                    ingested = ingest_csv_bytes(
                        raw_bytes,
                        csv_file.name,
                        auto_fix=auto_fix,
                        ingestion_mode="batch",
                        source_path=source_path,
                        job_id=job_id,
                    )
                    files_processed += 1
                    created.append(
                        {
                            "dataset_id": ingested["dataset_id"],
                            "source_file": csv_file.name,
                            "source_path": source_path,
                        }
                    )

                    with self._lock:
                        live_job = self._find_job(job_id)
                        if live_job:
                            live_job.setdefault("processed_signatures", {})[source_path] = signature
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{source_path}: {exc}")

            if errors and files_processed == 0:
                status = "failed"
            elif errors:
                status = "partial_success"
            else:
                status = "success"

            run_record = {
                "run_id": run_id,
                "job_id": job_id,
                "job_name": job_snapshot["name"],
                "triggered_by": triggered_by,
                "started_at": started_at,
                "finished_at": _now_iso(),
                "status": status,
                "files_seen": files_seen,
                "files_processed": files_processed,
                "datasets_created": created,
                "errors": errors,
            }

            with self._lock:
                live_job = self._find_job(job_id)
                if live_job:
                    live_job["last_run_at"] = run_record["finished_at"]
                    live_job["last_status"] = status
                    live_job["last_error"] = errors[0] if errors else None
                    self._next_run_ts[job_id] = time.time() + int(live_job.get("poll_seconds", 300))

                self._state["runs"].append(run_record)
                self._state["runs"] = self._state["runs"][-MAX_RUN_HISTORY:]
                self._save_state()

            return run_record
        finally:
            with self._lock:
                self._running_jobs.discard(job_id)
