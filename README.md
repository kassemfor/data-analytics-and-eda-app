# Modern Auto Analytics Engine

For a full usage guide, see `WIKI.md`.

A full-stack app that combines:
- automated EDA issue detection/fixes (7 core tricks)
- modern Parquet + DuckDB analytics workflow
- optional MotherDuck query execution
- background folder-based CSV ingestion with scheduled profiling

## Stack
- Backend: FastAPI + Pandas + DuckDB + PyArrow
- Frontend: React + TypeScript + Vite
- Container runtime: Docker Compose

## Features
- CSV upload and auto-pipeline execution
- Data quality automation:
  - missing value imputation
  - duplicate row removal
  - outlier capping (IQR)
  - category normalization
  - type inference
  - skewness correction
  - high-correlation feature detection
- Cleaned dataset persisted as Parquet
- Built-in benchmark: Pandas vs DuckDB
- SQL query runner against `dataset` view
- Optional MotherDuck mode (`MOTHERDUCK_TOKEN`)
- Batch scheduler:
  - monitor a folder for CSV files
  - ingest changed/new files in background
  - generate full automated profiling report for each ingested file
  - retain run history and job status

## One-Command Start (Docker Compose)
```bash
docker compose up --build
```

Open:
- Frontend: http://127.0.0.1:4173
- Backend API: http://127.0.0.1:8000

Compose mounts:
- `./backend/storage` -> dataset/report storage
- `./batch_inbox` -> folder for batch CSV ingestion (`/data/inbox` in container)

To test batch ingestion quickly with Compose:
1. Drop one or more `.csv` files into `batch_inbox/`.
2. In UI, create a batch job with watch directory `/data/inbox`.
3. The job runs in background and creates datasets + reports automatically.

## Local Dev Run
### Backend
```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

### Frontend
```bash
cd frontend
npm install
npm run dev -- --host 127.0.0.1 --port 4173
```

Set custom backend URL if needed:
```bash
export VITE_API_BASE_URL=http://127.0.0.1:8000
```

## Tests
```bash
cd backend
source .venv/bin/activate
pytest -q
```

## MotherDuck Usage
- Select `MotherDuck` in the UI query section.
- Provide token in the input field, or set:
```bash
export MOTHERDUCK_TOKEN=md_your_token_here
```

## API Endpoints
Core:
- `GET /api/health`
- `POST /api/upload`
- `GET /api/datasets`
- `GET /api/datasets/{dataset_id}`
- `POST /api/datasets/{dataset_id}/query`

Batch scheduler:
- `GET /api/batch/jobs`
- `POST /api/batch/jobs`
- `PATCH /api/batch/jobs/{job_id}`
- `DELETE /api/batch/jobs/{job_id}`
- `POST /api/batch/jobs/{job_id}/run`
- `GET /api/batch/runs?limit=30`
