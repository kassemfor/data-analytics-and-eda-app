# Auto Analytics Engine Wiki

## 1. What this app does
The app automates three major workflows:
- Automated EDA data-quality detection and fixes.
- Parquet + DuckDB analytics and SQL exploration.
- Background folder ingestion with scheduler jobs.

## 2. Start the app
### Docker (recommended)
```bash
docker compose up --build
```

Open:
- App UI: `http://127.0.0.1:4173`
- Backend API: `http://127.0.0.1:8000`

Stop:
```bash
docker compose down
```

### Local dev (optional)
Backend:
```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

Frontend:
```bash
cd frontend
npm install
npm run dev -- --host 127.0.0.1 --port 4173
```

## 3. UI workflow
### Upload and auto-fix a dataset
1. Click `Choose CSV`.
2. Select a `.csv` file.
3. Click `Run Automated Pipeline`.
4. Review generated panels:
- `Quality Delta`
- `EDA Tricks Covered`
- `Fixes Applied`
- `Pandas vs DuckDB Benchmark`

### Run SQL
1. Use a suggestion button or edit SQL manually.
2. Choose engine:
- `DuckDB` for local analytics.
- `MotherDuck` for cloud execution.
3. Click `Run Query`.

## 4. Batch scheduler (folder automation)
Use this for automatic recurring ingestion.

### Setup
1. Put CSVs in the watched folder.
- With Docker, default mapped folder is local `batch_inbox/` and container path `/data/inbox`.
2. In `Batch Folder Automation`:
- `Watch Directory`: `/data/inbox`
- `Poll Seconds`: e.g. `300`
- Optional `Job Name`
3. Click `Create + Run Job`.

### Job actions
- `Run Now`: immediate scan/ingestion.
- `Pause`/`Resume`: enable or disable scheduler execution.
- `Delete`: remove the job definition.

### Run history
`Recent Batch Runs` shows:
- status (`success`, `partial_success`, `failed`)
- files seen/processed
- datasets created
- errors (if any)

## 5. API reference
Core endpoints:
- `GET /api/health`
- `POST /api/upload`
- `GET /api/datasets`
- `GET /api/datasets/{dataset_id}`
- `POST /api/datasets/{dataset_id}/query`

Batch endpoints:
- `GET /api/batch/jobs`
- `POST /api/batch/jobs`
- `PATCH /api/batch/jobs/{job_id}`
- `DELETE /api/batch/jobs/{job_id}`
- `POST /api/batch/jobs/{job_id}/run`
- `GET /api/batch/runs?limit=30`

## 6. Troubleshooting
### Error: `Could not load batch scheduler data.`
Cause:
- Frontend cannot reach batch endpoints.

Checks:
```bash
curl -i http://127.0.0.1:4173/api/batch/jobs
curl -i 'http://127.0.0.1:4173/api/batch/runs?limit=5'
```
Expected: `HTTP/1.1 200 OK`.

If you still see the message after deploy:
1. Hard refresh browser (`Cmd+Shift+R` on macOS) to load latest JS bundle.
2. Confirm containers are healthy:
```bash
docker compose ps
```
3. Rebuild if needed:
```bash
docker compose up -d --build
```

### Upload failing with 404 on `/api/api/...`
Cause:
- Old cached frontend bundle.
Fix:
- Hard refresh and ensure newest JS is loaded from `/assets/index-*.js`.

### MotherDuck errors
- Set token in UI field, or export:
```bash
export MOTHERDUCK_TOKEN=md_your_token_here
```

## 7. Data storage locations
- Cleaned datasets and reports: `backend/storage/`
- Batch input folder (Docker): `batch_inbox/`
- Batch scheduler state file: `backend/storage/batch_state.json`
