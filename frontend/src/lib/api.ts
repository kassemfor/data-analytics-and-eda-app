import type { BatchJob, BatchRun, QueryResponse, UploadResponse } from './types';

const RAW_API_BASE = import.meta.env.VITE_API_BASE_URL ?? 'http://127.0.0.1:8000';
const NORMALIZED_API_BASE = RAW_API_BASE.replace(/\/+$/, '');
const API_PREFIX = NORMALIZED_API_BASE.endsWith('/api')
  ? NORMALIZED_API_BASE
  : `${NORMALIZED_API_BASE}/api`;

async function parseError(response: Response, fallback: string): Promise<never> {
  const contentType = response.headers.get('content-type') ?? '';
  if (contentType.includes('application/json')) {
    const body = (await response.json()) as { detail?: string };
    throw new Error(body.detail ?? fallback);
  }
  const text = await response.text();
  throw new Error(text || fallback);
}

export async function uploadCsv(file: File, autoFix = true): Promise<UploadResponse> {
  const form = new FormData();
  form.append('file', file);
  form.append('auto_fix', String(autoFix));

  const response = await fetch(`${API_PREFIX}/upload`, {
    method: 'POST',
    body: form
  });

  if (!response.ok) {
    return parseError(response, 'Upload failed');
  }

  return (await response.json()) as UploadResponse;
}

export async function runQuery(
  datasetId: string,
  sql: string,
  engine: 'duckdb' | 'motherduck',
  motherduckToken?: string
): Promise<QueryResponse> {
  const response = await fetch(`${API_PREFIX}/datasets/${datasetId}/query`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      sql,
      engine,
      motherduck_token: motherduckToken || undefined
    })
  });

  if (!response.ok) {
    return parseError(response, 'Query failed');
  }

  return (await response.json()) as QueryResponse;
}

export async function listBatchJobs(): Promise<BatchJob[]> {
  const response = await fetch(`${API_PREFIX}/batch/jobs`);
  if (!response.ok) {
    return parseError(response, 'Failed to load batch jobs');
  }
  const payload = (await response.json()) as { jobs: BatchJob[] };
  return payload.jobs;
}

export async function listBatchRuns(limit = 20): Promise<BatchRun[]> {
  const response = await fetch(`${API_PREFIX}/batch/runs?limit=${limit}`);
  if (!response.ok) {
    return parseError(response, 'Failed to load batch runs');
  }
  const payload = (await response.json()) as { runs: BatchRun[] };
  return payload.runs;
}

export async function createBatchJob(payload: {
  watch_dir: string;
  poll_seconds: number;
  auto_fix: boolean;
  enabled: boolean;
  name?: string;
  run_on_create?: boolean;
}): Promise<{ job: BatchJob; run: BatchRun | null }> {
  const response = await fetch(`${API_PREFIX}/batch/jobs`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    return parseError(response, 'Failed to create batch job');
  }

  return (await response.json()) as { job: BatchJob; run: BatchRun | null };
}

export async function updateBatchJob(
  jobId: string,
  payload: {
    name?: string;
    watch_dir?: string;
    poll_seconds?: number;
    auto_fix?: boolean;
    enabled?: boolean;
  }
): Promise<BatchJob> {
  const response = await fetch(`${API_PREFIX}/batch/jobs/${jobId}`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    return parseError(response, 'Failed to update batch job');
  }

  const body = (await response.json()) as { job: BatchJob };
  return body.job;
}

export async function runBatchJob(jobId: string): Promise<BatchRun> {
  const response = await fetch(`${API_PREFIX}/batch/jobs/${jobId}/run`, {
    method: 'POST'
  });

  if (!response.ok) {
    return parseError(response, 'Failed to run batch job');
  }

  const body = (await response.json()) as { run: BatchRun };
  return body.run;
}

export async function deleteBatchJob(jobId: string): Promise<void> {
  const response = await fetch(`${API_PREFIX}/batch/jobs/${jobId}`, {
    method: 'DELETE'
  });

  if (!response.ok) {
    return parseError(response, 'Failed to delete batch job');
  }
}
