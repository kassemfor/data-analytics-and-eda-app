import { useEffect, useMemo, useState } from 'react';
import {
  createBatchJob,
  deleteBatchJob,
  listBatchJobs,
  listBatchRuns,
  runBatchJob,
  runQuery,
  updateBatchJob,
  uploadCsv
} from './lib/api';
import type { BatchJob, BatchRun, PipelineReport, QueryResponse } from './lib/types';

type EngineMode = 'duckdb' | 'motherduck';

function formatValue(value: unknown): string {
  if (typeof value === 'number') {
    return Number.isInteger(value) ? value.toLocaleString() : value.toFixed(4);
  }
  if (value === null || value === undefined) {
    return 'null';
  }
  return String(value);
}

function App() {
  const [file, setFile] = useState<File | null>(null);
  const [isUploading, setUploading] = useState(false);
  const [isQuerying, setQuerying] = useState(false);
  const [isBatchBusy, setBatchBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [datasetId, setDatasetId] = useState<string | null>(null);
  const [report, setReport] = useState<PipelineReport | null>(null);

  const [sql, setSql] = useState('SELECT * FROM dataset LIMIT 25');
  const [engine, setEngine] = useState<EngineMode>('duckdb');
  const [motherduckToken, setMotherduckToken] = useState('');
  const [queryResult, setQueryResult] = useState<QueryResponse | null>(null);

  const [batchJobs, setBatchJobs] = useState<BatchJob[]>([]);
  const [batchRuns, setBatchRuns] = useState<BatchRun[]>([]);
  const [watchDir, setWatchDir] = useState('/data/inbox');
  const [jobName, setJobName] = useState('');
  const [pollSeconds, setPollSeconds] = useState(300);

  const summaryCards = useMemo(() => {
    if (!report) return [];

    return [
      {
        title: 'Rows',
        before: report.before.rows,
        after: report.after.rows
      },
      {
        title: 'Missing Cells',
        before: report.before.missing_cells,
        after: report.after.missing_cells
      },
      {
        title: 'Duplicate Rows',
        before: report.before.duplicate_rows,
        after: report.after.duplicate_rows
      },
      {
        title: 'Columns',
        before: report.before.columns,
        after: report.after.columns
      }
    ];
  }, [report]);

  async function refreshBatchData() {
    const [jobs, runs] = await Promise.all([listBatchJobs(), listBatchRuns(15)]);
    setBatchJobs(jobs);
    setBatchRuns(runs);
    setError((previous) =>
      previous === 'Could not load batch scheduler data.' ? null : previous
    );
  }

  useEffect(() => {
    let isMounted = true;

    const load = async () => {
      try {
        const [jobs, runs] = await Promise.all([listBatchJobs(), listBatchRuns(15)]);
        if (!isMounted) return;
        setBatchJobs(jobs);
        setBatchRuns(runs);
        setError((previous) =>
          previous === 'Could not load batch scheduler data.' ? null : previous
        );
      } catch {
        if (!isMounted) return;
        setError('Could not load batch scheduler data.');
      }
    };

    void load();
    const interval = window.setInterval(() => {
      void load();
    }, 15000);

    return () => {
      isMounted = false;
      window.clearInterval(interval);
    };
  }, []);

  async function handleUpload() {
    if (!file) {
      setError('Pick a CSV file first.');
      return;
    }

    setError(null);
    setUploading(true);
    setQueryResult(null);

    try {
      const response = await uploadCsv(file, true);
      setDatasetId(response.dataset_id);
      setReport(response.report);
      if (response.query_suggestions.length > 0) {
        setSql(response.query_suggestions[0].sql);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Upload failed.');
    } finally {
      setUploading(false);
    }
  }

  async function handleRunQuery() {
    if (!datasetId) {
      setError('Upload a dataset first.');
      return;
    }

    setError(null);
    setQuerying(true);

    try {
      const result = await runQuery(datasetId, sql, engine, motherduckToken);
      setQueryResult(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Query failed.');
    } finally {
      setQuerying(false);
    }
  }

  async function handleCreateBatchJob() {
    if (!watchDir.trim()) {
      setError('Enter a watch folder path.');
      return;
    }

    setError(null);
    setBatchBusy(true);

    try {
      await createBatchJob({
        name: jobName.trim() || undefined,
        watch_dir: watchDir.trim(),
        poll_seconds: pollSeconds,
        auto_fix: true,
        enabled: true,
        run_on_create: true
      });
      await refreshBatchData();
      setJobName('');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not create batch job.');
    } finally {
      setBatchBusy(false);
    }
  }

  async function handleRunBatchJob(jobId: string) {
    setError(null);
    setBatchBusy(true);

    try {
      await runBatchJob(jobId);
      await refreshBatchData();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not run batch job.');
    } finally {
      setBatchBusy(false);
    }
  }

  async function handleToggleBatchJob(job: BatchJob) {
    setError(null);
    setBatchBusy(true);

    try {
      await updateBatchJob(job.job_id, { enabled: !job.enabled });
      await refreshBatchData();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not update batch job.');
    } finally {
      setBatchBusy(false);
    }
  }

  async function handleDeleteBatchJob(jobId: string) {
    setError(null);
    setBatchBusy(true);

    try {
      await deleteBatchJob(jobId);
      await refreshBatchData();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not delete batch job.');
    } finally {
      setBatchBusy(false);
    }
  }

  return (
    <div className="app-shell">
      <div className="ambient ambient-a" />
      <div className="ambient ambient-b" />

      <main className="app-grid">
        <section className="hero panel reveal">
          <p className="eyebrow">Modern Data Analytics Stack</p>
          <h1>Auto EDA + Parquet + DuckDB + MotherDuck</h1>
          <p>
            Upload a CSV and the app automatically applies the 7 core EDA data-quality fixes,
            writes clean Parquet, benchmarks Pandas vs DuckDB, and lets you query with SQL.
          </p>
          <div className="file-controls">
            <label className="file-picker" htmlFor="csv-input">
              {file ? file.name : 'Choose CSV'}
            </label>
            <input
              id="csv-input"
              type="file"
              accept=".csv"
              onChange={(event) => setFile(event.target.files?.[0] ?? null)}
            />
            <button onClick={handleUpload} disabled={isUploading}>
              {isUploading ? 'Running pipeline...' : 'Run Automated Pipeline'}
            </button>
          </div>
          {datasetId ? <p className="dataset-id">Dataset ID: {datasetId}</p> : null}
        </section>

        <section className="panel reveal">
          <h2>Batch Folder Automation</h2>
          <p>
            Point to a folder of CSV files to ingest in the background. Changed files are
            automatically profiled and saved as datasets.
          </p>
          <div className="batch-form">
            <label>
              Job Name (optional)
              <input
                value={jobName}
                onChange={(event) => setJobName(event.target.value)}
                placeholder="Nightly Sales Folder"
              />
            </label>
            <label>
              Watch Directory
              <input
                value={watchDir}
                onChange={(event) => setWatchDir(event.target.value)}
                placeholder="/data/inbox"
              />
            </label>
            <div className="inline-fields">
              <label>
                Poll Seconds
                <input
                  type="number"
                  min={10}
                  value={pollSeconds}
                  onChange={(event) => setPollSeconds(Number(event.target.value) || 10)}
                />
              </label>
              <button onClick={handleCreateBatchJob} disabled={isBatchBusy}>
                {isBatchBusy ? 'Working...' : 'Create + Run Job'}
              </button>
            </div>
          </div>

          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Job</th>
                  <th>Folder</th>
                  <th>Status</th>
                  <th>Processed Files</th>
                  <th>Next Run</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {batchJobs.map((job) => (
                  <tr key={job.job_id}>
                    <td>{job.name}</td>
                    <td>{job.watch_dir}</td>
                    <td>
                      <span className={`status-pill status-${job.last_status}`}>{job.last_status}</span>
                    </td>
                    <td>{job.processed_files}</td>
                    <td>{job.next_run_at ?? '-'}</td>
                    <td>
                      <div className="job-actions">
                        <button onClick={() => handleRunBatchJob(job.job_id)} disabled={isBatchBusy || job.running}>
                          Run Now
                        </button>
                        <button onClick={() => handleToggleBatchJob(job)} disabled={isBatchBusy}>
                          {job.enabled ? 'Pause' : 'Resume'}
                        </button>
                        <button onClick={() => handleDeleteBatchJob(job.job_id)} disabled={isBatchBusy}>
                          Delete
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
                {batchJobs.length === 0 ? (
                  <tr>
                    <td colSpan={6}>No batch jobs configured.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>

          <h3>Recent Batch Runs</h3>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Job</th>
                  <th>Status</th>
                  <th>Seen</th>
                  <th>Processed</th>
                  <th>Datasets</th>
                  <th>Finished At</th>
                </tr>
              </thead>
              <tbody>
                {batchRuns.map((run) => (
                  <tr key={run.run_id}>
                    <td>{run.job_name}</td>
                    <td>
                      <span className={`status-pill status-${run.status}`}>{run.status}</span>
                    </td>
                    <td>{run.files_seen}</td>
                    <td>{run.files_processed}</td>
                    <td>{run.datasets_created.length}</td>
                    <td>{run.finished_at}</td>
                  </tr>
                ))}
                {batchRuns.length === 0 ? (
                  <tr>
                    <td colSpan={6}>No batch runs yet.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>

        {report ? (
          <>
            <section className="panel reveal">
              <h2>Quality Delta</h2>
              <div className="card-grid">
                {summaryCards.map((card) => (
                  <article key={card.title} className="metric-card">
                    <h3>{card.title}</h3>
                    <p>
                      <span className="label">Before</span> {formatValue(card.before)}
                    </p>
                    <p>
                      <span className="label">After</span> {formatValue(card.after)}
                    </p>
                  </article>
                ))}
              </div>
            </section>

            <section className="panel reveal">
              <h2>EDA Tricks Covered</h2>
              <div className="chips">
                {report.tricks_covered.map((item) => (
                  <span key={item} className="chip">
                    {item}
                  </span>
                ))}
              </div>
            </section>

            <section className="panel reveal">
              <h2>Fixes Applied</h2>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Operation</th>
                      <th>Columns</th>
                      <th>Rows Impacted</th>
                      <th>Duplicates Removed</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.fixes_applied.map((fix) => (
                      <tr key={fix.operation}>
                        <td>{fix.operation}</td>
                        <td>{formatValue(fix.columns_touched ?? '-')}</td>
                        <td>{formatValue(fix.rows_impacted ?? '-')}</td>
                        <td>{formatValue(fix.duplicates_removed ?? '-')}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="panel reveal">
              <h2>Pandas vs DuckDB Benchmark</h2>
              <div className="benchmark-row">
                <div>
                  <p className="label">Pandas</p>
                  <strong>{report.benchmark.pandas_ms} ms</strong>
                </div>
                <div>
                  <p className="label">DuckDB</p>
                  <strong>{report.benchmark.duckdb_ms} ms</strong>
                </div>
                <div>
                  <p className="label">Query</p>
                  <code>{report.benchmark.query}</code>
                </div>
              </div>
            </section>

            <section className="panel reveal">
              <h2>SQL Query Runner</h2>
              <div className="query-tools">
                <div className="suggestions">
                  {report.query_suggestions.map((suggestion) => (
                    <button key={suggestion.name} onClick={() => setSql(suggestion.sql)}>
                      {suggestion.name}
                    </button>
                  ))}
                </div>
                <div className="engine-row">
                  <label>
                    Engine
                    <select value={engine} onChange={(event) => setEngine(event.target.value as EngineMode)}>
                      <option value="duckdb">DuckDB</option>
                      <option value="motherduck">MotherDuck</option>
                    </select>
                  </label>
                  {engine === 'motherduck' ? (
                    <label>
                      MotherDuck Token
                      <input
                        type="password"
                        value={motherduckToken}
                        onChange={(event) => setMotherduckToken(event.target.value)}
                        placeholder="md_..."
                      />
                    </label>
                  ) : null}
                </div>
                <textarea value={sql} onChange={(event) => setSql(event.target.value)} rows={8} />
                <button onClick={handleRunQuery} disabled={isQuerying}>
                  {isQuerying ? 'Executing...' : 'Run Query'}
                </button>
              </div>

              {queryResult ? (
                <div className="table-wrap">
                  <p>
                    Returned {queryResult.row_count} row(s) using {queryResult.engine}.
                  </p>
                  <table>
                    <thead>
                      <tr>
                        {queryResult.columns.map((column) => (
                          <th key={column}>{column}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {queryResult.rows.slice(0, 50).map((row, rowIndex) => (
                        <tr key={`row-${rowIndex}`}>
                          {row.map((cell, cellIndex) => (
                            <td key={`cell-${rowIndex}-${cellIndex}`}>{formatValue(cell)}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : null}
            </section>
          </>
        ) : null}

        {error ? <section className="panel error">{error}</section> : null}
      </main>
    </div>
  );
}

export default App;
