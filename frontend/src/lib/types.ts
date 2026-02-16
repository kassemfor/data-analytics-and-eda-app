export interface ColumnProfile {
  name: string;
  dtype: string;
  missing: number;
  missing_pct: number;
  unique: number;
}

export interface NumericSummary {
  column: string;
  count: number;
  mean: number | null;
  median: number | null;
  std: number | null;
  min: number | null;
  max: number | null;
  skew: number | null;
}

export interface CategorySummary {
  column: string;
  top_values: Record<string, number>;
}

export interface FixDetail {
  operation: string;
  columns_touched?: number;
  rows_impacted?: number;
  duplicates_removed?: number;
  detail?: unknown;
  pairs_found?: number;
}

export interface PipelineReport {
  created_at: string;
  source_file: string;
  before: {
    rows: number;
    columns: number;
    duplicate_rows: number;
    missing_cells: number;
    column_profile: ColumnProfile[];
    numeric_summary: NumericSummary[];
    categorical_summary: CategorySummary[];
  };
  after: {
    rows: number;
    columns: number;
    duplicate_rows: number;
    missing_cells: number;
    column_profile: ColumnProfile[];
    numeric_summary: NumericSummary[];
    categorical_summary: CategorySummary[];
  };
  tricks_covered: string[];
  fixes_applied: FixDetail[];
  quality_delta: {
    duplicate_rows_before: number;
    duplicate_rows_after: number;
    missing_cells_before: number;
    missing_cells_after: number;
  };
  benchmark: {
    query: string;
    pandas_ms: number;
    duckdb_ms: number;
    pandas_result: Record<string, unknown>;
    duckdb_result: Record<string, unknown>;
  };
  query_suggestions: { name: string; sql: string }[];
}

export interface UploadResponse {
  dataset_id: string;
  report: PipelineReport;
  benchmark: PipelineReport['benchmark'];
  query_suggestions: PipelineReport['query_suggestions'];
  parquet_path: string;
}

export interface QueryResponse {
  columns: string[];
  rows: unknown[][];
  row_count: number;
  engine: 'duckdb' | 'motherduck';
}

export interface BatchJob {
  job_id: string;
  name: string;
  watch_dir: string;
  poll_seconds: number;
  auto_fix: boolean;
  enabled: boolean;
  created_at: string;
  last_run_at: string | null;
  last_status: string;
  last_error: string | null;
  processed_files: number;
  next_run_at: string | null;
  running: boolean;
}

export interface BatchRun {
  run_id: string;
  job_id: string;
  job_name: string;
  triggered_by: string;
  started_at: string;
  finished_at: string;
  status: 'success' | 'partial_success' | 'failed';
  files_seen: number;
  files_processed: number;
  datasets_created: Array<{
    dataset_id: string;
    source_file: string;
    source_path: string;
  }>;
  errors: string[];
}
