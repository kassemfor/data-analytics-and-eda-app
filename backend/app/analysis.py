from __future__ import annotations

import time
from typing import Any

import duckdb
import numpy as np
import pandas as pd


def _to_native(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, (pd.Timedelta,)):
        return str(value)
    if pd.isna(value):
        return None
    return value


def json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [json_ready(item) for item in value]
    return _to_native(value)


def infer_and_fix_data_types(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    working = df.copy()
    conversions: list[dict[str, Any]] = []

    for col in working.columns:
        series = working[col]
        if series.dtype != "object":
            continue

        non_null = series.dropna().astype(str)
        if non_null.empty:
            continue

        numeric_converted = pd.to_numeric(series, errors="coerce")
        numeric_ratio = float(numeric_converted.notna().sum() / len(non_null))

        if numeric_ratio >= 0.85:
            working[col] = numeric_converted
            conversions.append(
                {
                    "column": col,
                    "from": "object",
                    "to": "numeric",
                    "confidence": round(numeric_ratio, 3),
                }
            )
            continue

        # Skip datetime parsing unless values look date-like to avoid noisy parse warnings.
        date_like_ratio = float(non_null.str.contains(r"\d{2,4}[-/]\d{1,2}[-/]\d{1,2}", regex=True).mean())
        if date_like_ratio >= 0.6:
            datetime_converted = pd.to_datetime(series, errors="coerce", utc=True)
            datetime_ratio = float(datetime_converted.notna().sum() / len(non_null))
            if datetime_ratio >= 0.85:
                working[col] = datetime_converted
                conversions.append(
                    {
                        "column": col,
                        "from": "object",
                        "to": "datetime",
                        "confidence": round(datetime_ratio, 3),
                    }
                )

    return working, conversions


def profile_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    row_count = int(len(df))
    column_count = int(len(df.columns))

    columns: list[dict[str, Any]] = []
    for col in df.columns:
        missing = int(df[col].isna().sum())
        columns.append(
            {
                "name": str(col),
                "dtype": str(df[col].dtype),
                "missing": missing,
                "missing_pct": round((missing / row_count * 100.0) if row_count else 0.0, 2),
                "unique": int(df[col].nunique(dropna=True)),
            }
        )

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    numeric_summary: list[dict[str, Any]] = []
    for col in numeric_cols:
        series = df[col]
        stats = {
            "column": str(col),
            "count": int(series.count()),
            "mean": _to_native(series.mean()),
            "median": _to_native(series.median()),
            "std": _to_native(series.std()),
            "min": _to_native(series.min()),
            "max": _to_native(series.max()),
            "skew": _to_native(series.skew()),
        }
        numeric_summary.append(stats)

    categorical_cols = (
        df.select_dtypes(include=["object", "category", "bool"]).columns.tolist()
    )
    categorical_summary: list[dict[str, Any]] = []
    for col in categorical_cols:
        top_values = df[col].fillna("<missing>").value_counts().head(5).to_dict()
        categorical_summary.append(
            {
                "column": str(col),
                "top_values": {str(k): int(v) for k, v in top_values.items()},
            }
        )

    high_corr_pairs = find_high_correlations(df)

    return json_ready(
        {
            "rows": row_count,
            "columns": column_count,
            "duplicate_rows": int(df.duplicated().sum()),
            "missing_cells": int(df.isna().sum().sum()),
            "column_profile": columns,
            "numeric_summary": numeric_summary,
            "categorical_summary": categorical_summary,
            "high_correlation_pairs": high_corr_pairs,
        }
    )


def find_high_correlations(df: pd.DataFrame, threshold: float = 0.9) -> list[dict[str, Any]]:
    numeric = df.select_dtypes(include=[np.number])
    if numeric.shape[1] < 2:
        return []

    corr = numeric.corr(numeric_only=True)
    pairs: list[dict[str, Any]] = []

    for i, col_a in enumerate(corr.columns):
        for j, col_b in enumerate(corr.columns):
            if j <= i:
                continue
            value = corr.iloc[i, j]
            if pd.isna(value):
                continue
            if abs(float(value)) >= threshold:
                pairs.append(
                    {
                        "feature_a": str(col_a),
                        "feature_b": str(col_b),
                        "correlation": round(float(value), 4),
                    }
                )

    pairs.sort(key=lambda item: abs(item["correlation"]), reverse=True)
    return pairs


def _fill_missing_values(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    working = df.copy()
    filled_by_column: dict[str, int] = {}

    for col in working.columns:
        missing = int(working[col].isna().sum())
        if missing == 0:
            continue

        if pd.api.types.is_numeric_dtype(working[col]):
            fill_value = working[col].median()
            if pd.isna(fill_value):
                fill_value = 0
            working[col] = working[col].fillna(fill_value)
        else:
            mode = working[col].mode(dropna=True)
            fill_value = mode.iloc[0] if not mode.empty else "unknown"
            working[col] = working[col].fillna(fill_value)

        filled_by_column[str(col)] = missing

    return working, {
        "operation": "fill_missing_values",
        "columns_touched": len(filled_by_column),
        "filled_cells": int(sum(filled_by_column.values())),
        "detail": filled_by_column,
    }


def _drop_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    duplicate_rows = int(df.duplicated().sum())
    deduped = df.drop_duplicates(ignore_index=True)
    return deduped, {
        "operation": "remove_duplicate_rows",
        "duplicates_removed": duplicate_rows,
    }


def _clip_outliers_iqr(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    working = df.copy()
    numeric_cols = working.select_dtypes(include=[np.number]).columns
    outlier_map: dict[str, int] = {}

    for col in numeric_cols:
        series = working[col]
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if pd.isna(iqr) or iqr == 0:
            continue

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        mask = (series < lower) | (series > upper)
        outlier_count = int(mask.sum())
        if outlier_count == 0:
            continue

        working[col] = series.clip(lower=lower, upper=upper)
        outlier_map[str(col)] = outlier_count

    return working, {
        "operation": "cap_outliers_iqr",
        "columns_touched": len(outlier_map),
        "rows_impacted": int(sum(outlier_map.values())),
        "detail": outlier_map,
    }


def _normalize_categories(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    working = df.copy()
    object_cols = working.select_dtypes(include=["object", "category"]).columns
    normalization_changes: dict[str, int] = {}

    for col in object_cols:
        original = working[col]

        normalized = (
            original.astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
            .str.lower()
        )

        null_mask = original.isna()
        normalized = normalized.mask(null_mask, other=np.nan)

        changed = int((normalized.fillna("<na>") != original.fillna("<na>").astype(str)).sum())
        if changed > 0:
            working[col] = normalized
            normalization_changes[str(col)] = changed

    return working, {
        "operation": "normalize_categorical_text",
        "columns_touched": len(normalization_changes),
        "rows_impacted": int(sum(normalization_changes.values())),
        "detail": normalization_changes,
    }


def _transform_skewed_numeric(df: pd.DataFrame, skew_threshold: float = 1.0) -> tuple[pd.DataFrame, dict[str, Any]]:
    working = df.copy()
    numeric_cols = working.select_dtypes(include=[np.number]).columns
    transformed: list[str] = []

    for col in numeric_cols:
        series = working[col]
        skewness = series.skew()
        if pd.isna(skewness) or abs(float(skewness)) < skew_threshold:
            continue
        if series.min() < 0:
            continue

        working[col] = np.log1p(series)
        transformed.append(str(col))

    return working, {
        "operation": "log_transform_skewed_features",
        "columns_touched": len(transformed),
        "detail": transformed,
    }


def run_automated_eda_pipeline(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    report: dict[str, Any] = {
        "tricks_covered": [
            "missing-value detection and imputation",
            "duplicate row detection and removal",
            "IQR-based outlier capping",
            "inconsistent text category normalization",
            "automatic data-type inference",
            "skewness correction using log1p",
            "redundant feature detection with correlation",
        ]
    }

    before = profile_dataframe(df)
    report["before"] = before

    typed_df, type_conversions = infer_and_fix_data_types(df)
    report["type_conversions"] = type_conversions

    filled_df, missing_fix = _fill_missing_values(typed_df)
    deduped_df, dedupe_fix = _drop_duplicates(filled_df)
    clipped_df, outlier_fix = _clip_outliers_iqr(deduped_df)
    normalized_df, category_fix = _normalize_categories(clipped_df)
    transformed_df, skew_fix = _transform_skewed_numeric(normalized_df)

    corr_pairs = find_high_correlations(transformed_df)
    correlation_fix = {
        "operation": "high_correlation_feature_detection",
        "pairs_found": len(corr_pairs),
        "detail": corr_pairs[:20],
    }

    fixes = [
        {
            "operation": "auto_type_inference",
            "columns_touched": len(type_conversions),
            "detail": type_conversions,
        },
        missing_fix,
        dedupe_fix,
        outlier_fix,
        category_fix,
        skew_fix,
        correlation_fix,
    ]

    after = profile_dataframe(transformed_df)

    report["after"] = after
    report["fixes_applied"] = json_ready(fixes)
    report["quality_delta"] = {
        "duplicate_rows_before": before["duplicate_rows"],
        "duplicate_rows_after": after["duplicate_rows"],
        "missing_cells_before": before["missing_cells"],
        "missing_cells_after": after["missing_cells"],
    }

    return transformed_df, json_ready(report)


def benchmark_pandas_vs_duckdb(df: pd.DataFrame, parquet_path: str) -> dict[str, Any]:
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    benchmark_query = "SELECT COUNT(*) AS row_count FROM read_parquet(?)"
    if numeric_cols:
        benchmark_query = (
            f'SELECT COUNT(*) AS row_count, AVG("{numeric_cols[0]}") AS mean_value '
            "FROM read_parquet(?)"
        )

    start = time.perf_counter()
    pandas_result: dict[str, Any] = {"row_count": int(len(df))}
    if numeric_cols:
        pandas_result["mean_value"] = _to_native(df[numeric_cols[0]].mean())
    pandas_ms = (time.perf_counter() - start) * 1000

    con = duckdb.connect(database=":memory:")
    start = time.perf_counter()
    duckdb_row = con.execute(benchmark_query, [parquet_path]).fetchone()
    duckdb_ms = (time.perf_counter() - start) * 1000
    con.close()

    duckdb_result: dict[str, Any] = {"row_count": int(duckdb_row[0])}
    if numeric_cols and len(duckdb_row) > 1:
        duckdb_result["mean_value"] = _to_native(duckdb_row[1])

    return json_ready(
        {
            "query": benchmark_query,
            "pandas_ms": round(pandas_ms, 3),
            "duckdb_ms": round(duckdb_ms, 3),
            "pandas_result": pandas_result,
            "duckdb_result": duckdb_result,
        }
    )


def build_query_suggestions(df: pd.DataFrame) -> list[dict[str, str]]:
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    categorical_cols = (
        df.select_dtypes(include=["object", "category", "bool"]).columns.tolist()
    )

    suggestions: list[dict[str, str]] = [
        {
            "name": "Preview rows",
            "sql": "SELECT * FROM dataset LIMIT 25",
        },
        {
            "name": "Row count",
            "sql": "SELECT COUNT(*) AS row_count FROM dataset",
        },
    ]

    if numeric_cols:
        first_numeric = numeric_cols[0]
        suggestions.append(
            {
                "name": f"Distribution stats for {first_numeric}",
                "sql": (
                    f'SELECT MIN("{first_numeric}") AS min_value, '
                    f'AVG("{first_numeric}") AS avg_value, '
                    f'MAX("{first_numeric}") AS max_value '
                    "FROM dataset"
                ),
            }
        )

    if categorical_cols and numeric_cols:
        first_category = categorical_cols[0]
        first_numeric = numeric_cols[0]
        suggestions.append(
            {
                "name": f"Grouped mean by {first_category}",
                "sql": (
                    f'SELECT "{first_category}", AVG("{first_numeric}") AS avg_metric '
                    "FROM dataset "
                    f'GROUP BY "{first_category}" '
                    "ORDER BY avg_metric DESC LIMIT 20"
                ),
            }
        )

    if len(numeric_cols) >= 2:
        col_a = numeric_cols[0]
        col_b = numeric_cols[1]
        suggestions.append(
            {
                "name": f"Correlation proxy: {col_a} vs {col_b}",
                "sql": (
                    f'SELECT CORR("{col_a}", "{col_b}") AS correlation_value '
                    "FROM dataset"
                ),
            }
        )

    return suggestions
