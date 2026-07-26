"""
EDA Script for OMS Postgres Data (oms_core schema)
----------------------------------------------------
Run this OUTSIDE Airflow (locally or via `docker compose exec`), NOT inside the dags/ folder.

Usage:
    python explore_oms_data.py

Produces:
    1. Schema report (column names, types, nullability) for every table
    2. Row counts + duplicate counts
    3. Null counts per column
    4. Sample rows
    5. Unique value breakdown for low-cardinality (categorical) columns
    6. A final summary table across all 8 OMS tables
    7. Saves everything to eda_report.txt for easy review/sharing
"""

import pandas as pd
from sqlalchemy import create_engine

# ---------------------------------------------------------------------------
# 1. CONFIG
# ---------------------------------------------------------------------------
DB_CONN_STRING = "postgresql://neondb_owner:npg_nf0Bpoz8SKtF@ep-tiny-sunset-ah3xbklk-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require"
SCHEMA = "oms_core"
OMS_TABLES = [
    "customers",
    "dates",
    "employees",
    "orderitems",
    "orders",
    "products",
    "stores",
    "suppliers",
]
CATEGORICAL_UNIQUE_THRESHOLD = 20  # columns with fewer unique values than this get their values printed
OUTPUT_FILE = "eda_report.txt"

engine = create_engine(DB_CONN_STRING)


# ---------------------------------------------------------------------------
# 2. HELPERS
# ---------------------------------------------------------------------------
def get_schema_info():
    query = f"""
        SELECT table_name, column_name, data_type, is_nullable, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = '{SCHEMA}'
        ORDER BY table_name, ordinal_position;
    """
    return pd.read_sql(query, engine)


def explore_table(table_name, log_lines):
    df = pd.read_sql(f"SELECT * FROM {SCHEMA}.{table_name}", engine)

    log_lines.append(f"\n{'=' * 70}")
    log_lines.append(f"TABLE: {table_name}   ({len(df)} rows, {len(df.columns)} columns)")
    log_lines.append("=" * 70)

    # dtypes
    log_lines.append("\n-- Column dtypes (pandas) --")
    log_lines.append(df.dtypes.to_string())

    # nulls
    log_lines.append("\n-- Null counts --")
    null_counts = df.isnull().sum()
    nulls_present = null_counts[null_counts > 0]
    if nulls_present.empty:
        log_lines.append("No nulls in any column. ✅")
    else:
        log_lines.append(nulls_present.to_string())

    # duplicates
    dup_count = df.duplicated().sum()
    log_lines.append(f"\n-- Duplicate rows: {dup_count} --")

    # sample
    log_lines.append("\n-- Sample rows (first 3) --")
    log_lines.append(df.head(3).to_string())

    # categorical breakdown
    log_lines.append("\n-- Categorical / text column breakdown --")
    text_cols = df.select_dtypes(include=["object", "string"]).columns
    if len(text_cols) == 0:
        log_lines.append("No text/object columns.")
    for col in text_cols:
        n_unique = df[col].nunique()
        if n_unique < CATEGORICAL_UNIQUE_THRESHOLD:
            log_lines.append(f"  {col}: {n_unique} unique -> {sorted(df[col].dropna().unique().tolist())}")
        else:
            sample_vals = df[col].dropna().unique()[:3].tolist()
            log_lines.append(f"  {col}: {n_unique} unique values (sample: {sample_vals})")

    return {
        "table": table_name,
        "rows": len(df),
        "columns": len(df.columns),
        "nulls_total": int(null_counts.sum()),
        "duplicates": int(dup_count),
    }


# ---------------------------------------------------------------------------
# 3. MAIN
# ---------------------------------------------------------------------------
def main():
    log_lines = []

    log_lines.append("OMS DATA EDA REPORT")
    log_lines.append(f"Schema: {SCHEMA}")
    log_lines.append(f"Tables: {', '.join(OMS_TABLES)}")

    # --- Schema info for all tables at once ---
    log_lines.append("\n" + "#" * 70)
    log_lines.append("# FULL SCHEMA (information_schema.columns)")
    log_lines.append("#" * 70)
    schema_df = get_schema_info()
    log_lines.append(schema_df.to_string())

    # --- Per-table deep dive ---
    summary_rows = []
    for table in OMS_TABLES:
        print(f"Exploring {table} ...")
        try:
            summary = explore_table(table, log_lines)
            summary_rows.append(summary)
        except Exception as e:
            log_lines.append(f"\n[ERROR] Failed to explore table '{table}': {e}")
            print(f"  -> ERROR: {e}")

    # --- Final summary table ---
    log_lines.append("\n" + "#" * 70)
    log_lines.append("# SUMMARY ACROSS ALL TABLES")
    log_lines.append("#" * 70)
    summary_df = pd.DataFrame(summary_rows)
    log_lines.append(summary_df.to_string(index=False))

    # --- Write report to file + print to console ---
    report_text = "\n".join(log_lines)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(report_text)

    print("\n" + "=" * 70)
    print(f"EDA complete. Full report saved to: {OUTPUT_FILE}")
    print("=" * 70)
    print("\n--- Quick Summary ---")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()