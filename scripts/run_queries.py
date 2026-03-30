import os
import time
import json
import textwrap

import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", 5433)),
    "dbname":   os.getenv("DB_NAME",     "ecommerce"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def run_query(cur, sql: str, label: str = ""):
    start = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    elapsed = time.perf_counter() - start
    col_names = [d[0] for d in cur.description] if cur.description else []
    return rows, col_names, elapsed


def explain_analyze(cur, sql: str):
    explain_sql = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql
    cur.execute(explain_sql)
    plan = cur.fetchone()[0]
    return plan


def print_table(rows, col_names, max_rows: int = 10):
    if not rows:
        print("  (no rows returned)")
        return
    col_widths = [max(len(str(c)), max((len(str(r[i])) for r in rows[:max_rows]), default=0))
                  for i, c in enumerate(col_names)]
    header = " | ".join(str(c).ljust(col_widths[i]) for i, c in enumerate(col_names))
    sep    = "-+-".join("-" * w for w in col_widths)
    print(f"  {header}")
    print(f"  {sep}")
    for row in rows[:max_rows]:
        line = " | ".join(str(v).ljust(col_widths[i]) for i, v in enumerate(row))
        print(f"  {line}")
    if len(rows) > max_rows:
        print(f"  ... ({len(rows) - max_rows} more rows)")


def extract_plan_summary(plan_json: list) -> dict:
    node = plan_json[0]["Plan"]
    return {
        "planning_time_ms":   plan_json[0].get("Planning Time", 0),
        "execution_time_ms":  plan_json[0].get("Execution Time", 0),
        "node_type":          node.get("Node Type"),
        "actual_rows":        node.get("Actual Rows"),
        "total_cost":         node.get("Total Cost"),
    }


def run_file(conn, filename: str, label: str, use_explain: bool = False):
    filepath = os.path.join(SQL_DIR, filename)
    with open(filepath) as f:
        raw = f.read()

    print(f"\n{'=' * 70}")
    print(f"  {label}")
    print(f"{'=' * 70}")

    cleaned_lines = [l for l in raw.split("\n") if not l.strip().startswith("--")]
    cleaned_raw = "\n".join(cleaned_lines)
    statements = [s.strip() for s in cleaned_raw.split(";") if s.strip()]

    with conn.cursor() as cur:
        for stmt in statements:
            if not stmt:
                continue
            lines = stmt.split("\n")
            comment_lines = []
            sql_lines = []
            for line in lines:
                if line.strip().startswith("--"):
                    comment_lines.append(line)
                else:
                    sql_lines.append(line)
            actual_sql = "\n".join(sql_lines).strip()
            if not actual_sql:
                continue
            if comment_lines:
                print(f"\n  {comment_lines[0].strip()}")

            lowered = actual_sql.lower().lstrip()
            if lowered.startswith("explain"):
                try:
                    cur.execute(actual_sql)
                    plan_rows = cur.fetchall()
                    print("\n  EXPLAIN ANALYZE output (first 20 lines):")
                    for row in plan_rows[:20]:
                        print(f"    {row[0]}")
                except Exception as exc:
                    print(f"  [EXPLAIN error]: {exc}")
                    conn.rollback()
            else:
                try:
                    rows, cols, elapsed = run_query(cur, actual_sql)
                    print(f"\n  Query completed in {elapsed * 1000:.1f} ms  |  {len(rows)} rows")
                    print_table(rows, cols, max_rows=8)
                except Exception as exc:
                    print(f"  [Query error]: {exc}")
                    conn.rollback()


def dataset_overview(conn):
    tables = [
        "customers", "sellers", "products", "orders",
        "order_items", "order_reviews", "order_payments",
    ]
    print(f"\n{'=' * 70}")
    print("  DATASET OVERVIEW")
    print(f"{'=' * 70}")
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            count = cur.fetchone()[0]
            print(f"  {t:<25} {count:>10,} rows")

        cur.execute(
            "SELECT MIN(purchase_timestamp), MAX(purchase_timestamp) FROM orders"
        )
        min_d, max_d = cur.fetchone()
        print(f"\n  Date range: {min_d.date()} → {max_d.date()}")


def main():
    print("Connecting to PostgreSQL...")
    conn = get_connection()
    conn.autocommit = True

    dataset_overview(conn)

    run_file(conn, "tier1_basic.sql",        "TIER 1: Basic Queries")
    run_file(conn, "tier2_intermediate.sql", "TIER 2: Intermediate Queries (with EXPLAIN ANALYZE)")
    run_file(conn, "tier3_advanced.sql",     "TIER 3: Advanced Window Functions (with EXPLAIN ANALYZE)")
    run_file(conn, "tier4_optimization.sql", "TIER 4: Query Optimization Pairs")

    conn.close()
    print(f"\n{'=' * 70}")
    print("  All queries complete.")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
