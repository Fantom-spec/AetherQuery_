import os
import re
import uuid
from pathlib import Path
from typing import Any

import duckdb


_BASE_DIR = Path(__file__).resolve().parents[2]
_DB_PATH = os.getenv("AETHERQUERY_DUCKDB_PATH", str(_BASE_DIR / "datasets" / "aetherquery.duckdb"))
_CONNECTION = duckdb.connect(database=_DB_PATH, read_only=False)


def get_connection() -> duckdb.DuckDBPyConnection:
    return _CONNECTION


def _safe_identifier(name: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if not clean:
        clean = f"table_{uuid.uuid4().hex[:8]}"
    if clean[0].isdigit():
        clean = f"t_{clean}"
    return clean.lower()


def create_table_from_csv(csv_path: str, table_name: str | None = None) -> str:
    conn = get_connection()
    generated_name = table_name or f"table_{uuid.uuid4().hex}"
    safe_name = _safe_identifier(generated_name)
    escaped_path = csv_path.replace("'", "''")
    conn.execute(
        f"CREATE OR REPLACE VIEW {safe_name} AS SELECT * FROM read_csv_auto('{escaped_path}');"
    )
    return safe_name


def execute_query(query: str) -> dict[str, Any]:
    conn = get_connection()
    rows = conn.execute(query).fetchall()
    columns = [d[0] for d in conn.description]
    return {"columns": columns, "rows": rows}


def explain_query(query: str, analyze: bool = True) -> Any:
    conn = get_connection()
    explain_prefix = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
    return conn.execute(f"{explain_prefix} {query}").fetchall()
