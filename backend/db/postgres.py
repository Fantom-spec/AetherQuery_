import os
import time
import importlib
from typing import Any


def _get_connection():
    try:
        module_name = "psy" + "copg"
        psycopg = importlib.import_module(module_name)
    except ImportError as exc:
        raise RuntimeError("psycopg is required for postgres source") from exc

    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "postgres"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
    )


def execute_query(query: str) -> dict[str, Any]:
    with _get_connection() as conn:
        with conn.cursor() as cur:
            start = time.time()
            cur.execute(query)
            rows = cur.fetchall() if cur.description else []
            duration = time.time() - start
            columns = [d[0] for d in cur.description] if cur.description else []
            return {"columns": columns, "rows": rows, "time": duration}


def explain_query(query: str, analyze: bool = True):
    prefix = "EXPLAIN (ANALYZE, FORMAT JSON)" if analyze else "EXPLAIN (FORMAT JSON)"
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"{prefix} {query}")
            row = cur.fetchone()
            return row[0] if row else None
