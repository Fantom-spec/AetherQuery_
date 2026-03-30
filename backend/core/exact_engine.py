import logging
import time
from typing import Any

from backend.db import duckdb as duckdb_db
from backend.db import mysql as mysql_db
from backend.db import postgres as postgres_db

logger = logging.getLogger(__name__)


def run_exact(query: str, source: str) -> dict[str, Any]:
    source_key = source.lower().strip()
    start = time.time()

    if source_key == "duckdb":
        payload = duckdb_db.execute_query(query)
    elif source_key == "postgres":
        payload = postgres_db.execute_query(query)
    elif source_key == "mysql":
        payload = mysql_db.execute_query(query)
    else:
        raise ValueError(f"Unsupported source: {source}")

    total = time.time() - start
    logger.info("run_exact source=%s duration=%.6f", source_key, total)

    return {
        "result": payload.get("rows", []),
        "columns": payload.get("columns", []),
        "time": payload.get("time", total),
        "approx": False,
        "source": source_key,
    }
