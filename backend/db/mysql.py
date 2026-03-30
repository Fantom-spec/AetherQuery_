import os
import time
import importlib
from typing import Any


def _get_connection():
    try:
        module_name = "mysql" + "." + "connector"
        mysql_connector = importlib.import_module(module_name)
    except ImportError as exc:
        raise RuntimeError("mysql-connector-python is required for mysql source") from exc

    return mysql_connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "root"),
        database=os.getenv("MYSQL_DATABASE", "mysql"),
    )


def execute_query(query: str) -> dict[str, Any]:
    conn = _get_connection()
    try:
        cur = conn.cursor()
        start = time.time()
        cur.execute(query)
        rows = cur.fetchall() if cur.description else []
        duration = time.time() - start
        columns = [d[0] for d in cur.description] if cur.description else []
        return {"columns": columns, "rows": rows, "time": duration}
    finally:
        conn.close()


def explain_query(query: str, analyze: bool = True):
    conn = _get_connection()
    try:
        cur = conn.cursor()
        prefix = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
        cur.execute(f"{prefix} {query}")
        return cur.fetchall()
    finally:
        conn.close()
