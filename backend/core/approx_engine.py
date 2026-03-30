import re
from typing import Any

from backend.core.exact_engine import run_exact

SAMPLE_RATE = 0.1


def _sample_clause(source: str) -> str:
    if source == "duckdb":
        return "USING SAMPLE 10 PERCENT"
    return "TABLESAMPLE SYSTEM (10)"


def _rewrite_agg_query(query: str, source: str) -> str:
    normalized = query.strip().rstrip(";")

    count_match = re.match(
        r"(?is)^select\s+count\s*\(\s*\*\s*\)\s+from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$",
        normalized,
    )
    if count_match:
        table = count_match.group(1)
        return (
            "SELECT COUNT(*) / 0.1 AS approx_value FROM "
            f"(SELECT * FROM {table} {_sample_clause(source)}) t"
        )

    sum_match = re.match(
        r"(?is)^select\s+sum\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)\s+from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$",
        normalized,
    )
    if sum_match:
        col = sum_match.group(1)
        table = sum_match.group(2)
        return (
            f"SELECT SUM({col}) / 0.1 AS approx_value FROM "
            f"(SELECT * FROM {table} {_sample_clause(source)}) t"
        )

    avg_match = re.match(
        r"(?is)^select\s+avg\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)\s+from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$",
        normalized,
    )
    if avg_match:
        col = avg_match.group(1)
        table = avg_match.group(2)
        return (
            f"SELECT AVG({col}) AS approx_value FROM "
            f"(SELECT * FROM {table} {_sample_clause(source)}) t"
        )

    raise ValueError("Approx mode currently supports simple COUNT/SUM/AVG on one table")


def run_approx(query: str, source: str = "duckdb") -> dict[str, Any]:
    source_key = source.lower().strip()
    rewritten = _rewrite_agg_query(query, source_key)
    exact_payload = run_exact(rewritten, source_key)

    value = None
    if exact_payload["result"]:
        value = exact_payload["result"][0][0]

    return {
        "result": value,
        "rows": exact_payload["result"],
        "columns": exact_payload.get("columns", []),
        "time": exact_payload["time"],
        "approx": True,
        "sample_rate": SAMPLE_RATE,
        "rewritten_query": rewritten,
        "source": source_key,
    }
