from typing import Any

from backend.core.approx_engine import run_approx
from backend.core.exact_engine import run_exact


def route_query(query: str, mode: str, source: str) -> dict[str, Any]:
    mode_key = mode.lower().strip()
    if mode_key == "approx":
        return run_approx(query, source)
    return run_exact(query, source)
