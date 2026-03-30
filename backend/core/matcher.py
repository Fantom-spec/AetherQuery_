import hashlib
import json
from typing import Any

_DENY_KEYS = {
    "Actual Startup Time",
    "Actual Total Time",
    "Actual Rows",
    "Actual Loops",
    "Execution Time",
    "Planning Time",
    "Startup Cost",
    "Total Cost",
    "Plan Rows",
    "Plan Width",
    "Shared Hit Blocks",
    "Shared Read Blocks",
    "Shared Dirtied Blocks",
    "Shared Written Blocks",
    "Local Hit Blocks",
    "Local Read Blocks",
    "Local Dirtied Blocks",
    "Local Written Blocks",
    "Temp Read Blocks",
    "Temp Written Blocks",
    "Workers",
    "Workers Planned",
    "Workers Launched",
    "Peak Memory Usage",
    "Disk Usage",
    "HashAgg Batches",
    "Batches",
    "Async Capable",
    "Parent Relationship",
    "Rows Removed by Filter",
    "WAL Records",
    "WAL FPI",
    "WAL Bytes",
}


def _normalize_plan(plan_json: Any) -> Any:
    if isinstance(plan_json, list) and len(plan_json) == 1:
        plan_json = plan_json[0]

    if isinstance(plan_json, dict) and "Plan" in plan_json:
        plan_json = plan_json["Plan"]

    if isinstance(plan_json, dict):
        normalized = {}
        for key, value in plan_json.items():
            if key in _DENY_KEYS:
                continue
            if key == "Plans" and isinstance(value, list):
                normalized["Plans"] = [_normalize_plan(child) for child in value]
            else:
                normalized[key] = _normalize_plan(value)
        return normalized

    if isinstance(plan_json, list):
        return [_normalize_plan(item) for item in plan_json]

    return plan_json


def _fingerprint(normalized_plan: Any) -> str:
    serialized = json.dumps(normalized_plan, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def match_plans(plan1: Any, plan2: Any) -> float:
    norm1 = _normalize_plan(plan1)
    norm2 = _normalize_plan(plan2)

    fp1 = _fingerprint(norm1)
    fp2 = _fingerprint(norm2)
    if fp1 == fp2:
        return 1.0

    set1 = set(json.dumps(norm1, sort_keys=True).split(','))
    set2 = set(json.dumps(norm2, sort_keys=True).split(','))
    if not set1 and not set2:
        return 1.0
    union = len(set1 | set2)
    if union == 0:
        return 0.0
    return round(len(set1 & set2) / union, 4)
