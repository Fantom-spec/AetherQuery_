from fastapi import APIRouter, HTTPException

from backend.core.approx_engine import _rewrite_agg_query
from backend.models.query import ExecuteRequest

router = APIRouter()


@router.post("/optimize")
async def optimize_query(req: ExecuteRequest):
    try:
        rewritten = _rewrite_agg_query(req.query, req.source.lower().strip())
        return {"success": True, "mode": "approx", "rewritten_query": rewritten}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
