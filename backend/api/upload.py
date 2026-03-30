import shutil
import uuid
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, UploadFile

from backend.db.duckdb import create_table_from_csv

router = APIRouter()

_BASE_DIR = Path(__file__).resolve().parents[2]
_UPLOAD_DIR = _BASE_DIR / "datasets"
_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@router.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    file_id = uuid.uuid4().hex
    temp_path = _UPLOAD_DIR / f"{file_id}.csv"

    with temp_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    table_name = create_table_from_csv(str(temp_path))
    return {"table_name": table_name, "path": str(temp_path)}
