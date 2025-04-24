from fastapi import APIRouter, HTTPException, Form, status, UploadFile, File
import threading
from utils.threading_functions import run_migration, run_insert_data

router = APIRouter()


@router.post("/insert_data")
async def insert_data(
        db_name: str = Form(...),
        file: UploadFile = File(...)
):
    if not db_name.strip() or not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please provide a valid database name in the 'db_name' field and upload a valid file in the 'file' field"
        )

    file_content = await file.read()

    thread = threading.Thread(
        target=run_insert_data,
        args=(db_name, file_content, file.filename)
    )
    thread.start()

    return {
        "message": "Data insertion started in background."
    }


@router.post("/migrate_data")
async def migrate_data(source_db: str = Form(...), target_db: str = Form(...)):
    for name, value in {"source_db": source_db, "target_db": target_db}.items():
        if not value.strip():
            raise HTTPException(
                status_code=400,
                detail=f"{name.replace('_', ' ').title()} is required"
            )

    thread = threading.Thread(target=run_migration, args=(source_db, target_db))
    thread.start()

    return {
        "message": f"Migration from '{source_db}' to '{target_db}' has started in the background."
    }
