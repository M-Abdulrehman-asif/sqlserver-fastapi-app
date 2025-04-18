from fastapi import APIRouter, HTTPException, Form
from database.source_db import DatabaseHandler
from utils.read_file import read_file
from fastapi import UploadFile, File
from utils.insert_data import insert_data_in_table


router = APIRouter()


@router.post("/insert_data")
async def insert_data(db_name: str = Form(...), file: UploadFile = File(...)):
    db_handler = DatabaseHandler(db_name)
    try:
        df = await read_file(file)
        db_handler.create_db()       # Step 1: Create DB
        db_handler.connect()         # Step 2: Connect (creates engine + session)
        db_handler.init_db()         # Step 3: Create tables

        rows = insert_data_in_table(df, db_handler)  # Step 4: Insert data

        return {"message": f"{rows} rows inserted successfully!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

    finally:
        db_handler.disconnect()

