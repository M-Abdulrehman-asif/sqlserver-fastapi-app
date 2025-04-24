from fastapi import APIRouter, HTTPException, Form, status, UploadFile, File
from database.source_db import DatabaseHandler
from utils.read_file import read_file
from utils.insert_data import insert_data_in_table
from database.dest_db import TargetDatabaseHandler
from utils.handle_functions import migrate_known_tables, handle_tables

router = APIRouter()


@router.post("/insert_data")
async def insert_data(
        db_name: str = Form(...),
        file: UploadFile = File(...)
):
    if not db_name or db_name.strip() == "":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail= "Please provide a valid database name in the 'db_name' field"
        )

    if not file or file.filename == "":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please upload a valid file in the 'file' field"
        )


    db_handler = DatabaseHandler(db_name)
    try:
        db_handler.create_db()
        db_handler.connect_db()
        db_handler.init_db()

        sheets_dict = await read_file(file)
        insert_data_in_table(sheets_dict, db_handler)

        return {
            "message": "Data inserted successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Data processing failed: {str(e)}"
        )
    finally:
        db_handler.disconnect_db()


@router.post("/migrate_data")
async def migrate_data(
        source_db: str = Form(...),
        target_db: str = Form(...),
):
    if not source_db.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Source database name is required"
        )

    if not target_db.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Target database name is required"
        )


    source_handler = DatabaseHandler(source_db)
    target_handler = TargetDatabaseHandler(target_db)

    try:
        source_handler.connect_db()
        target_handler.create_db()
        target_handler.connect()

        source_metadata = handle_tables(source_handler, target_handler)
        inserted_counts = migrate_known_tables(
            source_handler.session,
            target_handler.session,
            source_metadata
        )

        return {
            "status": "success",
            "message": "Migration completed",
            "stats": {
                "tables_created": list(source_metadata.tables.keys()),
                "rows_migrated": inserted_counts
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Migration failed: {str(e)}"
        )
    finally:
        source_handler.disconnect_db()
        target_handler.disconnect()
