from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import User, Post, Comment, Product
from db_manager import Base, SourceSession, TargetSession

router = APIRouter()


class DatabaseNames(BaseModel):
    """Pydantic model to accept only database names."""
    source_db: str
    target_db: str


def get_db_url(db_name: str) -> str:
    """Generate the full database URL from the database name."""
    return f"mssql+pyodbc://DESKTOP-RUAG96D\\ORACLE@localhost/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"


def create_database_if_not_exists(db_name: str):
    """Create the database if it does not already exist."""
    master_engine = create_engine(
        "mssql+pyodbc://DESKTOP-RUAG96D\\ORACLE@localhost/master?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    )

    with master_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        query = text(f"SELECT name FROM sys.databases WHERE name = :db_name")
        result = conn.execute(query, {"db_name": db_name}).fetchone()

        if not result:
            conn.execute(text(f"CREATE DATABASE {db_name}"))
            print(f"Database '{db_name}' created.")


@router.post("/migrate-all")
def migrate_all_data(db_names: DatabaseNames):
    """Migrate data from the source database to the target database."""
    # Construct the full database URLs
    source_db_url = get_db_url(db_names.source_db)
    target_db_url = get_db_url(db_names.target_db)

    # Create the target database if it doesn't exist
    create_database_if_not_exists(db_names.target_db)

    # Create the engine and session for source and target databases
    source_engine = create_engine(source_db_url)
    target_engine = create_engine(target_db_url)

    SourceSession = sessionmaker(bind=source_engine)
    TargetSession = sessionmaker(bind=target_engine)

    source_db = SourceSession()
    target_db = TargetSession()

    try:
        # Migrate data from source to target
        for model in [User, Post, Comment, Product]:
            records = source_db.query(model).all()
            print(f"Found {len(records)} records in {model.__name__} to migrate.")
            for item in records:
                data = {column.name: getattr(item, column.name) for column in model.__table__.columns}
                new_item = model(**data)
                target_db.add(new_item)
            print(f"Migrating {model.__name__} - Total records: {len(records)}")

        target_db.commit()
        return {"status": "success", "message": "All data migrated!"}
    except Exception as e:
        print(f"Error during migration: {e}")  # Detailed error
        target_db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        source_db.close()
        target_db.close()
