from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import User, Post, Comment, Product
from db_manager import Base

router = APIRouter()


class DatabaseNames(BaseModel):
    source_db: str
    target_db: str


def get_db_url(db_name: str) -> str:
    return (
        f"mssql+pyodbc://DESKTOP-RUAG96D\\ORACLE@localhost/"
        f"{db_name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    )


def create_database_if_not_exists(db_name: str) -> None:
    engine = create_engine(get_db_url("master"))
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        db_exists = conn.execute(
            text("SELECT 1 FROM sys.databases WHERE name = :db_name"),
            {"db_name": db_name}
        ).scalar()
        if not db_exists:
            conn.execute(text(f"CREATE DATABASE [{db_name}]"))
            print(f"Database '{db_name}' created.")


def migrate_model(source_db, target_db, model) -> None:
    records = source_db.query(model).all()
    print(f"Found {len(records)} records in {model.__name__} to migrate.")

    for record in records:
        data = {
            column.name: getattr(record, column.name)
            for column in model.__table__.columns
        }
        target_db.add(model(**data))

    target_db.commit()
    print(f"Migrated {model.__name__} - Total records: {len(records)}")


@router.post("/migrate-all")
def migrate_all_data(db_names: DatabaseNames):
    source_url = get_db_url(db_names.source_db)
    target_url = get_db_url(db_names.target_db)

    create_database_if_not_exists(db_names.target_db)

    source_engine = create_engine(source_url)
    target_engine = create_engine(target_url)

    Base.metadata.create_all(bind=target_engine)

    source_session = sessionmaker(bind=source_engine)()
    target_session = sessionmaker(bind=target_engine)()

    try:
        for model in [User, Post, Comment, Product]:
            migrate_model(source_session, target_session, model)

        return {"status": "success", "message": "All data migrated!"}

    except Exception as e:
        print(f"Error during migration: {e}")
        target_session.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        source_session.close()
        target_session.close()
