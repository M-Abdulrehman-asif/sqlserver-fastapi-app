import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv


load_dotenv()
# Get database connection details from environment variables (you can set these in your system or use a .env file)
DB_HOST = os.getenv("DB_HOST", "localhost")  # Default to 'localhost' if not set
DB_USER = os.getenv("DB_USER", "DESKTOP-RUAG96D\\ORACLE")  # Default to your username if not set
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC+Driver+17+for+SQL+Server")  # Default to your driver
DB_CONNECTION_TYPE = os.getenv("DB_CONNECTION_TYPE", "mssql+pyodbc")

def get_database_url(db_name: str) -> str:
    """
    Constructs the database URL dynamically based on the provided database name.
    """
    return f"{DB_CONNECTION_TYPE}://{DB_USER}@{DB_HOST}/{db_name}?driver={DB_DRIVER}&trusted_connection=yes"

# Use the function to get the URL dynamically
SOURCE_DB = get_database_url("test_db")
TARGET_DB = get_database_url("new_db")

# Create engine and session for source and target databases
source_engine = create_engine(SOURCE_DB)
target_engine = create_engine(TARGET_DB)

SessionLocal = sessionmaker(bind=source_engine, autocommit=False, autoflush=False)
Base = declarative_base()

SourceSession = sessionmaker(bind=source_engine)
TargetSession = sessionmaker(bind=target_engine)

def db_connect():
    """Create a new database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def db_create():
    """Create the necessary tables in the database."""
    try:
        Base.metadata.create_all(bind=source_engine)  # Use source engine for creating tables
        print("Tables created successfully.")
    except SQLAlchemyError as e:
        print(f"Error creating tables: {e}")
        raise

def db_init():
    """Initialize the database."""
    try:
        db_create()
        print("Database initialized.")
    except Exception as e:
        print(f"DB Init failed: {e}")
