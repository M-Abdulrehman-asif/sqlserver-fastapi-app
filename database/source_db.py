import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base


load_dotenv()

Base = declarative_base()

class DatabaseHandler:
    def __init__(self, db_name: str):
        if not db_name:
            raise ValueError("Database name required")

        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.trusted_connection = os.getenv("TRUSTED_CONNECTION")
        self.admin_db = os.getenv("MSSQL_ADMIN_DB")
        self.db_name = db_name
        self.engine = None
        self.session_factory = None
        self.base_url = (
            f"mssql+pyodbc://{self.user}:{self.password}@{self.host}/{self.db_name}"
            f"?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection={self.trusted_connection}&encrypt=no"
        )

    def create_db(self):
        temp_url = self.base_url.replace(f"/{self.db_name}", f"/{self.admin_db}")
        engine = create_engine(
            temp_url,
            isolation_level="AUTOCOMMIT",
            connect_args={"timeout": 30}
        )
        with engine.connect() as conn:
            conn.execute(text(
                f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'{self.db_name}') "
                f"CREATE DATABASE [{self.db_name}]"
            ))

    def get_session(self):
        return self.session_factory()

    def connect_db(self):
        self.engine = create_engine(self.base_url)
        self.session_factory = sessionmaker(bind=self.engine)

    def init_db(self):
        Base.metadata.create_all(self.engine)

    def disconnect_db(self):
        self.engine = None
        self.session_factory = None

    @property
    def session(self):
        return self.session_factory()
