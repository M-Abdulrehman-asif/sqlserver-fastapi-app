import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")

Base = declarative_base()


class TargetDatabaseHandler:
    def __init__(self, db_name):
        self.db_name = db_name
        self.admin_db = os.getenv("MSSQL_ADMIN_DB")
        self.engine = None
        self.session = None
        self.master_url = (
            f"mssql+pyodbc://{DB_USER}@{DB_HOST}/{self.admin_db}"
            f"?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        )

    def create_db(self):
        master_engine = create_engine(self.master_url)

        with master_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            exists = conn.execute(
                text("SELECT 1 FROM sys.databases WHERE name = :db_name"),
                {"db_name": self.db_name}
            ).scalar()

            if not exists:
                conn.execute(text(f"CREATE DATABASE [{self.db_name}]"))
                print(f"Database {self.db_name} created.")
            else:
                print(f"Database {self.db_name} already exists.")

    def connect(self):
        db_url = (
            f"mssql+pyodbc://{DB_USER}@{DB_HOST}/{self.db_name}"
            f"?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        )
        self.engine = create_engine(db_url)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def init_db(self, source_metadata):
        self.connect()
        if not self.engine:
            raise Exception("Engine not created. Call connect() first.")
        source_metadata.create_all(bind=self.engine)
        print("Tables created in target database from source metadata.")

    def disconnect(self):
        if self.session:
            self.session.close()
        if self.engine:
            self.engine.dispose()
