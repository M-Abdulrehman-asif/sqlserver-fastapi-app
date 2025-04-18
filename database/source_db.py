import os
from sqlalchemy.orm import declarative_base

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")

Base = declarative_base()


class DatabaseHandler:
    def __init__(self, db_name):
        self.db_name = db_name
        self.engine = None
        self.session = None

    def create_db(self):
        # Step 1: Connect to master DB with AUTOCOMMIT to create the database
        master_url = (
            f"mssql+pyodbc://{DB_USER}@{DB_HOST}/master"
            f"?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        )
        master_engine = create_engine(master_url)

        with master_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f"CREATE DATABASE [{self.db_name}]"))

    def init_db(self):
        if not self.engine:
            raise Exception("Engine not created. Call connect() first.")
        Base.metadata.create_all(self.engine)

    def connect(self):
        db_url = (
            f"mssql+pyodbc://{DB_USER}@{DB_HOST}/{self.db_name}"
            f"?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        )
        self.engine = create_engine(db_url)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def disconnect(self):
        if self.session:
            self.session.close()

