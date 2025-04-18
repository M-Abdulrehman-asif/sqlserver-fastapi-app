from source_db import get_database_url
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


TARGET_DB = get_database_url("new_db")

target_engine = create_engine(TARGET_DB)

TargetSession = sessionmaker(bind=target_engine)