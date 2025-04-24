from database.dest_db import TargetDatabaseHandler
from utils.handle_functions import migrate_known_tables, reflect_metadata
from database.source_db import DatabaseHandler
from utils.read_file import read_file_sync
from utils.insert_data import insert_data_in_table

def run_insert_data(db_name: str, file_content: bytes, filename: str):
    db_handler = DatabaseHandler(db_name)
    try:
        db_handler.create_db()
        db_handler.connect_db()
        db_handler.init_db()

        sheets_dict = read_file_sync(file_content, filename)
        insert_data_in_table(sheets_dict, db_handler)

        print("Data inserted successfully.")

    except Exception as e:
        print(f"Data processing failed: {str(e)}")
    finally:
        db_handler.disconnect_db()


def run_migration(source_db: str, target_db: str):
    source_handler = DatabaseHandler(source_db)
    target_handler = TargetDatabaseHandler(target_db)

    try:
        source_handler.connect_db()
        source_metadata = reflect_metadata(source_handler)

        target_handler.create_db()
        target_handler.init_db(source_metadata)

        inserted_counts = migrate_known_tables(
            source_handler.session,
            target_handler.session,
            source_metadata
        )

        print(f"Migration completed from '{source_db}' to '{target_db}'")
        print(f"Tables migrated: {list(source_metadata.tables.keys())}")
        print(f"Rows inserted: {inserted_counts}")

    except Exception as e:
        print(f"Migration failed: {str(e)}")
    finally:
        source_handler.disconnect_db()
        target_handler.disconnect()
