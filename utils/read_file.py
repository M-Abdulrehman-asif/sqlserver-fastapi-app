import pandas as pd
from io import BytesIO

def read_file_sync(file_content: bytes, filename: str):
    print(f"Starting to read file: {filename}")

    try:
        if not file_content:
            print("File content is empty.")
            raise ValueError("Uploaded file is empty.")

        print("Loading Excel file into pandas...")
        xls = pd.ExcelFile(BytesIO(file_content))
        print(f"Excel file loaded successfully. Sheet names: {xls.sheet_names}")

        all_sheets_df = {}
        for sheet_name in xls.sheet_names:
            print(f"Parsing sheet: {sheet_name}")
            df = xls.parse(sheet_name)
            print(f"Sheet '{sheet_name}' parsed. Shape: {df.shape}")

            if not isinstance(df, pd.DataFrame):
                print(f"Sheet '{sheet_name}' is not a valid DataFrame.")
                raise ValueError(f"Sheet '{sheet_name}' could not be parsed as DataFrame")

            all_sheets_df[sheet_name.lower()] = df

        print("All sheets parsed and stored successfully.")
        return all_sheets_df

    except Exception as e:
        print(f"Error occurred while reading Excel file: {str(e)}")
        raise ValueError(f"Invalid Excel file: {str(e)}")
