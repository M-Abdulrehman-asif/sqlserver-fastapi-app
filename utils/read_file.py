from fastapi import UploadFile
import pandas as pd
from io import BytesIO


# Helper function to read the Excel file
async def read_file(file: UploadFile):
    contents = await file.read()
    sheets_data = pd.read_excel(BytesIO(contents), sheet_name=None)  # Read all sheets
    return sheets_data