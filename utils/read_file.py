from fastapi import UploadFile, HTTPException
import pandas as pd
from io import BytesIO


async def read_file(file: UploadFile):
    try:
        contents = await file.read()
        if not contents:
            raise HTTPException(
                status_code=400,
                detail="Uploaded file is empty."
            )

        xls = pd.ExcelFile(BytesIO(contents))
        all_sheets_df = {
            sheet_name: xls.parse(sheet_name)
            for sheet_name in xls.sheet_names
        }

        for sheet_name, df in all_sheets_df.items():
            if not isinstance(df, pd.DataFrame):
                raise HTTPException(
                    status_code=400,
                    detail=f"Sheet '{sheet_name}' could not be parsed as DataFrame"
                )

        return all_sheets_df

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid Excel file: {str(e)}"
        )


def read_file_sync(file_content: bytes):
    try:
        if not file_content:
            raise ValueError("Uploaded file is empty.")

        xls = pd.ExcelFile(BytesIO(file_content))
        all_sheets_df = {
            sheet_name: xls.parse(sheet_name)
            for sheet_name in xls.sheet_names
        }

        for sheet_name, df in all_sheets_df.items():
            if not isinstance(df, pd.DataFrame):
                raise ValueError(f"Sheet '{sheet_name}' could not be parsed as DataFrame")

        return all_sheets_df

    except Exception as e:
        raise ValueError(f"Invalid Excel file: {str(e)}")
