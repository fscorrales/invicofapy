#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Working With Files in Python
Source: https://realpython.com/working-with-files-in-python/#:~:text=To%20get%20a%20list%20of,scandir()%20in%20Python%203.
"""

__all__ = [
    "read_csv",
    "read_xls",
    "get_list_of_files",
    "get_df_from_sql_table",
    "export_dataframe_as_excel_response",
    "export_multiple_dataframes_to_excel",
    "upload_multiple_dataframes_to_google_sheets",
    "GoogleExportResponse",
]


import os
import sqlite3
from io import BytesIO
from typing import List, Optional, Tuple

import pandas as pd
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ..config import logger
from .google_sheets import GoogleSheets
from .safe_get import (
    sanitize_dataframe_for_json,
    sanitize_dataframe_for_json_with_datetime,
)


# --------------------------------------------------
def read_csv(PATH: str, names=None, header=None) -> pd.DataFrame:
    """ "Read from csv report"""
    df = pd.read_csv(
        PATH,
        index_col=None,
        header=header,
        na_filter=False,
        dtype=str,
        encoding="ISO-8859-1",
        on_bad_lines="warn",
        names=names,
    )
    df.columns = [str(x) for x in range(df.shape[1])]
    return df


# --------------------------------------------------
def read_xls(PATH: str, header: int = None) -> pd.DataFrame:
    """ "Read from xls report"""
    df = pd.read_excel(PATH, index_col=None, header=header, na_filter=False, dtype=str)
    if header is None:
        df.columns = [str(x) for x in range(df.shape[1])]
    return df


# --------------------------------------------------
# def read_pdf(self, PATH:str, names=None, header=None) -> pd.DataFrame:
#     """"Read from pdf report"""
#     tables = tabula.read_pdf(
#         PATH, pages='all', multiple_tables=False,
#         pandas_options={
#             'index_col':None, 'header':header,'na_filter':False,
#             'dtype':str, 'on_bad_lines':'warn', 'names':names
#         }
#     )
#     df = pd.DataFrame()
#     # n_col = df.shape[1]
#     # df.columns = [str(x) for x in range(n_col)]
#     # return df
#     for i in range(len(tables)):
#         table = tables[i]
#         n_col = table.shape[1]
#         table.columns = [str(x) for x in range(n_col)]
#         df=pd.concat([df, table],)
#     return df


# --------------------------------------------------
def get_list_of_files(path: str, years: list[str] = None) -> list:
    """Get list of files in a folder
    :param path: folder or file.
    :param years: list of years to filter the files.
    """
    # Check if path is a file and returns it.
    # Otherwise returns list of files in folder.
    file_list = []
    if os.path.isfile(path):
        file_list.append(path)
    else:
        for entry in os.listdir(path):
            full_path = os.path.join(path, entry)
            if os.path.isfile(full_path) and (
                years is None or os.path.basename(full_path)[:4] in years
            ):
                file_list.append(full_path)
    print("File list to update:")
    print(file_list)
    return file_list


# --------------------------------------------------
def get_df_from_sql_table(sqlite_path: str, table: str) -> pd.DataFrame:
    with sqlite3.connect(sqlite_path) as conn:
        return pd.read_sql_query(f"SELECT * FROM {table}", conn)


# --------------------------------------------------
def export_dataframe_as_excel_response(
    df: pd.DataFrame,
    filename: str = "data.xlsx",
    sheet_name: str = "Hoja1",
    upload_to_google_sheets: bool = False,
    google_sheet_key: str = None,
) -> StreamingResponse:
    try:
        # 1️⃣ Sanitizar
        df = sanitize_dataframe_for_json(df)
        df = df.drop(columns=["_id"], errors="ignore")

        # 2️⃣ Upload a Google Sheets
        if upload_to_google_sheets and google_sheet_key:
            gs_service = GoogleSheets()
            gs_service.to_google_sheets(
                df=df,
                spreadsheet_key=google_sheet_key,
                wks_name=sheet_name,
            )

        # 3️⃣ Exportar a buffer Excel
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
        buffer.seek(0)

        # 4️⃣ Enviar como respuesta HTTP
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except Exception as e:
        logger.error(f"Error exporting DataFrame as Excel: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error exporting data to Excel",
        )


# --------------------------------------------------
def export_multiple_dataframes_to_excel(
    df_sheet_pairs: List[Tuple[pd.DataFrame, str]],
    filename: str = "data.xlsx",
    spreadsheet_key: Optional[str] = None,
    upload_to_google_sheets: bool = False,
) -> StreamingResponse:
    """
    Exporta múltiples DataFrames a distintas hojas de un archivo Excel (y opcionalmente a Google Sheets).
    Args:
        df_sheet_pairs: Lista de tuplas (DataFrame, nombre_de_hoja).
        filename: Nombre del archivo Excel de salida.
        spreadsheet_key: Clave del Google Sheets (si aplica).
        upload_to_google_sheets: Subir también a Google Sheets.
    Returns:
        StreamingResponse con el archivo Excel.
    """
    try:
        # 1️⃣ Sanitizar y preparar DataFrames
        sanitized_pairs = []
        for df, sheet_name in df_sheet_pairs:
            if not df.empty:
                df = sanitize_dataframe_for_json_with_datetime(df)
                df = df.drop(columns=["_id"], errors="ignore")
            sanitized_pairs.append((df, sheet_name))

        # 2️⃣ Subir a Google Sheets
        if upload_to_google_sheets and spreadsheet_key:
            gs = GoogleSheets()
            for df, sheet_name in sanitized_pairs:
                # if not df.empty:
                gs.to_google_sheets(
                    df=df,
                    spreadsheet_key=spreadsheet_key,
                    wks_name=sheet_name,
                )

        # 3️⃣ Escribir a Excel
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            for df, sheet_name in sanitized_pairs:
                if not df.empty:
                    df.to_excel(writer, index=False, sheet_name=sheet_name)
        buffer.seek(0)

        # 4️⃣ Retornar como respuesta
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except Exception as e:
        logger.error(f"Error al exportar múltiples DataFrames: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error al exportar múltiples DataFrames",
        )


# --------------------------------------------------
class GoogleExportResponse(BaseModel):
    status: str = None
    sheets_uploaded: list[str] = []
    rows: dict[str, int] = {}


# --------------------------------------------------
def upload_multiple_dataframes_to_google_sheets(
    df_sheet_pairs: List[Tuple[pd.DataFrame, str]],
    spreadsheet_key: str,
) -> GoogleExportResponse:
    """
    Sube múltiples DataFrames a distintas hojas de un Google Sheets.

    Args:
        df_sheet_pairs: Lista de tuplas (DataFrame, nombre_de_hoja).
        spreadsheet_key: Clave del Google Sheets destino.

    Raises:
        HTTPException: Si ocurre un error durante la carga.
    """
    schema = GoogleExportResponse()
    try:
        # 1️⃣ Sanitizar y preparar DataFrames
        sanitized_pairs = []
        for df, sheet_name in df_sheet_pairs:
            if not df.empty:
                df = sanitize_dataframe_for_json_with_datetime(df)
                df = df.drop(columns=["_id"], errors="ignore")
            sanitized_pairs.append((df, sheet_name))

        # 2️⃣ Subir a Google Sheets
        gs = GoogleSheets()
        for df, sheet_name in sanitized_pairs:
            gs.to_google_sheets(
                df=df,
                spreadsheet_key=spreadsheet_key,
                wks_name=sheet_name,
            )

        schema.status = "success"
        schema.sheets_uploaded = [name for _, name in df_sheet_pairs]
        schema.rows = {name: len(df) for df, name in df_sheet_pairs}

        return schema

    except Exception as e:
        logger.error(f"Error al subir DataFrames a Google Sheets: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error al subir DataFrames a Google Sheets",
        )
