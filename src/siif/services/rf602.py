#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gamail.com>
Date   : 14-feb-2025
Purpose: Read, process and write SIIF's rf602 (Prespuesto de Gastos por Fuente) report
"""

__all__ = []

import argparse
import asyncio
import datetime as dt
import inspect
import io
import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from playwright._impl._browser import Browser, BrowserContext, Page
from playwright.async_api import async_playwright

from .connect import (
    ConnectSIIF,
    ReportCategory,
    go_to_reports,
    login,
    logout,
    select_report_module,
    select_specific_report_by_id,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rf602",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # parser.add_argument("username", metavar="username", help="Username for SIIF access")

    # parser.add_argument("password", metavar="password", help="Password for SIIF access")

    parser.add_argument(
        "-u",
        "--username",
        help="Username for SIIF access",
        metavar="username",
        type=str,
        default=None,
    )

    parser.add_argument(
        "-p",
        "--password",
        help="Password for SIIF access",
        metavar="password",
        type=str,
        default=None,
    )

    parser.add_argument(
        "-e",
        "--ejercicio",
        metavar="ejercicio",
        default=2025,
        type=int,
        help="Ejercicio to download from SIIF",
    )

    parser.add_argument(
        "-d", "--download", help="Download report from SIIF", action="store_true"
    )

    parser.add_argument(
        "--headless", help="Run browser in headless mode", action="store_true"
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="xls_file",
        default=None,
        type=argparse.FileType("r"),
        help="SIIF' rf602.xls report. Must be in the same folder",
    )

    args = parser.parse_args()

    if args.username is None or args.password is None:
        # Load environment variables
        load_dotenv()
        args.username = os.getenv("SIIF_USERNAME")
        args.password = os.getenv("SIIF_PASSWORD")
        if args.username is None or args.password is None:
            parser.error("Both --username and --password are required.")

    actual_year = dt.datetime.now().year
    if args.ejercicio < 2010 or args.ejercicio > actual_year:
        parser.error(
            f"--ejercicio '{args.ejercicio}' must be between 2010 and {actual_year}"
        )

    if args.file and args.download:
        parser.error("You cannot use --file and --download together. Choose one.")

    return args


# --------------------------------------------------
async def download(
    connect: ConnectSIIF, dir_path: str, ejercicios: list = str(dt.datetime.now().year)
) -> None:
    try:
        # Getting DOM elements
        input_ejercicio = connect.reports_page.locator(
            "//input[@id='pt1:txtAnioEjercicio::content']"
        )
        btn_get_reporte = connect.reports_page.locator("//div[@id='pt1:btnVerReporte']")
        btn_xls = connect.reports_page.locator("//input[@id='pt1:rbtnXLS::content']")
        await btn_xls.click()

        # Form submit
        if not isinstance(ejercicios, list):
            ejercicios = [ejercicios]
        for ejercicio in ejercicios:
            await input_ejercicio.clear()
            await input_ejercicio.fill(ejercicio)
            async with connect.reports_page.expect_download() as download_info:
                # Perform the action that initiates download
                await btn_get_reporte.click()
            download = await download_info.value
            # # Wait for the download process to complete and save the downloaded file somewhere
            # file_path = os.path.join(dir_path, ejercicio + "-rf602.xls")
            # print(f"Descargando el reporte en: {file_path}")
            # await download.save_as(file_path)

            file_bytes = await download.read()  # Leer el archivo en memoria
            # Convertir los bytes a un DataFrame de pandas
            df = pd.read_excel(io.BytesIO(file_bytes))
            # Procesar el DataFrame (limpieza)
            df_clean = process_dataframe(df)
            print(df_clean)

    except Exception as e:
        print(f"Error al descargar el reporte: {e}")
        await logout(connect)


# --------------------------------------------------
def process_dataframe(self) -> pd.DataFrame:
    """ "Transform read xls file"""
    df = self.df
    df["ejercicio"] = df.iloc[5, 2][-4:]
    df = df.tail(-16)
    df = df.loc[
        :,
        [
            "ejercicio",
            "2",
            "3",
            "6",
            "7",
            "8",
            "9",
            "10",
            "13",
            "14",
            "15",
            "16",
            "18",
            "20",
        ],
    ]
    df = df.replace(to_replace="", value=None)
    df = df.dropna(subset=["2"])
    df = df.rename(
        columns={
            "2": "programa",
            "3": "subprograma",
            "6": "proyecto",
            "7": "actividad",
            "8": "partida",
            "9": "fuente",
            "10": "org",
            "13": "credito_original",
            "14": "credito_vigente",
            "15": "comprometido",
            "16": "ordenado",
            "18": "saldo",
            "20": "pendiente",
        }
    )
    df["programa"] = df["programa"].str.zfill(2)
    df["subprograma"] = df["subprograma"].str.zfill(2)
    df["proyecto"] = df["proyecto"].str.zfill(2)
    df["actividad"] = df["actividad"].str.zfill(2)
    df["grupo"] = df["partida"].str[0] + "00"
    df["estructura"] = (
        df["programa"]
        + "-"
        + df["subprograma"]
        + "-"
        + df["proyecto"]
        + "-"
        + df["actividad"]
        + "-"
        + df["partida"]
    )
    df = df.loc[
        :,
        [
            "ejercicio",
            "estructura",
            "fuente",
            "programa",
            "subprograma",
            "proyecto",
            "actividad",
            "grupo",
            "partida",
            "org",
            "credito_original",
            "credito_vigente",
            "comprometido",
            "ordenado",
            "saldo",
            "pendiente",
        ],
    ]
    to_numeric_cols = [
        "credito_original",
        "credito_vigente",
        "comprometido",
        "ordenado",
        "saldo",
        "pendiente",
    ]
    df[to_numeric_cols] = df[to_numeric_cols].apply(pd.to_numeric).astype(np.float64)

    self.df = df
    return self.df


# --------------------------------------------------
async def main():
    """Make a jazz noise here"""

    args = get_args()

    dir_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

    async with async_playwright() as p:
        connect_siif = await login(
            args.username, args.password, playwright=p, headless=False
        )
        await go_to_reports(connect=connect_siif)
        await select_report_module(connect=connect_siif, module=ReportCategory.Gastos)
        await select_specific_report_by_id(connect=connect_siif, report_id="38")
        await download(
            connect=connect_siif, dir_path=dir_path, ejercicios=str(args.ejercicio)
        )
        await logout(connect=connect_siif)


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy
    # .venv/Scripts/python src/siif/services/rf602.py

    # python -m src.siif.services.rf602
