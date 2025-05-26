#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gamail.com>
Date   : 14-feb-2025
Purpose: Read, process and write SIIF's rf602 (Prespuesto de Gastos por Fuente) report
"""

__all__ = ["Rf602"]

import argparse
import asyncio
import datetime as dt
import inspect
import os

import numpy as np
import pandas as pd
from playwright.async_api import Download, async_playwright

from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rf602",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

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
        "--ejercicios",
        metavar="ejercicios",
        default=[dt.datetime.now().year],
        type=int,
        choices=range(2010, dt.datetime.now().year + 1),
        nargs="+",
        help="Ejercicios to download from SIIF",
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
        from ...config import settings

        args.username = settings.SIIF_USERNAME
        args.password = settings.SIIF_PASSWORD
        if args.username is None or args.password is None:
            parser.error("Both --username and --password are required.")

    if args.file and args.download:
        parser.error("You cannot use --file and --download together. Choose one.")

    return args


# --------------------------------------------------
class Rf602(SIIFReportManager):
    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.select_report_module(module=ReportCategory.Gastos)
        await self.select_specific_report_by_id(report_id="38")

    # --------------------------------------------------
    async def download_report(
        self, ejercicio: str = str(dt.datetime.now().year)
    ) -> Download:
        try:
            self.download = None
            # Getting DOM elements
            input_ejercicio = self.siif.reports_page.locator(
                "//input[@id='pt1:txtAnioEjercicio::content']"
            )
            btn_get_reporte = self.siif.reports_page.locator(
                "//div[@id='pt1:btnVerReporte']"
            )
            btn_xls = self.siif.reports_page.locator(
                "//input[@id='pt1:rbtnXLS::content']"
            )
            await btn_xls.click()

            await input_ejercicio.clear()
            await input_ejercicio.fill(str(ejercicio))

            async with self.siif.context.expect_page() as popup_info:
                async with self.siif.reports_page.expect_download() as download_info:
                    await btn_get_reporte.click()  # Se abre el popup aquí

            popup_page = await popup_info.value  # Obtener la ventana emergente
            self.download = await download_info.value  # Obtener el archivo descargado

            # Cerrar la ventana emergente (si realmente se abrió)
            if popup_page:
                await popup_page.close()

            return self.download

        except Exception as e:
            print(f"Error al descargar el reporte: {e}")
            # await self.logout()

    # --------------------------------------------------
    async def process_dataframe(self, dataframe: pd.DataFrame = None) -> pd.DataFrame:
        """ "Transform read xls file"""
        if dataframe is None:
            df = self.df.copy()
        else:
            df = dataframe.copy()
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
        df[to_numeric_cols] = (
            df[to_numeric_cols].apply(pd.to_numeric).astype(np.float64)
        )

        self.clean_df = df
        return self.clean_df


# --------------------------------------------------
async def main():
    """Make a jazz noise here"""

    args = get_args()

    save_path = os.path.dirname(
        os.path.abspath(inspect.getfile(inspect.currentframe()))
    )

    async with async_playwright() as p:
        connect_siif = await login(
            args.username, args.password, playwright=p, headless=False
        )
        try:
            rf602 = Rf602(siif=connect_siif)
            await rf602.go_to_reports()
            await rf602.go_to_specific_report()
            for ejercicio in args.ejercicios:
                await rf602.download_report(ejercicio=str(ejercicio))
                await rf602.save_xls_file(
                    save_path=save_path,
                    file_name=str(ejercicio) + "-rf602.xls",
                )
                await rf602.read_xls_file()
                print(rf602.df)
                await rf602.process_dataframe()
                print(rf602.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")
        finally:
            await rf602.logout()


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy
    # .venv/Scripts/python src/siif/services/rf602.py

    # python -m src.siif.handlers.rf602
