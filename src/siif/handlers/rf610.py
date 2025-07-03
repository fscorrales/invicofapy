#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 01-jul-2025
Purpose: Read, process and write SIIF's rf610 () report
"""

__all__ = ["Rf610"]

import argparse
import asyncio
import datetime as dt
import inspect
import os

import numpy as np
import pandas as pd
from playwright.async_api import Download, async_playwright

from ...utils.validate import validate_excel_file
from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rf610",
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
        "-d",
        "--download",
        help="Download report from SIIF",
        action="store_true",
        default=False,
    )

    parser.add_argument(
        "--headless", help="Run browser in headless mode", action="store_true"
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="xls_file",
        default=None,
        type=str,
        help="SIIF' rf610.xls report. Must be in the same folder",
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
class Rf610(SIIFReportManager):
    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.select_report_module(module=ReportCategory.Gastos)
        await self.select_specific_report_by_id(report_id="7")

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

        df = self.df.replace(to_replace="", value=None)
        df["ejercicio"] = pd.to_numeric(df.iloc[9, 33][-4:], errors="coerce")
        df = df.rename(
            columns={
                "5": "programa",
                "7": "subprograma",
                "8": "proyecto",
                "11": "actividad",
                "13": "grupo",
                "16": "partida",
                "19": "desc_partida",
                "37": "credito_original",
                "43": "credito_vigente",
                "48": "comprometido",
                "54": "ordenado",
                "59": "saldo",
            }
        )
        df = df.tail(-30)
        df = df.loc[
            :,
            [
                "ejercicio",
                "programa",
                "subprograma",
                "proyecto",
                "actividad",
                "grupo",
                "partida",
                "desc_partida",
                "credito_original",
                "credito_vigente",
                "comprometido",
                "ordenado",
                "saldo",
            ],
        ]
        df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
        df["programa"] = df["programa"].ffill()
        df["subprograma"] = df["subprograma"].ffill()
        df["proyecto"] = df["proyecto"].ffill()
        df["actividad"] = df["actividad"].ffill()
        df["grupo"] = df["grupo"].ffill()
        df["partida"] = df["partida"].ffill()
        df["desc_partida"] = df["desc_partida"].ffill()
        df = df.dropna(subset=["credito_original"])
        df[["programa", "desc_programa"]] = df["programa"].str.split(n=1, expand=True)
        df[["subprograma", "desc_subprograma"]] = df["subprograma"].str.split(
            n=1, expand=True
        )
        df[["proyecto", "desc_proyecto"]] = df["proyecto"].str.split(n=1, expand=True)
        df[["actividad", "desc_actividad"]] = df["actividad"].str.split(
            n=1, expand=True
        )
        print("Separamos los grupos")
        df[["grupo", "desc_grupo"]] = df["grupo"].str.split(n=1, expand=True)
        df["programa"] = df["programa"].str.zfill(2)
        df["subprograma"] = df["subprograma"].str.zfill(2)
        df["proyecto"] = df["proyecto"].str.zfill(2)
        df["actividad"] = df["actividad"].str.zfill(2)
        df["desc_programa"] = df["desc_programa"].str.strip()
        df["desc_subprograma"] = df["desc_subprograma"].str.strip()
        df["desc_proyecto"] = df["desc_proyecto"].str.strip()
        df["desc_actividad"] = df["desc_actividad"].str.strip()
        df["desc_grupo"] = df["desc_grupo"].str.strip()
        df["desc_partida"] = df["desc_partida"].str.strip()
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
        to_numeric_cols = [
            "credito_original",
            "credito_vigente",
            "comprometido",
            "ordenado",
            "saldo",
        ]
        df[to_numeric_cols] = (
            df[to_numeric_cols].apply(pd.to_numeric).astype(np.float64)
        )

        first_cols = [
            "ejercicio",
            "estructura",
            "programa",
            "desc_programa",
            "subprograma",
            "desc_subprograma",
            "proyecto",
            "desc_proyecto",
            "actividad",
            "desc_actividad",
            "grupo",
            "desc_grupo",
            "partida",
            "desc_partida",
        ]
        df = df.loc[:, first_cols].join(df.drop(first_cols, axis=1))

        self.clean_df = df
        return self.clean_df


# --------------------------------------------------
async def main():
    """Make a jazz noise here"""

    args = get_args()

    save_path = os.path.dirname(
        os.path.abspath(inspect.getfile(inspect.currentframe()))
    )

    if args.file is not None:
        args.file = validate_excel_file(os.path.join(save_path, args.file))

    async with async_playwright() as p:
        try:
            if args.download:
                connect_siif = await login(
                    args.username, args.password, playwright=p, headless=False
                )
                rf610 = Rf610(siif=connect_siif)
                await rf610.go_to_reports()
                await rf610.go_to_specific_report()
                for ejercicio in args.ejercicios:
                    await rf610.download_report(ejercicio=str(ejercicio))
                    await rf610.save_xls_file(
                        save_path=save_path,
                        file_name=str(ejercicio) + "-rf610.xls",
                    )
                    await rf610.read_xls_file(args.file)
                    print(rf610.df)
                    await rf610.process_dataframe()
                    print(rf610.clean_df)
                await rf610.logout()
            else:
                rf610 = Rf610()
                await rf610.read_xls_file(args.file)
                print(rf610.df)
                await rf610.process_dataframe()
                print(rf610.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.rf610 -d
    # poetry run python -m src.siif.handlers.rf610 -f 2025-rf610.xls
