#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 18-jul-2025
Purpose: Read, process and write SIIF's ri102 () report
"""

__all__ = ["Ri102"]

import argparse
import asyncio
import datetime as dt
import inspect
import os

import pandas as pd
from playwright.async_api import Download, async_playwright

from ...config import logger
from ...utils import (
    RouteReturnSchema,
    get_df_from_sql_table,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
    validate_excel_file,
)
from ..repositories.ri102 import Ri102Repository
from ..schemas.ri102 import Ri102Report
from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's ri102",
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
        help="SIIF' ri102.xls report. Must be in the same folder",
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
class Ri102(SIIFReportManager):
    # --------------------------------------------------
    async def download_and_process_report(
        self, ejercicio: int = dt.datetime.now().year
    ) -> pd.DataFrame:
        """Download and process the ri102 report for a specific year."""
        try:
            await self.go_to_specific_report()
            self.download = await self.download_report(ejercicio=str(ejercicio))
            if self.download is None:
                raise ValueError("No se pudo descargar el reporte ri102.")
            await self.read_xls_file()
            return await self.process_dataframe()
        except Exception as e:
            print(f"Error al descargar y procesar el reporte: {e}")

    # --------------------------------------------------
    async def download_and_sync_validated_to_repository(
        self, ejercicio: int = dt.datetime.now().year
    ) -> RouteReturnSchema:
        """Download, process and sync the ri102 report to the repository."""
        try:
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=await self.download_and_process_report(ejercicio=ejercicio),
                model=Ri102Report,
                field_id="cod_recurso",
            )
            return await sync_validated_to_repository(
                repository=Ri102Repository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": ejercicio},
                title=f"SIIF RF610 Report del {ejercicio}",
                logger=logger,
                label=f"Ejercicio {ejercicio} del ri102",
            )
        except Exception as e:
            print(f"Error al descargar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def sync_validated_sqlite_to_repository(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        """Download, process and sync the ri102 report to the repository."""
        try:
            df = get_df_from_sql_table(sqlite_path, table="ppto_rec_ri102")
            df.drop(columns=["id"], inplace=True)
            df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
            df.rename(
                columns={
                    "cod_rec": "cod_recurso",
                    "desc_rec": "desc_recurso",
                },
                inplace=True,
            )

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=Ri102Report,
                field_id="estructura",
            )

            return await sync_validated_to_repository(
                repository=Ri102Repository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": {"$lt": 2024}},
                title="Sync SIIF Ri102 Report from SQLite",
                logger=logger,
                label="Sync SIIF Ri102 Report from SQLite",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.select_report_module(module=ReportCategory.Recursos)
        await self.select_specific_report_by_id(report_id="28")

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
                "//div[@id='pt1:btnEjecutarReporte']"
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

            await self.go_back_to_reports_list()

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

        df["ejercicio"] = pd.to_numeric(df.iloc[5, 17], errors="coerce")
        df["tipo"] = df["2"].str[0:2] + "000"
        df["clase"] = df["2"].str[0:3] + "00"
        df = df.replace(to_replace="", value=None)
        df = df.loc[
            :,
            [
                "ejercicio",
                "tipo",
                "clase",
                "2",
                "4",
                "11",
                "12",
                "14",
                "15",
                "19",
                "22",
                "25",
            ],
        ]
        df = df.dropna(subset=["4"])
        df = df.rename(
            columns={
                "2": "cod_recurso",
                "4": "desc_recurso",
                "11": "fuente",
                "12": "org_fin",
                "14": "ppto_inicial",
                "15": "ppto_modif",
                "19": "ppto_vigente",
                "22": "ingreso",
                "25": "saldo",
            }
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

    if args.file is not None:
        args.file = validate_excel_file(os.path.join(save_path, args.file))

    async with async_playwright() as p:
        try:
            if args.download:
                connect_siif = await login(
                    args.username, args.password, playwright=p, headless=False
                )
                ri102 = Ri102(siif=connect_siif)
                await ri102.go_to_reports()
                await ri102.go_to_specific_report()
                for ejercicio in args.ejercicios:
                    await ri102.download_report(ejercicio=str(ejercicio))
                    await ri102.save_xls_file(
                        save_path=save_path,
                        file_name=str(ejercicio) + "-ri102.xls",
                    )
                    await ri102.read_xls_file(args.file)
                    print(ri102.df)
                    await ri102.process_dataframe()
                    print(ri102.clean_df)
                await ri102.logout()
            else:
                ri102 = Ri102()
                await ri102.read_xls_file(args.file)
                print(ri102.df)
                await ri102.process_dataframe()
                print(ri102.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.ri102 -d
    # poetry run python -m src.siif.handlers.ri102 -f 2025-ri102.xls
