#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 20-jul-2025
Purpose: Read, process and write SIIF's rci02 () report
"""

__all__ = ["Rci02"]

import argparse
import asyncio
import datetime as dt
import inspect
import os

import numpy as np
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
from ..repositories.rci02 import Rci02Repository
from ..schemas.rci02 import Rci02Report
from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rci02",
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
class Rci02(SIIFReportManager):
    # --------------------------------------------------
    async def download_and_process_report(
        self, ejercicio: int = dt.datetime.now().year
    ) -> pd.DataFrame:
        """Download and process the rci02 report for a specific year."""
        try:
            await self.go_to_specific_report()
            self.download = await self.download_report(ejercicio=str(ejercicio))
            if self.download is None:
                raise ValueError("No se pudo descargar el reporte rci02.")
            await self.read_xls_file()
            return await self.process_dataframe()
        except Exception as e:
            print(f"Error al descargar y procesar el reporte: {e}")

    # --------------------------------------------------
    async def download_and_sync_validated_to_repository(
        self, ejercicio: int = dt.datetime.now().year
    ) -> RouteReturnSchema:
        """Download, process and sync the rci02 report to the repository."""
        try:
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=await self.download_and_process_report(ejercicio=ejercicio),
                model=Rci02Report,
                field_id="nro_entrada",
            )
            return await sync_validated_to_repository(
                repository=Rci02Repository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": ejercicio},
                title=f"SIIF Rci02 Report del {ejercicio}",
                logger=logger,
                label=f"Ejercicio {ejercicio} del rci02",
            )
        except Exception as e:
            print(f"Error al descargar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def sync_validated_sqlite_to_repository(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        """Download, process and sync the rci02 report to the repository."""
        try:
            df = get_df_from_sql_table(sqlite_path, table="comprobantes_rec_rci02")
            df.drop(columns=["id"], inplace=True)
            df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
            df = df.loc[df["ejercicio"] < 2024]

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=Rci02Report,
                field_id="nro_entrada",
            )

            return await sync_validated_to_repository(
                repository=Rci02Repository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": {"$lt": 2024}},
                title="Sync SIIF Rci02 Report from SQLite",
                logger=logger,
                label="Sync SIIF Rci02 Report from SQLite",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.select_report_module(module=ReportCategory.Recursos)
        await self.select_specific_report_by_id(report_id="33")

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
            input_fecha_desde = self.siif.reports_page.locator(
                "//input[@id='pt1:idFechaDesde::content']"
            )
            input_fecha_hasta = self.siif.reports_page.locator(
                "//input[@id='pt1:idFechaHasta::content']"
            )
            btn_get_reporte = self.siif.reports_page.locator(
                "//div[@id='pt1:btnEjecutarReporte']"
            )
            btn_xls = self.siif.reports_page.locator(
                "//input[@id='pt1:rbtnXLS::content']"
            )
            await btn_xls.click()

            # Ejercicio
            await input_ejercicio.clear()
            await input_ejercicio.fill(str(ejercicio))
            # Fecha Desde
            await input_fecha_desde.clear()
            fecha_desde = dt.datetime.strftime(
                dt.date(year=int(ejercicio), month=1, day=1), "%d/%m/%Y"
            )
            await input_fecha_desde.fill(fecha_desde)
            # Fecha Hasta
            await input_fecha_hasta.clear()
            fecha_hasta = dt.datetime(year=(int(ejercicio) + 1), month=12, day=31)
            fecha_hasta = min(fecha_hasta, dt.datetime.now())
            fecha_hasta = dt.datetime.strftime(fecha_hasta, "%d/%m/%Y")
            await input_fecha_hasta.fill(fecha_hasta)

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

        df["ejercicio"] = df.iloc[3, 34]
        df = df.replace(to_replace="", value=None)
        df = df.tail(-22)
        df = df.dropna(subset=["2"])
        df = df.rename(
            columns={
                "17": "fecha",
                "6": "fuente",
                "28": "cta_cte",
                "2": "nro_entrada",
                "23": "importe",
                "32": "glosa",
                "42": "es_verificado",
                "10": "clase_reg",
                "13": "clase_mod",
            }
        )
        df["mes"] = df["fecha"].str[5:7] + "/" + df["ejercicio"]
        df["es_remanente"] = df["glosa"].str.contains("REMANENTE")
        df["es_invico"] = df["glosa"].str.contains("%")
        df["es_verificado"] = np.where(df["es_verificado"] == "S", True, False)
        df["importe"] = df["importe"].apply(pd.to_numeric).astype(np.float64)

        df["fecha"] = pd.to_datetime(df["fecha"], format="%Y-%m-%d %H:%M:%S")
        df["fecha"] = df["fecha"].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)
        df = df.loc[
            :,
            [
                "ejercicio",
                "mes",
                "fecha",
                "fuente",
                "cta_cte",
                "nro_entrada",
                "importe",
                "glosa",
                "es_remanente",
                "es_invico",
                "es_verificado",
                "clase_reg",
                "clase_mod",
            ],
        ]

        df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
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
                rci02 = Rci02(siif=connect_siif)
                await rci02.go_to_reports()
                await rci02.go_to_specific_report()
                for ejercicio in args.ejercicios:
                    await rci02.download_report(ejercicio=str(ejercicio))
                    await rci02.save_xls_file(
                        save_path=save_path,
                        file_name=str(ejercicio) + "-rci02.xls",
                    )
                    await rci02.read_xls_file(args.file)
                    print(rci02.df)
                    await rci02.process_dataframe()
                    print(rci02.clean_df)
                await rci02.logout()
            else:
                rci02 = Rci02()
                await rci02.read_xls_file(args.file)
                print(rci02.df)
                await rci02.process_dataframe()
                print(rci02.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.rci02 -d
    # poetry run python -m src.siif.handlers.rci02 -f 2025-rci02.xls
