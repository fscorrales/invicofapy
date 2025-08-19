#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 24-jul-2025
Purpose: Read, process and write SIIF's rvicon03 () report
"""

__all__ = ["Rvicon03"]

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
from ..repositories.rvicon03 import Rvicon03Repository
from ..schemas.rvicon03 import Rvicon03Report
from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rvicon03",
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
        help="SIIF' rvicon03.xls report. Must be in the same folder",
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
class Rvicon03(SIIFReportManager):
    # --------------------------------------------------
    async def download_and_process_report(
        self, ejercicio: int = dt.datetime.now().year
    ) -> pd.DataFrame:
        """Download and process the rvicon03 report for a specific year."""
        try:
            await self.go_to_specific_report()
            self.download = await self.download_report(ejercicio=str(ejercicio))
            if self.download is None:
                raise ValueError("No se pudo descargar el reporte rvicon03.")
            await self.read_xls_file()
            return await self.process_dataframe()
        except Exception as e:
            print(f"Error al descargar y procesar el reporte: {e}")

    # --------------------------------------------------
    async def download_and_sync_validated_to_repository(
        self, ejercicio: int = dt.datetime.now().year
    ) -> RouteReturnSchema:
        """Download, process and sync the rvicon03 report to the repository."""
        try:
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=await self.download_and_process_report(ejercicio=ejercicio),
                model=Rvicon03Report,
                field_id="cta_contable",
            )
            return await sync_validated_to_repository(
                repository=Rvicon03Repository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": ejercicio},
                title=f"SIIF Rvicon03 Report del {ejercicio}",
                logger=logger,
                label=f"Ejercicio {ejercicio} del rvicon03",
            )
        except Exception as e:
            print(f"Error al descargar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def sync_validated_sqlite_to_repository(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        """Download, process and sync the rvicon03 report to the repository."""
        try:
            df = get_df_from_sql_table(
                sqlite_path, table="resumen_contable_cta_rvicon03"
            )
            df.drop(columns=["id"], inplace=True)
            df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
            df = df.loc[df["ejercicio"] < 2024]
            df.rename(
                columns={
                    "nivel_desc": "desc_nivel",
                    "cta_contable_desc": "desc_cta_contable",
                },
                inplace=True,
            )

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=Rvicon03Report,
                field_id="cta_contable",
            )

            return await sync_validated_to_repository(
                repository=Rvicon03Repository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": {"$lt": 2024}},
                title="Sync SIIF Rvicon03 Report from SQLite",
                logger=logger,
                label="Sync SIIF Rvicon03 Report from SQLite",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.select_report_module(module=ReportCategory.Contabilidad)
        await self.select_specific_report_by_id(report_id="2079")

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

        df["ejercicio"] = pd.to_numeric(df.iloc[3, 2][-4:], errors="coerce")
        df = df.tail(-18)
        df = df.loc[:, ["ejercicio", "2", "6", "7", "8", "10", "11", "12", "13", "15"]]
        df = df.replace(to_replace="", value=None)
        df = df.dropna(subset=["2"])
        df = df.rename(
            columns={
                "2": "nivel_descripcion",
                "6": "saldo_inicial",
                "7": "debe",
                "8": "haber",
                "10": "ajuste_debe",
                "11": "ajuste_haber",
                "12": "fondos_debe",
                "13": "fondos_haber",
                "15": "saldo_final",
            }
        )
        df["nivel"] = np.where(
            df["saldo_inicial"].isnull(), df["nivel_descripcion"].str[0:4], None
        )
        df["nivel"] = df["nivel"].ffill()
        df["desc_nivel"] = np.where(
            df["saldo_inicial"].isnull(), df["nivel_descripcion"].str[8:], None
        )
        df["desc_nivel"] = df["desc_nivel"].ffill()
        df = df.dropna(subset=["saldo_inicial"])

        df["cta_contable"] = (
            df["nivel_descripcion"]
            .str.split("-", expand=True)
            .iloc[:, :3]
            .agg("-".join, axis=1)
        )
        df["desc_cta_contable"] = df["nivel_descripcion"].apply(
            lambda x: "-".join(filter(None, x.split("-")[3:]))
            if x is not None
            else None
        )
        df = df.loc[
            :,
            [
                "ejercicio",
                "nivel",
                "desc_nivel",
                "cta_contable",
                "desc_cta_contable",
                "saldo_inicial",
                "debe",
                "haber",
                "ajuste_debe",
                "ajuste_haber",
                "fondos_debe",
                "fondos_haber",
                "saldo_final",
            ],
        ]
        to_numeric_cols = [
            "saldo_inicial",
            "debe",
            "haber",
            "ajuste_debe",
            "ajuste_haber",
            "fondos_debe",
            "fondos_haber",
            "saldo_final",
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(pd.to_numeric)
        df = df[~df[to_numeric_cols].apply(lambda x: (x == 0).all(), axis=1)]

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
                rvicon03 = Rvicon03(siif=connect_siif)
                await rvicon03.go_to_reports()
                await rvicon03.go_to_specific_report()
                for ejercicio in args.ejercicios:
                    await rvicon03.download_report(ejercicio=str(ejercicio))
                    await rvicon03.save_xls_file(
                        save_path=save_path,
                        file_name=str(ejercicio) + "-rvicon03.xls",
                    )
                    await rvicon03.read_xls_file(args.file)
                    print(rvicon03.df)
                    await rvicon03.process_dataframe()
                    print(rvicon03.clean_df)
                await rvicon03.logout()
            else:
                rvicon03 = Rvicon03()
                await rvicon03.read_xls_file(args.file)
                print(rvicon03.df)
                await rvicon03.process_dataframe()
                print(rvicon03.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.rvicon03 -d
    # poetry run python -m src.siif.handlers.rvicon03 -f 2025-rvicon03.xls
