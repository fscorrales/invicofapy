#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 18-jul-2025
Purpose: Read, process and write SIIF's rcocc31 () report
"""

__all__ = ["Rcocc31"]

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
from ..repositories.rcocc31 import Rcocc31Repository
from ..schemas.rcocc31 import Rcocc31Report
from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rcocc31",
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
        "-c",
        "--cuentas",
        metavar="Cuentas Contables",
        default=["1112-2-6"],
        nargs="+",
        type=str,
        help="Cuentas Contables to download from SIIF",
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
        help="SIIF' rcocc31.xls report. Must be in the same folder",
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
class Rcocc31(SIIFReportManager):
    # --------------------------------------------------
    async def download_and_process_report(
        self,
        ejercicio: int = dt.datetime.now().year,
        cta_contable: str = "1112-2-6",
    ) -> pd.DataFrame:
        """Download and process the rcocc31 report for a specific year."""
        try:
            await self.go_to_specific_report()
            self.download = await self.download_report(
                ejercicio=str(ejercicio), cta_contable=cta_contable
            )
            if self.download is None:
                raise ValueError("No se pudo descargar el reporte rcocc31.")
            await self.read_xls_file()
            return await self.process_dataframe()
        except Exception as e:
            print(f"Error al descargar y procesar el reporte: {e}")

    # --------------------------------------------------
    async def download_and_sync_validated_to_repository(
        self, ejercicio: int = dt.datetime.now().year, cta_contable: str = "1112-2-6"
    ) -> RouteReturnSchema:
        """Download, process and sync the rcocc31 report to the repository."""
        try:
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=await self.download_and_process_report(
                    ejercicio=ejercicio, cta_contable=cta_contable
                ),
                model=Rcocc31Report,
                field_id="nro_entrada",
            )
            return await sync_validated_to_repository(
                repository=Rcocc31Repository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": ejercicio, "cta_contable": cta_contable},
                title=f"SIIF Rcocc31 Report cta contable: {cta_contable} y ejercicio: {ejercicio}",
                logger=logger,
                label=f"Ejercicio {ejercicio} y cta contable {cta_contable} del rcocc31",
            )
        except Exception as e:
            print(f"Error al descargar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def sync_validated_sqlite_to_repository(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        """Download, process and sync the rcocc31 report to the repository."""
        try:
            df = get_df_from_sql_table(sqlite_path, table="mayor_contable_rcocc31")
            df.drop(columns=["id"], inplace=True)
            df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
            df = df.loc[df["ejercicio"] < 2024]

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=Rcocc31Report,
                field_id="nro_entrada",
            )

            return await sync_validated_to_repository(
                repository=Rcocc31Repository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": {"$lt": 2024}},
                title="Sync SIIF Rcocc31 Report from SQLite",
                logger=logger,
                label="Sync SIIF Rcocc31 Report from SQLite",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.select_report_module(module=ReportCategory.Contabilidad)
        await self.select_specific_report_by_id(report_id="387")

    # --------------------------------------------------
    async def download_report(
        self,
        ejercicio: str = str(dt.datetime.now().year),
        cta_contable: str = "1112-2-6",
    ) -> Download:
        try:
            self.download = None
            # Getting DOM elements
            input_ejercicio = self.siif.reports_page.locator(
                "//input[@id='pt1:txtAnioEjercicio::content']"
            )
            input_nivel = self.siif.reports_page.locator(
                "//input[@id='pt1:txtNivel::content']"
            )
            input_mayor = self.siif.reports_page.locator(
                "//input[@id='pt1:txtMayor::content']"
            )
            input_subcuenta = self.siif.reports_page.locator(
                "//input[@id='pt1:txtSubCuenta::content']"
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
            # Cuentas Contables
            nivel, mayor, subcuenta = cta_contable.split("-")
            await input_nivel.clear()
            await input_nivel.fill(nivel)
            await input_mayor.clear()
            await input_mayor.fill(mayor)
            await input_subcuenta.clear()
            await input_subcuenta.fill(subcuenta)
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

        df["ejercicio"] = df.iloc[3, 1][-4:]
        df["cta_contable"] = (
            df.iloc[10, 6] + "-" + df.iloc[10, 11] + "-" + df.iloc[10, 12]
        )
        df = df.replace(to_replace="", value=None)
        df = df.iloc[20:, :]
        df = df.rename(
            {
                "3": "nro_entrada",
                "10": "nro_original",
                "14": "fecha_aprobado",
                "19": "auxiliar_1",
                "22": "auxiliar_2",
                "25": "tipo_comprobante",
                "26": "debitos",
                "28": "creditos",
                "29": "saldo",
            },
            axis="columns",
        )
        df = df.dropna(subset=["nro_entrada"])
        df["fecha_aprobado"] = pd.to_datetime(df["fecha_aprobado"], format="%Y-%m-%d %H:%M:%S")
        df["fecha"] = df["fecha_aprobado"]
        # df.loc[df['fecha_aprobado'].dt.year.astype(str) == df['ejercicio'], 'fecha'] = df['fecha_aprobado']
        df.loc[df["fecha_aprobado"].dt.year.astype(str) != df["ejercicio"], "fecha"] = (
            pd.to_datetime(df["ejercicio"] + "-12-31", format="%Y-%m-%d")
        )
        df["mes"] = (
            df["fecha"].dt.month.astype(str).str.zfill(2) + "/" + df["ejercicio"]
        )

        df = df.loc[
            :,
            [
                "ejercicio",
                "mes",
                "fecha",
                "fecha_aprobado",
                "cta_contable",
                "nro_entrada",
                "nro_original",
                "auxiliar_1",
                "auxiliar_2",
                "tipo_comprobante",
                "debitos",
                "creditos",
                "saldo",
            ],
        ]
        df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
        to_numeric_cols = ["debitos", "creditos", "saldo"]
        df[to_numeric_cols] = df[to_numeric_cols].apply(pd.to_numeric)

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
                rcocc31 = Rcocc31(siif=connect_siif)
                await rcocc31.go_to_reports()
                await rcocc31.go_to_specific_report()
                for ejercicio in args.ejercicios:
                    for cta_contable in args.cuentas:
                        await rcocc31.download_report(ejercicio=str(ejercicio))
                        await rcocc31.save_xls_file(
                            save_path=save_path,
                            file_name=str(ejercicio)
                            + "-rcocc31 ("
                            + cta_contable
                            + ").xls",
                        )
                        await rcocc31.read_xls_file(args.file)
                        print(rcocc31.df)
                        await rcocc31.process_dataframe()
                        print(rcocc31.clean_df)
                await rcocc31.logout()
            else:
                rcocc31 = Rcocc31()
                await rcocc31.read_xls_file(args.file)
                print(rcocc31.df)
                await rcocc31.process_dataframe()
                print(rcocc31.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.rcocc31 -d
    # poetry run python -m src.siif.handlers.rcocc31 -f '2025-rcocc31 (1112-2-6).xls'
