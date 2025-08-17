#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 21-jul-2025
Purpose: Read, process and write SIIF's rdeu012 () report
"""

__all__ = ["Rdeu012"]

import argparse
import asyncio
import datetime as dt
import inspect
import os
from datetime import timedelta

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
from ..repositories.rdeu012 import Rdeu012Repository
from ..schemas.rdeu012 import Rdeu012Report
from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rdeu012",
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
        "-m",
        "--meses",
        metavar="Año y mes",
        default=["202212"],
        type=str,
        nargs="+",
        help="Lista de año y mes en formato yyyymm",
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
        help="SIIF' rdeu012.xls report. Must be in the same folder",
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
class Rdeu012(SIIFReportManager):
    # --------------------------------------------------
    async def download_and_process_report(
        self, mes: str = dt.datetime.strftime(dt.datetime.now(), "%Y-%m")
    ) -> pd.DataFrame:
        """Download and process the rdeu012 report for a specific year."""
        try:
            await self.go_to_specific_report()
            self.download = await self.download_report(mes=str(mes))
            if self.download is None:
                raise ValueError("No se pudo descargar el reporte rdeu012.")
            await self.read_xls_file()
            return await self.process_dataframe()
        except Exception as e:
            print(f"Error al descargar y procesar el reporte: {e}")

    # --------------------------------------------------
    async def download_and_sync_validated_to_repository(
        self, mes: str = dt.datetime.strftime(dt.datetime.now(), "%Y-%m")
    ) -> RouteReturnSchema:
        """Download, process and sync the rdeu012 report to the repository."""
        try:
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=await self.download_and_process_report(mes=mes),
                model=Rdeu012Report,
                field_id="nro_comprobante",
            )
            return await sync_validated_to_repository(
                repository=Rdeu012Repository(),
                validation=validate_and_errors,
                delete_filter={"mes_hasta": mes},
                title=f"SIIF Rdeu012 Report del mes: {mes}",
                logger=logger,
                label=f"Mes {mes} del rdeu012",
            )
        except Exception as e:
            print(f"Error al descargar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def sync_validated_sqlite_to_repository(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        """Download, process and sync the rdeu012 report to the repository."""
        try:
            df = get_df_from_sql_table(sqlite_path, table="deuda_flotante_rdeu012")
            df.drop(columns=["id"], inplace=True)
            df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
            df = df.loc[df["ejercicio"] < 2024]

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=Rdeu012Report,
                field_id="nro_comprobante",
            )

            return await sync_validated_to_repository(
                repository=Rdeu012Repository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": {"$lt": 2024}},
                title="Sync SIIF Rdeu012 Report from SQLite",
                logger=logger,
                label="Sync SIIF Rdeu012 Report from SQLite",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.select_report_module(module=ReportCategory.Gastos)
        await self.select_specific_report_by_id(report_id="267")

    # --------------------------------------------------
    async def download_report(
        self, mes: str = dt.datetime.strftime(dt.datetime.now(), "%Y-%m")
    ) -> Download:
        try:
            self.download = None
            # Getting DOM elements
            input_cod_fuente = self.siif.reports_page.locator(
                "//input[@id='pt1:inputText3::content']"
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

            # Fuente
            await input_cod_fuente.clear()
            await input_cod_fuente.fill("0")
            # Fecha desde
            await input_fecha_desde.clear()
            await input_cod_fuente.fill("01/01/2010")
            # Fecha hasta
            int_ejercicio = int(mes[0:4])
            if int_ejercicio > 2010 and int_ejercicio <= dt.datetime.now().year:
                fecha_hasta = dt.datetime(
                    year=(int_ejercicio), month=int(mes[-2:]), day=1
                )
                next_month = fecha_hasta.replace(day=28) + timedelta(days=4)
                fecha_hasta = next_month - timedelta(days=next_month.day)
                fecha_hasta = min(fecha_hasta.date(), dt.date.today())
                fecha_hasta = dt.datetime.strftime(fecha_hasta, "%d/%m/%Y")
                await input_fecha_desde.clear()
                input_fecha_hasta.fill(fecha_hasta)
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

        df["6"] = df["6"].replace(to_replace="TODOS", value="")
        df.loc[df["6"] != "27", "fuente"] = df["6"]
        df["fecha_desde"] = df.iloc[15, 2].split(" ")[2]
        df["fecha_hasta"] = df.iloc[15, 2].split(" ")[6]
        df["fecha_desde"] = pd.to_datetime(df["fecha_desde"], format="%d/%m/%Y")
        df["fecha_hasta"] = pd.to_datetime(df["fecha_hasta"], format="%d/%m/%Y")
        df["mes_hasta"] = (
            df["fecha_hasta"].dt.month.astype(str).str.zfill(2)
            + "/"
            + df["fecha_hasta"].dt.year.astype(str)
        )
        df = df.replace(to_replace="", value=None)
        df = df.tail(-13)
        df["fuente"] = df["fuente"].fillna(method="ffill")
        df = df.dropna(subset=["2"])
        df = df.dropna(subset=["18"])
        df = df.rename(
            columns={
                "2": "nro_entrada",
                "4": "nro_origen",
                "7": "fecha_aprobado",
                "9": "org_fin",
                "10": "importe",
                "13": "saldo",
                "14": "nro_expte",
                "15": "cta_cte",
                "17": "glosa",
                "18": "cuit",
                "19": "beneficiario",
            }
        )

        to_numeric = ["importe", "saldo"]
        df[to_numeric] = df[to_numeric].apply(pd.to_numeric).astype(np.float64)

        df["fecha_aprobado"] = pd.to_datetime(df["fecha_aprobado"], format="%Y-%m-%d")
        df["mes_aprobado"] = df["fecha_aprobado"].dt.strftime("%m/%Y")

        df["fecha"] = np.where(
            df["fecha_aprobado"] > df["fecha_hasta"],
            df["fecha_hasta"],
            df["fecha_aprobado"],
        )
        # df = df >> \
        #     dplyr.mutate(
        #         fecha = dplyr.if_else(f.fecha_aprobado > f.fecha_hasta,
        #                                 f.fecha_hasta, f.fecha_aprobado)
        #     )

        # CYO aprobados en enero correspodientes al ejercicio anterior
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        df["fecha"] = df["fecha"].dt.strftime("%Y-%m-%d")
        condition = (df["mes_aprobado"].str[0:2] == "01") & (
            df["nro_entrada"].astype(int) > 1500
        )
        df.loc[condition, "fecha"] = (
            pd.to_numeric(df["mes_hasta"].loc[condition].str[-4:])
        ).astype(str) + "-12-31"

        df["fecha"] = pd.to_datetime(df["fecha"], format="%Y-%m-%d", errors="coerce")

        df["ejercicio"] = df["fecha"].dt.year
        df["mes"] = df["fecha"].dt.strftime("%m/%Y")

        df["nro_comprobante"] = (
            df["nro_entrada"].str.zfill(5) + "/" + df["mes"].str[-2:]
        )
        df = df.loc[
            :,
            [
                "ejercicio",
                "mes",
                "fecha",
                "mes_hasta",
                "fuente",
                "cta_cte",
                "nro_comprobante",
                "importe",
                "saldo",
                "cuit",
                "beneficiario",
                "glosa",
                "nro_expte",
                "nro_entrada",
                "nro_origen",
                "fecha_aprobado",
                "fecha_desde",
                "fecha_hasta",
                "org_fin",
            ],
        ]

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
                rdeu012 = Rdeu012(siif=connect_siif)
                await rdeu012.go_to_reports()
                await rdeu012.go_to_specific_report()
                for mes in args.meses:
                    await rdeu012.download_report(mes=str(mes))
                    await rdeu012.save_xls_file(
                        save_path=save_path,
                        file_name=mes[0:4] + mes[-2:] + "-rdeu012.xls",
                    )
                    await rdeu012.read_xls_file(args.file)
                    print(rdeu012.df)
                    await rdeu012.process_dataframe()
                    print(rdeu012.clean_df)
                await rdeu012.logout()
            else:
                rdeu012 = Rdeu012()
                await rdeu012.read_xls_file(args.file)
                print(rdeu012.df)
                await rdeu012.process_dataframe()
                print(rdeu012.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.rdeu012 -d
    # poetry run python -m src.siif.handlers.rdeu012 -f 202412-rdeu012.xls
