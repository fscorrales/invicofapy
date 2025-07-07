#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 13-jun-2025
Purpose: Read, process and write SIIF's rcg01_uejp (Comprobantes Ingresados por Entidad y Unidad Ejecutora) report
"""

__all__ = ["Rcg01Uejp"]

import argparse
import asyncio
import datetime as dt
import inspect
import os

import numpy as np
import pandas as pd
from playwright.async_api import Download, async_playwright

from ...config import logger
from ...utils.validate import (
    RouteReturnSchema,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..repositories.rcg01_uejp import Rcg01UejpRepository
from ..schemas.rcg01_uejp import Rcg01UejpReport
from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rcg01_uejp",
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
class Rcg01Uejp(SIIFReportManager):
    # --------------------------------------------------
    async def download_and_process_report(
        self, ejercicio: int = dt.datetime.now().year
    ) -> pd.DataFrame:
        """Download and process the rcg01_Uejp report for a specific year."""
        try:
            await self.go_to_specific_report()
            self.download = await self.download_report(ejercicio=str(ejercicio))
            if self.download is None:
                raise ValueError("No se pudo descargar el reporte rcg01_Uejp.")
            await self.read_xls_file()
            return await self.process_dataframe()
        except Exception as e:
            print(f"Error al descargar y procesar el reporte: {e}")

    # --------------------------------------------------
    async def download_and_sync_validated_to_repository(
        self, ejercicio: int = dt.datetime.now().year
    ) -> RouteReturnSchema:
        """Download, process and sync the rcg01_Uejp report to the repository."""
        try:
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=await self.download_and_process_report(ejercicio=ejercicio),
                model=Rcg01UejpReport,
                field_id="nro_comprobante",
            )
            return await sync_validated_to_repository(
                repository=Rcg01UejpRepository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": ejercicio},
                title=f"SIIF rcg01_Uejp Report del {ejercicio}",
                logger=logger,
                label=f"Ejercicio {ejercicio} del rcg01_Uejp",
            )
        except Exception as e:
            print(f"Error al descargar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.select_report_module(module=ReportCategory.Gastos)
        await self.select_specific_report_by_id(report_id="839")

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
            input_unidad_ejecutora = self.siif.reports_page.locator(
                "//input[@id='pt1:txtUnidadEjecutora::content']"
            )
            btn_get_reporte = self.siif.reports_page.locator(
                "//div[@id='pt1:btnVerReporte']"
            )
            btn_xls = self.siif.reports_page.locator(
                "//input[@id='pt1:rbtnXLS::content']"
            )
            await btn_xls.click()

            # Unidad Ejecutora
            await input_unidad_ejecutora.clear()
            await input_unidad_ejecutora.fill("0")
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

        df = df.replace(to_replace="", value=None)
        df["ejercicio"] = pd.to_numeric(df.iloc[2, 1][-4:], errors="coerce")
        df = df.tail(-16)
        df = df.drop(columns=["0", "17", "18"])
        df = df.rename(
            columns={
                "1": "nro_entrada",
                "2": "nro_origen",
                "3": "fuente",
                "4": "clase_reg",
                "5": "clase_mod",
                "6": "clase_gto",
                "7": "fecha",
                "8": "importe",
                "9": "cuit",
                "10": "beneficiario",
                "11": "nro_expte",
                "12": "cta_cte",
                "13": "es_comprometido",
                "14": "es_verificado",
                "15": "es_aprobado",
                "16": "es_pagado",
                "19": "nro_fondo",
            }
        )
        df = df.dropna(subset=["cuit"])
        df = df.dropna(subset=["nro_entrada"])
        df["beneficiario"] = df["beneficiario"].str.replace("\t", "")
        df["importe"] = pd.to_numeric(df["importe"]).astype(np.float64)
        df["es_comprometido"] = df["es_comprometido"] == "S"
        df["es_verificado"] = df["es_verificado"] == "S"
        df["es_aprobado"] = df["es_aprobado"] == "S"
        df["es_pagado"] = df["es_pagado"] == "S"
        df["fecha"] = pd.to_datetime(
            df["fecha"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
        )
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
                "nro_comprobante",
                "importe",
                "fuente",
                "cta_cte",
                "cuit",
                "nro_expte",
                "nro_fondo",
                "nro_entrada",
                "nro_origen",
                "clase_reg",
                "clase_mod",
                "clase_gto",
                "beneficiario",
                "es_comprometido",
                "es_verificado",
                "es_aprobado",
                "es_pagado",
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

    async with async_playwright() as p:
        connect_siif = await login(
            args.username, args.password, playwright=p, headless=False
        )
        try:
            rcg01_uejp = Rcg01Uejp(siif=connect_siif)
            await rcg01_uejp.go_to_reports()
            await rcg01_uejp.go_to_specific_report()
            for ejercicio in args.ejercicios:
                if args.download:
                    await rcg01_uejp.download_report(ejercicio=str(ejercicio))
                    await rcg01_uejp.save_xls_file(
                        save_path=save_path,
                        file_name=str(ejercicio) + "-rcg01_uejp.xls",
                    )
                await rcg01_uejp.read_xls_file(args.file)
                print(rcg01_uejp.df)
                await rcg01_uejp.process_dataframe()
                print(rcg01_uejp.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")
        finally:
            await rcg01_uejp.logout()


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.rcg01_uejp -d
