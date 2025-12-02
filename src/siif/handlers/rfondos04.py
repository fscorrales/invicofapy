#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 19-jun-2025
Purpose: Read, process and write SIIF's rfondos04 (...) report
"""

__all__ = ["Rfondos04"]

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
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..repositories.rfondos04 import Rfondos04Repository
from ..schemas.common import TipoComprobanteSIIF
from ..schemas.rfondos04 import Rfondos04Report
from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rfondos04 report",
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
        "-tc",
        "--tipo_comprobante",
        metavar="tipo_comprobante",
        default="REV",
        type=str,
        choices=[c.value for c in TipoComprobanteSIIF],
        help="Tipo Comprobante to download from SIIF",
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
        help="SIIF' rfondos04.xls report. Must be in the same folder",
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
class Rfondos04(SIIFReportManager):
    # --------------------------------------------------
    async def download_and_process_report(
        self,
        ejercicio: int = dt.datetime.now().year,
        tipo_comprobante: TipoComprobanteSIIF = TipoComprobanteSIIF.reversion_viatico.value,
    ) -> pd.DataFrame:
        """Download and process the rfondos04 report for a specific year."""
        try:
            await self.go_to_specific_report()
            self.download = await self.download_report(
                ejercicio=str(ejercicio), tipo_comprobante=str(tipo_comprobante)
            )
            if self.download is None:
                raise ValueError("No se pudo descargar el reporte rfondos04.")
            await self.read_xls_file()
            return await self.process_dataframe()
        except Exception as e:
            print(f"Error al descargar y procesar el reporte: {e}")

    # --------------------------------------------------
    async def download_and_sync_validated_to_repository(
        self,
        ejercicio: int = dt.datetime.now().year,
        tipo_comprobante: TipoComprobanteSIIF = TipoComprobanteSIIF.reversion_viatico.value,
    ) -> RouteReturnSchema:
        """Download, process and sync the rcg01_Uejp report to the repository."""
        try:
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=await self.download_and_process_report(
                    ejercicio=ejercicio, tipo_comprobante=tipo_comprobante
                ),
                model=Rfondos04Report,
                field_id="nro_comprobante",
            )
            return await sync_validated_to_repository(
                repository=Rfondos04Repository(),
                validation=validate_and_errors,
                delete_filter={
                    "ejercicio": ejercicio,
                    "tipo_comprobante": tipo_comprobante,
                },
                title=f"SIIF rfondos04 Report {ejercicio} - {tipo_comprobante}",
                logger=logger,
                label=f"Ejercicio {ejercicio} del rfondos04 (tipo comprobante: {tipo_comprobante}).",
            )
        except Exception as e:
            print(f"Error al descargar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.select_report_module(module=ReportCategory.Gastos)
        await self.select_specific_report_by_id(report_id="477")

    # --------------------------------------------------
    async def download_report(
        self,
        ejercicio: str = str(dt.datetime.now().year),
        tipo_comprobante: TipoComprobanteSIIF = TipoComprobanteSIIF.reversion_viatico.value,
    ) -> Download:
        try:
            self.download = None
            # Getting DOM elements
            input_ejercicio = self.siif.reports_page.locator(
                "//input[@id='pt1:txtAnioEjercicio::content']"
            )
            input_tipo_comprobante = self.siif.reports_page.locator(
                "//input[@id='pt1:txtTipoCte::content']"
            )
            btn_get_reporte = self.siif.reports_page.locator(
                "//div[@id='pt1:btnVerReporte']"
            )
            btn_xls = self.siif.reports_page.locator(
                "//input[@id='pt1:rbtnXLS::content']"
            )
            await btn_xls.click()

            # Ejercicio
            await input_ejercicio.clear()
            await input_ejercicio.fill(str(ejercicio))
            # Tipo Comprobante
            await input_tipo_comprobante.clear()
            await input_tipo_comprobante.fill(tipo_comprobante)

            await btn_get_reporte.click()

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
    async def process_dataframe(
        self,
        dataframe: pd.DataFrame = None,
    ) -> pd.DataFrame:
        """ "Transform read xls file"""
        if dataframe is None:
            df = self.df.copy()
        else:
            df = dataframe.copy()

        df = df.replace(to_replace="", value=None)
        # df["ejercicio"] = pd.to_numeric(df.iloc[4, 1][-4:], errors="coerce")
        # df["tipo_comprobante"] = tipo_comprobante
        df = df.tail(-17)  # Eliminar filas de encabezado innecesarias
        df = df.dropna(subset=["10"])
        df = df.rename(
            columns={
                "2": "ejercicio",
                "4": "nro_comprobante",
                "6": "nro_fondo",
                "13": "tipo_comprobante",
                "14": "fecha",
                "15": "importe",
                "19": "glosa",
                "20": "saldo_c01",
                "23": "saldo_asiento",
            }
        )

        df["mes"] = df["fecha"].str[5:7] + "/" + df["ejercicio"].astype(str)
        df["nro_comprobante"] = df["nro_fondo"].str.zfill(5) + "/" + df["mes"].str[-2:]

        # 👇 conversión a datetime64[ns]
        df["fecha"] = pd.to_datetime(
            df["fecha"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
        )

        # 👇 conversión explícita a datetime.datetime de Python
        # df["fecha"] = df["fecha"].dt.to_pydatetime()
        df["fecha"] = df["fecha"].apply(
            lambda x: x.to_pydatetime() if pd.notnull(x) else None
        )

        to_numeric_cols = ["importe", "saldo_c01", "saldo_asiento"]
        df[to_numeric_cols] = (
            df[to_numeric_cols].apply(pd.to_numeric).astype(np.float64)
        )

        df = df.loc[
            :,
            [
                "ejercicio",
                "mes",
                "fecha",
                "tipo_comprobante",
                "nro_comprobante",
                "nro_fondo",
                "glosa",
                "importe",
                "saldo_c01",
                "saldo_asiento",
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
            rfondos04 = Rfondos04(siif=connect_siif)
            await rfondos04.go_to_reports()
            await rfondos04.go_to_specific_report()
            for ejercicio in args.ejercicios:
                if args.download:
                    await rfondos04.download_report(
                        ejercicio=str(ejercicio),
                        tipo_comprobante=str(args.tipo_comprobante),
                    )
                    await rfondos04.save_xls_file(
                        save_path=save_path,
                        file_name=str(ejercicio)
                        + "-rfondos04 ("
                        + str(args.tipo_comprobante)
                        + ").xls",
                    )
                await rfondos04.read_xls_file(args.file)
                print(rfondos04.df)
                await rfondos04.process_dataframe()
                print(rfondos04.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")
        finally:
            await rfondos04.logout()


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.rfondos04 -d
    # poetry run python -m src.siif.handlers.rfondos04 -f "2025-rfondos04 (REV).xls"
