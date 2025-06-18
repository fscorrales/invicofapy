#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 18-jun-2025
Purpose: Read, process and write SIIF's rpa03g (...) report
"""

__all__ = ["Rpa03g"]

import argparse
import asyncio
import datetime as dt
import inspect
import os

import numpy as np
import pandas as pd
from playwright.async_api import Download, async_playwright

from ..schemas.common import GrupoPartidaSIIF
from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rpa03g",
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
        "-g",
        "--grupo_partida",
        metavar="grupo_partida",
        default=4,
        type=int,
        choices=[int(c.value) for c in GrupoPartidaSIIF],
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
class Rpa03g(SIIFReportManager):
    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.select_report_module(module=ReportCategory.Gastos)
        await self.select_specific_report_by_id(report_id="1175")

    # --------------------------------------------------
    async def download_report(
        self,
        ejercicio: str = str(dt.datetime.now().year),
        group_part: GrupoPartidaSIIF = "4",
    ) -> Download:
        try:
            self.download = None
            # Getting DOM elements
            input_ejercicio = self.siif.reports_page.locator(
                "//input[@id='pt1:txtAnioEjercicio::content']"
            )
            input_gpo_partida = self.siif.reports_page.locator(
                "//input[@id='pt1:txtGrupoPartida::content']"
            )
            input_mes_desde = self.siif.reports_page.locator(
                "//input[@id='pt1:txtMesDesde::content']"
            )
            input_mes_hasta = self.siif.reports_page.locator(
                "//input[@id='pt1:txtMesHasta::content']"
            )
            btn_get_reporte = self.siif.reports_page.locator(
                "//div[@id='pt1:btnVerReporte']"
            )
            btn_xls = self.siif.reports_page.locator(
                "//input[@id='pt1:rbtnXLS::content']"
            )
            await btn_xls.click()

            # Mes Desde
            await input_mes_desde.clear()
            await input_mes_desde.fill("1")
            # Mes Hasta
            await input_mes_hasta.clear()
            await input_mes_hasta.fill("12")
            # Ejercicio
            await input_ejercicio.clear()
            await input_ejercicio.fill(str(ejercicio))
            # Grupo de Partida
            await input_gpo_partida.clear()
            await input_gpo_partida.fill(group_part)

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
        df["ejercicio"] = pd.to_numeric(df.iloc[3, 18][-4:], errors="coerce")
        df = df.tail(-21)
        df = df.dropna(subset=["1"])
        df = df.rename(
            columns={
                "1": "nro_entrada",
                "5": "nro_origen",
                "8": "importe",
                "14": "fecha",
                "17": "partida",
                "19": "nro_expte",
                "21": "glosa",
                "23": "beneficiario",
            }
        )
        df["importe"] = pd.to_numeric(df["importe"]).astype(np.float64)
        df["grupo"] = df["partida"].str[0] + "00"
        df["mes"] = df["fecha"].str[5:7] + "/" + df["ejercicio"]
        df["nro_comprobante"] = (
            df["nro_entrada"].str.zfill(5) + "/" + df["mes"].str[-2:]
        )

        df["fecha"] = pd.to_datetime(df["fecha"], format="%Y-%m-%d")

        df = df.loc[
            :,
            [
                "ejercicio",
                "mes",
                "fecha",
                "nro_comprobante",
                "importe",
                "grupo",
                "partida",
                "nro_entrada",
                "nro_origen",
                "nro_expte",
                "glosa",
                "beneficiario",
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
            rpa03g = Rpa03g(siif=connect_siif)
            await rpa03g.go_to_reports()
            await rpa03g.go_to_specific_report()
            for ejercicio in args.ejercicios:
                if args.download:
                    await rpa03g.download_report(ejercicio=str(ejercicio))
                    await rpa03g.save_xls_file(
                        save_path=save_path,
                        file_name=str(ejercicio)
                        + "-gto_rpa03g (Gpo "
                        + args.grupo_partida
                        + "00).xls",
                    )
                await rpa03g.read_xls_file(args.file)
                print(rpa03g.df)
                await rpa03g.process_dataframe()
                print(rpa03g.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")
        finally:
            await rpa03g.logout()


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.rpa03g -d
