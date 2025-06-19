#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 19-jun-2025
Purpose: Read, process and write SIIF's rfondo07tp (...) report
"""

__all__ = ["Rfondo07tp"]

import argparse
import asyncio
import datetime as dt
import inspect
import os

import numpy as np
import pandas as pd
from playwright.async_api import Download, async_playwright

from ..schemas.common import TipoComprobanteSIIF
from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rfondo07tp",
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
        default="PA6",
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
        await self.select_specific_report_by_id(report_id="2070")

    # --------------------------------------------------
    async def download_report(
        self,
        ejercicio: str = str(dt.datetime.now().year),
        tipo_comprobante: TipoComprobanteSIIF = TipoComprobanteSIIF.adelanto_contratista.value,
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
            # Fecha Desde
            input_fecha_desde.clear()
            fecha_desde = dt.datetime.strftime(
                dt.date(year=int(ejercicio), month=1, day=1),
                '%d/%m/%Y'
            )
            input_fecha_desde.fill(fecha_desde)
            # Fecha Hasta
            input_fecha_hasta.clear()
            fecha_hasta = dt.datetime(year=(int(ejercicio)+1), month=12, day=31)
            fecha_hasta = min(fecha_hasta, dt.datetime.now())
            fecha_hasta = dt.datetime.strftime(fecha_hasta, '%d/%m/%Y'
            )
            input_fecha_hasta.fill(fecha_hasta)
            # Tipo Comprobante
            input_tipo_comprobante.clear()
            input_tipo_comprobante.fill(tipo_comprobante)

            btn_get_reporte.click()

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
        df["ejercicio"] = pd.to_numeric(df.iloc[4,1][-4:], errors="coerce")
        df['tipo_comprobante'] = df.iloc[11,2].split(':')[2].strip()
        df = df.tail(-19)
        df = df.dropna(subset=['10'])
        df = df.rename(columns={
            '3': 'nro_fondo',
            '6': 'glosa',
            '10': 'fecha',
            '12': 'ingresos',
            '15': 'egresos',
            '18': 'saldo',
        })
        df['mes'] = df['fecha'].str[5:7] + '/' + df['ejercicio'].astype(str)
        df['nro_comprobante'] = df['nro_fondo'].str.zfill(5) + '/' + df['mes'].str[-2:]

        df['fecha'] = pd.to_datetime(
            df['fecha'], format="%Y-%m-%d %H:%M:%S", errors="coerce"
        )

        df = df.loc[:, [
            'ejercicio', 'mes', 'fecha', 'tipo_comprobante', 'nro_comprobante',
            'nro_fondo', 'glosa', 'ingresos', 'egresos', 'saldo'
        ]]

        to_numeric_cols = [
            'ingresos', 'egresos', 'saldo'
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(pd.to_numeric).astype(np.float64) 

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
            rfondo07tp = Rfondo07tp(siif=connect_siif)
            await rfondo07tp.go_to_reports()
            await rfondo07tp.go_to_specific_report()
            for ejercicio in args.ejercicios:
                if args.download:
                    await rfondo07tp.download_report(
                        ejercicio=str(ejercicio), 
                        tipo_comprobante=str(args.tipo_comprobante)
                    )
                    await rfondo07tp.save_xls_file(
                        save_path=save_path,
                        file_name=str(ejercicio)
                        + "-rfondo07tp ("
                        + str(args.tipo_comprobante)
                        + ".xls",
                    )
                await rfondo07tp.read_xls_file(args.file)
                print(rfondo07tp.df)
                await rfondo07tp.process_dataframe()
                print(rfondo07tp.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")
        finally:
            await rfondo07tp.logout()


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.rpa03g -d
