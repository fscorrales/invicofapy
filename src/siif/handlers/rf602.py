#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 14-feb-2025
Purpose: Read, process and write SIIF's rf602 (Prespuesto de Gastos por Fuente) report
"""

__all__ = ["Rf602"]

import argparse
import asyncio
import datetime as dt
import inspect
import os

import numpy as np
import pandas as pd
from playwright.async_api import Download, async_playwright

from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rf602",
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
class Rf602(SIIFReportManager):
    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.select_report_module(module=ReportCategory.Gastos)
        await self.select_specific_report_by_id(report_id="38")

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
        df['origen'] = df['6'].str.split('-', n = 1).str[0]
        df['origen'] = df['origen'].str.split('=', n = 1).str[1]
        df['origen'] = df['origen'].str.replace('"','')
        df['origen'] = df['origen'].str.strip()
        
        if df.loc[0, 'origen'] == 'OBRAS':
            df = df.rename(columns = {
                '23':'beneficiario',
                '25':'libramiento_sgf',
                '26':'fecha',
                '27':'movimiento',
                '24':'cta_cte',
                '28':'importe_bruto',
                '29':'gcias',
                '30':'sellos',
                '31':'iibb',
                '32':'suss',
                '33':'invico',
                '34':'otras',
                '35':'importe_neto',
            })
            df['destino'] = ''
            df['seguro'] = '0'
            df['salud'] = '0'
            df['mutual'] = '0'
        else:
            df = df.rename(columns = {
                '26':'beneficiario',
                '27':'destino',
                '29':'libramiento_sgf',
                '30':'fecha',
                '31':'movimiento',
                '28':'cta_cte',
                '32':'importe_bruto',
                '33':'gcias',
                '34':'sellos',
                '35':'iibb',
                '36':'suss',
                '37':'invico',
                '38':'seguro',
                '39':'salud',
                '40':'mutual',
                '41':'importe_neto',
            })
            df['otras'] = '0'

        df['ejercicio'] = df['fecha'].str[-4:]
        df['mes'] = df['fecha'].str[3:5] + '/' + df['ejercicio']
        df['cta_cte'] = np.where(
            df['beneficiario'] == 'CREDITO ESPECIAL',
            '130832-07', 
            df['cta_cte']
        )

        df = df.loc[:, [
            'origen', 'ejercicio', 'mes', 'fecha', 
            'beneficiario', 'destino', 'libramiento_sgf',
            'movimiento', 'cta_cte', 'importe_bruto', 'gcias', 'sellos',
            'iibb', 'suss', 'invico', 'seguro', 'salud', 'mutual', 'otras',
            'importe_neto'
        ]]
        
        df.loc[:, 'importe_bruto':] = df.loc[:, 'importe_bruto':].apply(
            lambda x: x.str.replace(',', '').astype(float)
        )
        # df.loc[:,'importe_bruto':] = df.loc[:,'importe_bruto':].stack(
        # ).str.replace(',','').unstack()
        # df.loc[:,'importe_bruto':] = df.loc[:,'importe_bruto':].stack(
        # ).astype(float).unstack()
        df['retenciones'] = df.loc[:,'gcias':'otras'].sum(axis=1)

        df['importe_bruto'] = np.where(
            df['origen'] == 'EPAM', 
            df['importe_bruto'] + df['invico'],
            df['importe_bruto']
        )
        # Reubica la columna 'retenciones' antes de la columna 'importe_neto'
        columns = df.columns.tolist()

        # Reordena las columnas para que 'retenciones' esté antes de 'importe_neto'
        new_columns = [column for column in columns if column != 'retenciones'] + ['retenciones'] + [column for column in columns if column == 'importe_neto']

        # Reindexa el DataFrame con las nuevas columnas
        df = df.reindex(columns=new_columns, copy=False)
        
        df['ejercicio'] = df['fecha'].str[-4:]
        df['mes'] = df['fecha'].str[3:5] + '/' + df['ejercicio']
        df['cta_cte'] = np.where(
            df['beneficiario'] == 'CREDITO ESPECIAL',
            '130832-07', 
            df['cta_cte']
        )

        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%d/%m/%Y'
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

    async with async_playwright() as p:
        connect_siif = await login(
            args.username, args.password, playwright=p, headless=False
        )
        try:
            rf602 = Rf602(siif=connect_siif)
            await rf602.go_to_reports()
            await rf602.go_to_specific_report()
            for ejercicio in args.ejercicios:
                await rf602.download_report(ejercicio=str(ejercicio))
                await rf602.save_xls_file(
                    save_path=save_path,
                    file_name=str(ejercicio) + "-rf602.xls",
                )
                await rf602.read_xls_file()
                print(rf602.df)
                await rf602.process_dataframe()
                print(rf602.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")
        finally:
            await rf602.logout()


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy
    # .venv/Scripts/python src/siif/services/rf602.py

    # poetry run python -m src.siif.handlers.rf602
