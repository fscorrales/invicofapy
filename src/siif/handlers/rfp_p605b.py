#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 18-jul-2025
Purpose: Read, process and write SIIF's rfp_p605b () report
"""

__all__ = ["RfpP605b"]

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
from ..repositories.rfp_p605b import RfpP605bRepository
from ..schemas.rfp_p605b import RfpP605bReport
from .connect_siif import (
    ReportCategory,
    SIIFReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rfp_p605b",
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
        help="SIIF' rfp_p605b.xls report. Must be in the same folder",
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
class RfpP605b(SIIFReportManager):
    # --------------------------------------------------
    async def download_and_process_report(
        self, ejercicio: int = dt.datetime.now().year
    ) -> pd.DataFrame:
        """Download and process the rfp_p605b report for a specific year."""
        try:
            await self.go_to_specific_report()
            self.download = await self.download_report(ejercicio=str(ejercicio))
            if self.download is None:
                raise ValueError("No se pudo descargar el reporte rfp_p605b.")
            await self.read_xls_file()
            return await self.process_dataframe()
        except Exception as e:
            print(f"Error al descargar y procesar el reporte: {e}")

    # --------------------------------------------------
    async def download_and_sync_validated_to_repository(
        self, ejercicio: int = dt.datetime.now().year
    ) -> RouteReturnSchema:
        """Download, process and sync the rfp_p605b report to the repository."""
        try:
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=await self.download_and_process_report(ejercicio=ejercicio),
                model=RfpP605bReport,
                field_id="estructura",
            )
            return await sync_validated_to_repository(
                repository=RfpP605bRepository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": ejercicio},
                title=f"SIIF rfp_p605b Report del {ejercicio}",
                logger=logger,
                label=f"Ejercicio {ejercicio} del rfp_p605b",
            )
        except Exception as e:
            print(f"Error al descargar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def sync_validated_sqlite_to_repository(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        """Download, process and sync the rfp_p605b report to the repository."""
        try:
            df = get_df_from_sql_table(sqlite_path, table="form_gto_rfp_p605b")
            df.drop(columns=["id"], inplace=True)
            df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
            df = df.loc[df["ejercicio"] < 2024]
            df.rename(
                columns={
                    "desc_prog": "desc_programa",
                    "desc_subprog": "desc_subprograma",
                    "desc_proy": "desc_proyecto",
                    "desc_act": "desc_actividad",
                },
                inplace=True,
            )

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=RfpP605bReport,
                field_id="estructura",
            )

            return await sync_validated_to_repository(
                repository=RfpP605bRepository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": {"$lt": 2024}},
                title="Sync SIIF rfp_p605b Report from SQLite",
                logger=logger,
                label="Sync SIIF rfp_p605b Report from SQLite",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.select_report_module(module=ReportCategory.Formulacion)
        await self.select_specific_report_by_id(report_id="890")

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

        df = df.replace(to_replace="", value=None)
        df["ejercicio"] = pd.to_numeric(df.iloc[13, 1][-4:], errors="coerce")
        df = df.drop(range(22))

        df["programa"] = np.where(
            df["3"].str[0:8] == "Programa", df["3"].str[22:], None
        )
        df["programa"] = df["programa"].ffill()
        df["prog"] = df["programa"].str[:2]
        df["prog"] = df["prog"].str.strip()
        df["desc_prog"] = df["programa"].str[3:]
        df["desc_prog"] = df["desc_prog"].str.strip()
        df["subprograma"] = np.where(
            df["3"].str[0:11] == "SubPrograma", df["3"].str[19:], None
        )
        df["proyecto"] = np.where(
            df["3"].str[0:8] == "Proyecto", df["3"].str[24:], None
        )
        df["actividad"] = np.where(
            df["3"].str[0:9] == "Actividad", df["3"].str[20:], None
        )
        df["grupo"] = np.where(df["10"] != "", df["10"].str[0:3], None)
        df["grupo"] = df["grupo"].ffill()
        df["partida"] = np.where(df["9"] != "", df["9"], None)
        df["fuente_11"] = df["22"]
        df["fuente_10"] = df["19"]
        df = df.loc[
            :,
            [
                "ejercicio",
                "prog",
                "desc_prog",
                "subprograma",
                "proyecto",
                "actividad",
                "grupo",
                "partida",
                "fuente_11",
                "fuente_10",
            ],
        ]
        df = df.dropna(
            subset=["subprograma", "proyecto", "actividad", "partida"], how="all"
        )
        df["subprograma"] = df["subprograma"].ffill()
        df = df.dropna(subset=["proyecto", "actividad", "partida"], how="all")
        df["proyecto"] = df["proyecto"].ffill()
        df = df.dropna(subset=["actividad", "partida"], how="all")
        df["actividad"] = df["actividad"].ffill()
        df = df[df["partida"].str.len() == 3]
        df["sub"] = df["subprograma"].str[:2]
        df["sub"] = df["sub"].str.strip()
        df["desc_subprog"] = df["subprograma"].str[3:]
        df["desc_subprog"] = df["desc_subprog"].str.strip()
        df["proy"] = df["proyecto"].str[:2]
        df["proy"] = df["proy"].str.strip()
        df["desc_proy"] = df["proyecto"].str[3:]
        df["desc_proy"] = df["desc_proy"].str.strip()
        df["act"] = df["actividad"].str[:2]
        df["act"] = df["act"].str.strip()
        df["desc_act"] = df["actividad"].str[3:]
        df["desc_act"] = df["desc_act"].str.strip()
        df["fuente_10"] = df["fuente_10"].astype(float)
        df["fuente_11"] = df["fuente_11"].astype(float)
        df["fuente"] = np.select(
            [
                df["fuente_10"].astype(int) > 0,
                df["fuente_11"].astype(int) > 0,
            ],
            ["10", "11"],
        )
        df["formulado"] = df["fuente_10"] + df["fuente_11"]
        df["prog"] = df["prog"].str.zfill(2)
        df["sub"] = df["sub"].str.zfill(2)
        df["proy"] = df["proy"].str.zfill(2)
        df["act"] = df["act"].str.zfill(2)
        df["estructura"] = (
            df["prog"]
            + "-"
            + df["sub"]
            + "-"
            + df["proy"]
            + "-"
            + df["act"]
            + "-"
            + df["partida"]
        )
        df = df.loc[
            :,
            [
                "ejercicio",
                "estructura",
                "fuente",
                "prog",
                "desc_prog",
                "sub",
                "desc_subprog",
                "proy",
                "desc_proy",
                "act",
                "desc_act",
                "grupo",
                "partida",
                "formulado",
            ],
        ]
        df = df.rename(
            columns={
                "prog": "programa",
                "sub": "subprograma",
                "proy": "proyecto",
                "act": "actividad",
                "desc_prog": "desc_programa",
                "desc_subprog": "desc_subprograma",
                "desc_proy": "desc_proyecto",
                "desc_act": "desc_actividad",
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
                rfp_p605b = RfpP605b(siif=connect_siif)
                await rfp_p605b.go_to_reports()
                await rfp_p605b.go_to_specific_report()
                for ejercicio in args.ejercicios:
                    await rfp_p605b.download_report(ejercicio=str(ejercicio))
                    await rfp_p605b.save_xls_file(
                        save_path=save_path,
                        file_name=str(ejercicio) + "-rfp_p605b.xls",
                    )
                    await rfp_p605b.read_xls_file(args.file)
                    print(rfp_p605b.df)
                    await rfp_p605b.process_dataframe()
                    print(rfp_p605b.clean_df)
                await rfp_p605b.logout()
            else:
                rfp_p605b = RfpP605b()
                await rfp_p605b.read_xls_file(args.file)
                print(rfp_p605b.df)
                await rfp_p605b.process_dataframe()
                print(rfp_p605b.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.rfp_p605b -d
    # poetry run python -m src.siif.handlers.rfp_p605b -f 2025-rfp_p605b.xls
