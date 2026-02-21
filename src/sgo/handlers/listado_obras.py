#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 21-feb-2026
Purpose: Read, process and write SGO's Listado de Obras report
"""

__all__ = ["ListadoObras"]

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
)
from ..repositories import ListadoObrasRepository
from ..schemas import ListadoObrasReport
from .connect_sgo import (
    ReportCategory,
    SGOReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SGO's Listado de Obras report",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-u",
        "--username",
        help="Username for SGO access",
        metavar="username",
        type=str,
        default=None,
    )

    parser.add_argument(
        "-p",
        "--password",
        help="Password for SGO access",
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
        help="Ejercicios to download from SGV",
    )

    parser.add_argument(
        "-d", "--download", help="Download report from SGV", action="store_true"
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
        help="SGO's Listado de Obras.xls report. Must be in the same folder",
    )

    args = parser.parse_args()

    if args.username is None or args.password is None:
        from ...config import settings

        args.username = settings.SGO_USERNAME
        args.password = settings.SGO_PASSWORD
        if args.username is None or args.password is None:
            parser.error("Both --username and --password are required.")

    if args.file and args.download:
        parser.error("You cannot use --file and --download together. Choose one.")

    return args


# --------------------------------------------------
class ListadoObras(SGOReportManager):
    # --------------------------------------------------
    async def download_and_process_report(
        self, ejercicio: int = dt.datetime.now().year, mes: int = 12
    ) -> pd.DataFrame:
        """Download and process the Listado de Obras report for a specific year."""
        try:
            await self.go_to_specific_report()
            self.download = await self.download_report(
                ejercicio=str(ejercicio), mes=str(mes)
            )
            if self.download is None:
                raise ValueError("No se pudo descargar el reporte Listado de Obras.")
            await self.read_xls_file()
            return await self.process_dataframe()
        except Exception as e:
            print(f"Error al descargar y procesar el reporte: {e}")

    # --------------------------------------------------
    async def download_and_sync_validated_to_repository(
        self, ejercicio: int = dt.datetime.now().year, mes: int = 12
    ) -> RouteReturnSchema:
        """Download, process and sync the Listado de Obras report to the repository."""
        try:
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=await self.download_and_process_report(
                    ejercicio=ejercicio, mes=mes
                ),
                model=ListadoObrasReport,
                field_id="cod_barrio",
            )
            return await sync_validated_to_repository(
                repository=ListadoObrasRepository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": ejercicio},
                title=f"SGO Listado de Obras Report del {ejercicio}",
                logger=logger,
                label=f"Ejercicio {ejercicio} del SGO Listado de Obras Report",
            )
        except Exception as e:
            print(f"Error al descargar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def sync_validated_sqlite_to_repository(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        """Download, process and sync the Listado de Obras report to the repository."""
        try:
            df = get_df_from_sql_table(sqlite_path, table="saldo_barrio_variacion")
            df.drop(columns=["id"], inplace=True)
            df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
            df = df.loc[df["ejercicio"] < 2024]

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=ListadoObrasReport,
                field_id="cod_barrio",
            )

            return await sync_validated_to_repository(
                repository=ListadoObrasRepository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": {"$lt": 2024}},
                title="Sync SGO Listado de Obras Report from SQLite",
                logger=logger,
                label="Sync SGO Listado de Obras Report from SQLite",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def go_to_specific_report(self) -> None:
        await self.go_to_report(report=ReportCategory.SaldosBarriosEvolucion)
        # await self.select_report_module(module=ReportCategory.Gastos)
        # await self.select_specific_report_by_id(report_id="38")

    # --------------------------------------------------
    async def download_report(
        self, ejercicio: str = str(dt.datetime.now().year), mes: str = "12"
    ) -> Download:
        try:
            self.download = None
            # Getting DOM elements
            (
                input_ejercicio,
                input_mes,
            ) = await self.sgo.reports_page.locator(
                "//table[@class='tablaFiltros']//input"
            ).all()
            # input_ejercicio = self.sgv.reports_page.locator(
            #     "//input[@id='ctl00_ContentPlacePrincipal_ucInformeEvolucionDeSaldosPorBarrio_txtAño_TextBox1']"
            # )

            await input_mes.clear()
            await input_mes.fill(str(mes))
            await input_ejercicio.clear()
            await input_ejercicio.fill(str(ejercicio))
            await input_ejercicio.press("Enter")

            await self.sgo.reports_page.wait_for_load_state("networkidle")
            btn_export = self.sgo.reports_page.locator(
                "//a[@id='ctl00_ContentPlacePrincipal_ucInformeEvolucionDeSaldosPorBarrio_rpInformeEvoSaldosPorBarrio_ctl05_ctl04_ctl00_ButtonLink']"
            )
            await btn_export.click()

            await self.sgo.reports_page.wait_for_load_state("networkidle")
            btn_to_excel = self.sgo.reports_page.locator("//a[@title='Excel']")
            async with self.sgo.context.expect_page() as popup_info:
                async with self.sgo.reports_page.expect_download() as download_info:
                    await btn_to_excel.click()  # Se abre el popup aquí

            popup_page = await popup_info.value  # Obtener la ventana emergente
            self.download = await download_info.value  # Obtener el archivo descargado

            # Cerrar la ventana emergente (si realmente se abrió)
            if popup_page:
                await popup_page.close()

            # await self.go_back_to_reports_list()

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

        df = df.iloc[4:, :]

        df = df.rename(
            {
                "0": "cod_obra",
                "1": "mes_basico_obra",
                "2": "cod_contrato",
                "3": "obra",
                "4": "mes_basico_contrato",
                "5": "tipo_obra",
                "6": "localidad",
                "7": "contratista",
                "8": "activa",
                "9": "monto",
                "10": "monto_total",
                "11": "representante",
                "12": "operatoria",
                "13": "q_rubros",
                "14": "id_inspector",
                "15": "iniciador",
                "16": "estado",
                "17": "fecha_inicio",
                "18": "fecha_contrato",
                "19": "borrar",
                "20": "fecha_fin",
                "21": "plazo_est_dias",
                "22": "fecha_fin_est",
                "23": "plazo_ampl_est_dias",
                "24": "fecha_fin_ampl_est",
                "25": "avance_fis_real",
                "26": "avance_fciero_real",
                "27": "avance_fis_est",
                "28": "avance_fciero_est",
                "29": "monto_certificado",
                "30": "monto_certificado_obra",
                "31": "monto_pagado",
                "32": "borrar2",
                "33": "nro_ultimo_certif",
                "34": "nro_ultimo_certif_bc",
                "35": "mes_obra_certif",
                "36": "año_obra_certif",
                "37": "fecha_ultimo_certif",
                "38": "anticipo_acum",
                "39": "cant_anticipo",
                "40": "porc_anticipo",
                "41": "cant_certif_anticipo",
                "42": "fdo_reparo_acum",
                "43": "desc_fdo_reparo_acum",
                "44": "monto_redeterminado",
                "45": "nro_ultima_rederterminacion",
                "46": "mes_ultimo_basico",
                "47": "año_ultimo_basico",
                "48": "nro_ultima_medicion",
                "49": "mes_ultima_medicion",
                "50": "año_ultima_medicion",
            },
            axis="columns",
        )

        df[["mes_basico_obra", "año_basico_obra"]] = df["mes_basico_obra"].str.split(
            pat="/", n=1, expand=True
        )
        df["mes_basico_obra"] = df["mes_basico_obra"].astype(str)
        df["año_basico_obra"] = df["año_basico_obra"].astype(str)
        df["mes_basico_obra"] = (
            df["mes_basico_obra"].str.zfill(2) + "/" + df["año_basico_obra"]
        )
        df[["mes_basico_contrato", "año_basico_contrato"]] = df[
            "mes_basico_contrato"
        ].str.split(pat="/", n=1, expand=True)
        df["mes_basico_contrato"] = df["mes_basico_contrato"].astype(str)
        df["año_basico_contrato"] = df["año_basico_contrato"].astype(str)
        df["mes_basico_contrato"] = (
            df["mes_basico_contrato"].str.zfill(2) + "/" + df["año_basico_contrato"]
        )

        df["mes_obra_certif"] = np.where(
            df["mes_obra_certif"] != "",
            df["mes_obra_certif"].str.zfill(2) + "/" + df["año_obra_certif"],
            "",
        )
        df["mes_ultimo_basico"] = np.where(
            df["mes_ultimo_basico"] != "",
            df["mes_ultimo_basico"].str.zfill(2) + "/" + df["año_ultimo_basico"],
            "",
        )
        df["mes_ultima_medicion"] = np.where(
            df["mes_ultima_medicion"] != "",
            df["mes_ultima_medicion"].str.zfill(2) + "/" + df["año_ultima_medicion"],
            "",
        )

        df = df.drop(
            [
                "activa",
                "borrar",
                "borrar2",
                "año_obra_certif",
                "año_ultimo_basico",
                "año_ultima_medicion",
                "año_basico_obra",
                "año_basico_contrato",
            ],
            axis=1,
        )

        # Formateamos columnas
        to_numeric_cols = [
            "monto",
            "monto_total",
            "avance_fis_real",
            "avance_fciero_real",
            "avance_fis_est",
            "avance_fciero_est",
            "monto_certificado",
            "monto_certificado_obra",
            "monto_pagado",
            "anticipo_acum",
            "porc_anticipo",
            "fdo_reparo_acum",
            "desc_fdo_reparo_acum",
            "monto_redeterminado",
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(pd.to_numeric)
        to_numeric_cols = [
            "avance_fis_real",
            "avance_fciero_real",
            "avance_fis_est",
            "avance_fciero_est",
            "anticipo_acum",
            "porc_anticipo",
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(lambda x: x.round(4))

        # to_integer_cols= [
        #     'id_inspector'
        #     # 'q_rubros', 'id_inspector',
        #     # # 'plazo_est_dias', 'plazo_ampl_est_dias',
        #     # # 'nro_ultimo_certif', 'nro_ultimo_certif_bc',
        #     # # 'cant_anticipo', 'cant_certif_anticipo',
        #     # # 'nro_ultima_rederterminacion', 'nro_ultima_medicion',
        # ]
        # df[to_integer_cols] = df[to_integer_cols].astype(int)
        # for col in to_integer_cols:
        #     df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        to_date_cols = [
            "fecha_inicio",
            "fecha_fin",
            "fecha_contrato",
            "fecha_fin_est",
            "fecha_fin_ampl_est",
            "fecha_ultimo_certif",
        ]
        df[to_date_cols] = df[to_date_cols].apply(pd.to_datetime)

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
        connect_sgo = await login(
            args.username, args.password, playwright=p, headless=False
        )
        try:
            sgo = ListadoObras(sgo=connect_sgo)
            await sgo.go_to_specific_report()
            for ejercicio in args.ejercicios:
                if args.download:
                    await sgo.download_report(ejercicio=str(ejercicio))
                    await sgo.save_xls_file(
                        save_path=save_path,
                        file_name=str(ejercicio)
                        + "-InformeEvolucionDeSaldosPorBarrio.xls",
                    )
                await sgo.read_xls_file(args.file)
                print(sgo.df)
                await sgo.process_dataframe()
                print(sgo.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")
        finally:
            await sgo.logout()


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.sgo.handlers.listado_obras -d -e 2025
