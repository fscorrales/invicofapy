#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 11-jul-2025
Purpose: Read, process and write SSCC's 'Banco INVICO' report
"""

__all__ = ["BancoINVICO"]

import argparse
import datetime as dt
import inspect
import os
import time
from pathlib import Path
from typing import List, Union

import numpy as np
import pandas as pd
from pywinauto import keyboard

from ...config import logger
from ...utils import (
    RouteReturnSchema,
    get_df_from_sql_table,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..repositories.banco_invico import BancoINVICORepository
from ..schemas.banco_invico import BancoINVICOReport
from .connect_sscc import (
    SSCCReportManager,
    login,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SSCC's 'Banco INVICO' report",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-u",
        "--username",
        help="Username for SSCC access",
        metavar="username",
        type=str,
        default=None,
    )

    parser.add_argument(
        "-p",
        "--password",
        help="Password for SSCC access",
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
        help="Ejercicios to download from SSCC",
    )

    parser.add_argument(
        "-d", "--download", help="Download report from SSCC", action="store_true"
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="csv_file",
        default=None,
        type=argparse.FileType("r"),
        help="SSCC's csv report must be in the same folder",
    )

    args = parser.parse_args()

    if args.username is None or args.password is None:
        from ...config import settings

        args.username = settings.SSCC_USERNAME
        args.password = settings.SSCC_PASSWORD
        if args.username is None or args.password is None:
            parser.error("Both --username and --password are required.")

    if args.file and args.download:
        parser.error("You cannot use --file and --download together. Choose one.")

    return args


# --------------------------------------------------
class BancoINVICO(SSCCReportManager):
    # # --------------------------------------------------
    # async def go_to_specific_report(self) -> None:
    #     await self.select_report_module(module=ReportCategory.Gastos)
    #     await self.select_specific_report_by_id(report_id="38")

    # --------------------------------------------------
    async def sync_validated_sqlite_to_repository(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        """Download, process and sync the SSCC Banco INVICO report to the repository."""
        try:
            df = get_df_from_sql_table(sqlite_path, table="banco_invico")
            df.drop(columns=["id"], inplace=True)
            df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
            df = df.loc[df["ejercicio"] < 2024]

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=BancoINVICOReport,
                field_id="cod_imputacion",
            )

            return await sync_validated_to_repository(
                repository=BancoINVICORepository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": {"$lt": 2024}},
                title="Sync SSCC Banco INVICO Report from SQLite",
                logger=logger,
                label="Sync SSCC Banco INVICO Report from SQLite",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    def download_report(
        self,
        dir_path: Path,
        ejercicios: Union[List, str] = str(dt.datetime.now().year),
    ) -> None:
        try:
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                # Open menu Consulta General de Movimientos
                self.sscc.main.menu_select("Informes->Consulta General de Movimientos")

                dlg_consulta_gral_mov = self.sscc.main.child_window(
                    title="Consulta General de Movimientos (Vista No Actualizada)",
                    control_type="Window",
                )
                dlg_consulta_gral_mov.wait("exists")

                int_ejercicio = int(ejercicio)
                if int_ejercicio > 2010 and int_ejercicio <= dt.datetime.now().year:
                    campo_desde = dlg_consulta_gral_mov.child_window(
                        control_type="Pane", found_index=1
                    )
                    campo_hasta = dlg_consulta_gral_mov.child_window(
                        control_type="Pane", found_index=2
                    )
                    # Fecha Desde
                    campo_desde.click_input()  # Hace foco y cae en el AÑO por defecto
                    time.sleep(0.5)
                    keyboard.send_keys(ejercicio)  # Escribe el año (ej. 2026)

                    keyboard.send_keys("{LEFT}")  # Se mueve al MES
                    time.sleep(0.5)
                    keyboard.send_keys("01")  # Escribe el mes

                    keyboard.send_keys("{LEFT}")  # Se mueve al DÍA
                    time.sleep(0.5)
                    keyboard.send_keys("01")  # Escribe el día

                    # Fecha Hasta
                    fecha_hasta = dt.datetime(year=(int_ejercicio), month=12, day=31)
                    fecha_hasta = min(fecha_hasta, dt.datetime.now())
                    str_dia = fecha_hasta.strftime("%d")
                    str_mes = fecha_hasta.strftime("%m")
                    str_anio = fecha_hasta.strftime("%Y")

                    campo_hasta.click_input()  # Hace foco y cae en el AÑO por defecto
                    time.sleep(0.5)
                    keyboard.send_keys(str_anio)  # Escribe el año

                    keyboard.send_keys("{LEFT}")  # Se mueve al MES
                    time.sleep(0.5)
                    keyboard.send_keys(str_mes)  # Escribe el mes

                    keyboard.send_keys("{LEFT}")  # Se mueve al DÍA
                    time.sleep(0.5)
                    keyboard.send_keys(str_dia)  # Escribe el día

                    time.sleep(0.5)

                    # Actualizar
                    time.sleep(1)
                    keyboard.send_keys("{F5}")
                    vertical_scroll = self.sscc.main.child_window(
                        title="Vertical",
                        auto_id="NonClientVerticalScrollBar",
                        control_type="ScrollBar",
                        found_index=0,
                    ).wait("exists enabled visible ready", timeout=120)

                    # Exportar
                    keyboard.send_keys("{F7}")
                    btn_accept = self.sscc.main.child_window(
                        title="Aceptar", auto_id="9", control_type="Button"
                    )
                    btn_accept.wait("exists enabled visible ready")
                    btn_accept.click()
                    time.sleep(5)

                    # Armamos el nombre del reporte y su ruta absoluta temporal
                    report_name = f"{ejercicio}-bancoINVICO.csv"
                    # Forzamos a que se guarde directo en tu Escritorio de forma absoluta
                    temp_file_path = os.path.join(dir_path, report_name)

                    # Como la ventana "Exportar" tiene el foco y el cursor está en el campo Nombre:
                    # Borramos lo que haya y mandamos la ruta completa con el teclado del sistema
                    keyboard.send_keys("^a{BACKSPACE}")
                    time.sleep(0.5)
                    keyboard.send_keys(temp_file_path, with_spaces=True)
                    time.sleep(1)

                    # En lugar de buscar el botón Guardar por código, presionamos ENTER.
                    # En las ventanas de diálogo de Windows, ENTER ejecuta la acción principal (Guardar).
                    keyboard.send_keys("{ENTER}")
                    time.sleep(2)

                    dlg_consulta_gral_mov = self.sscc.main.child_window(
                        title="Consulta General de Movimientos", control_type="Window"
                    )
                    dlg_consulta_gral_mov.wait("active", timeout=60)

                    # Cerrar ventana
                    keyboard.send_keys("{F10}")

        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.logout()

    # --------------------------------------------------
    def process_dataframe(self, dataframe: pd.DataFrame = None) -> pd.DataFrame:
        """ "Transform read xls file"""
        if dataframe is None:
            df = self.df.copy()
        else:
            df = dataframe.copy()
        df = df.replace(to_replace="[\r\n]", value="")
        df["21"] = df["21"].str.strip()
        df = df.assign(
            fecha=df["20"],
            ejercicio=df["20"].str[-4:],
            mes=df["20"].str[3:5] + "/" + df["20"].str[-4:],
            cta_cte=df["22"],
            movimiento=df["21"],
            es_cheque=np.where(
                (df["21"] == "DEBITO") | (df["21"] == "DEPOSITO"), False, True
            ),
            concepto=df["23"],
            beneficiario=df["24"],
            moneda=df["25"],
            libramiento=df["26"],
            imputacion=df["27"],
            importe=df["28"].str.replace(",", "").astype(float),
        )
        df[["cod_imputacion", "imputacion"]] = df["imputacion"].str.split(
            pat="-", n=1, expand=True
        )
        df = df.loc[
            :,
            [
                "ejercicio",
                "mes",
                "fecha",
                "cta_cte",
                "movimiento",
                "es_cheque",
                "beneficiario",
                "importe",
                "concepto",
                "moneda",
                "libramiento",
                "cod_imputacion",
                "imputacion",
            ],
        ]

        df["fecha"] = pd.to_datetime(df["fecha"], format="%d/%m/%Y")
        df["fecha"] = df["fecha"].apply(
            lambda x: x.to_pydatetime() if pd.notnull(x) else None
        )

        self.clean_df = df
        return self.clean_df


# --------------------------------------------------
def main():
    """Make a jazz noise here"""

    args = get_args()

    save_path = os.path.dirname(
        os.path.abspath(inspect.getfile(inspect.currentframe()))
    )

    # connect_sscc = login(args.username, args.password)
    with login(args.username, args.password) as conn:
        try:
            banco_invico = BancoINVICO(sscc=conn)
            for ejercicio in args.ejercicios:
                if args.download:
                    banco_invico.download_report(
                        dir_path=save_path, ejercicios=str(ejercicio)
                    )
                if args.file:
                    filename = args.file
                else:
                    filename = str(ejercicio) + "-bancoINVICO.csv"
                banco_invico.read_csv_file(Path(os.path.join(save_path, filename)))
                print(banco_invico.df)
                banco_invico.process_dataframe()
                print(banco_invico.clean_df)
        except Exception as e:
            print(f"Error al iniciar sesión: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    main()
    # From /invicofapy

    # poetry run python -m src.sscc.handlers.banco_invico -d
