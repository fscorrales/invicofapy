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
from pywinauto import findwindows, keyboard, mouse

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
                ).wait("exists")

                int_ejercicio = int(ejercicio)
                if int_ejercicio > 2010 and int_ejercicio <= dt.datetime.now().year:
                    # Fecha Desde
                    ## Click on año desde
                    time.sleep(1)
                    mouse.click(coords=(495, 205))
                    keyboard.send_keys(ejercicio)
                    ## Click on mes desde
                    time.sleep(1)
                    mouse.click(coords=(470, 205))
                    keyboard.send_keys("01")
                    ## Click on día desde
                    time.sleep(1)
                    mouse.click(coords=(455, 205))
                    keyboard.send_keys("01")

                    # Fecha Hasta
                    fecha_hasta = dt.datetime(year=(int_ejercicio), month=12, day=31)
                    fecha_hasta = min(fecha_hasta, dt.datetime.now())
                    fecha_hasta = dt.datetime.strftime(fecha_hasta, "%d/%m/%Y")
                    ## Click on año hasta
                    time.sleep(1)
                    mouse.click(coords=(610, 205))
                    keyboard.send_keys(ejercicio)
                    ## Click on mes hasta
                    time.sleep(1)
                    mouse.click(coords=(590, 205))
                    keyboard.send_keys(fecha_hasta[3:5])
                    ## Click on día hasta
                    time.sleep(1)
                    mouse.click(coords=(575, 205))
                    keyboard.send_keys(fecha_hasta[0:2])

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
                    ).wait("exists enabled visible ready")
                    btn_accept.click()
                    time.sleep(5)
                    export_dlg_handles = findwindows.find_windows(title="Exportar")
                    if export_dlg_handles:
                        export_dlg = self.sscc.app.window_(handle=export_dlg_handles[0])

                    btn_escritorio = export_dlg.child_window(
                        title="Escritorio", control_type="TreeItem", found_index=1
                    ).wrapper_object()
                    btn_escritorio.click_input()

                    cmb_tipo = export_dlg.child_window(
                        title="Tipo:",
                        auto_id="FileTypeControlHost",
                        control_type="ComboBox",
                    ).wrapper_object()
                    cmb_tipo.type_keys("%{DOWN}")
                    cmb_tipo.select("Archivo ASCII separado por comas (*.csv)")

                    cmb_nombre = export_dlg.child_window(
                        title="Nombre:",
                        auto_id="FileNameControlHost",
                        control_type="ComboBox",
                    ).wrapper_object()
                    cmb_nombre.click_input()
                    report_name = (
                        ejercicio + " - Bancos - Consulta General de Movimientos.csv"
                    )
                    cmb_nombre.type_keys(report_name, with_spaces=True)
                    btn_guardar = export_dlg.child_window(
                        title="Guardar", auto_id="1", control_type="Button"
                    ).wrapper_object()
                    btn_guardar.click()

                    # self.sscc.main.wait("active", timeout=120)

                    dlg_consulta_gral_mov = self.sscc.main.child_window(
                        title="Consulta General de Movimientos", control_type="Window"
                    ).wait("active", timeout=60)

                    # Cerrar ventana
                    keyboard.send_keys("{F10}")

                    # Move file to destination
                    time.sleep(2)
                    self.move_report(dir_path, report_name)

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
                        dir_path=save_path,
                        ejercicios=str(ejercicio),
                        origenes=args.origenes,
                    )
                if args.file:
                    filename = args.file
                else:
                    filename = (
                        str(ejercicio)
                        + " - Bancos - Consulta General de Movimientos.csv"
                    )
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
