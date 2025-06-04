#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 04-may-2025
Purpose: Read, process and write SGF's 'Resumen de Rendiciones por Proveedores' report
"""

__all__ = ["ResumenRendProv"]

import argparse
import asyncio
import datetime as dt
import inspect
import os

import numpy as np
import pandas as pd

from .connect_siif import (
    ReportCategory,
    SGFReportManager,
    login,
)
from pywinauto import findwindows, keyboard, mouse
from typing import List, Union
from .schemas.common import Origen

# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SGF's 'Resumen de Rendiciones por Proveedores' report",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-u",
        "--username",
        help="Username for SGF access",
        metavar="username",
        type=str,
        default=None,
    )

    parser.add_argument(
        "-p",
        "--password",
        help="Password for SGF access",
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
        help="Ejercicios to download from SGF",
    )

    parser.add_argument(
        '-o', '--origenes', 
        metavar = 'Origenes',
        default = [c.value for c in Origen],
        type=str,
        nargs="+",
        choices=[c.value for c in Origen],
        help = "Origenes to download from SGF")

    parser.add_argument(
        "-d", "--download", help="Download report from SGF", action="store_true"
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="csv_file",
        default=None,
        type=argparse.FileType("r"),
        help="SGF's csv report must be in the same folder",
    )

    args = parser.parse_args()

    if args.username is None or args.password is None:
        from ...config import settings

        args.username = settings.SGF_USERNAME
        args.password = settings.SGF_PASSWORD
        if args.username is None or args.password is None:
            parser.error("Both --username and --password are required.")

    if args.file and args.download:
        parser.error("You cannot use --file and --download together. Choose one.")

    return args


# --------------------------------------------------
class ResumenRendProv(SGFReportManager):
    # # --------------------------------------------------
    # async def go_to_specific_report(self) -> None:
    #     await self.select_report_module(module=ReportCategory.Gastos)
    #     await self.select_specific_report_by_id(report_id="38")

    # --------------------------------------------------
    def download_report(
        self,
        dir_path:str, 
        ejercicios: Union[List, str] = str(dt.datetime.now().year)
        origenes: Union[List, str] = [v.value for v in Origen]
    ) -> None:
        try:
            if not isinstance(origenes, list):
                origenes = [origenes]
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for origen in origenes:
                for ejercicio in ejercicios:
                    # Open menu Consulta General de Movimientos
                    self.sgf.main.menu_select("Informes->Resumen de Rendiciones")

                    dlg_resumen_rend = self.sgf.main.child_window(
                        title="Informes - Resumen de Rendiciones", control_type="Window"
                    ).wait('exists')

                    int_ejercicio = int(ejercicio)
                    if int_ejercicio > 2010 and int_ejercicio <= dt.datetime.now().year:
                        # Origen
                        cmb_origen = self.sgf.main.child_window(auto_id="24", control_type="ComboBox").wrapper_object()
                        cmb_origen.type_keys("%{DOWN}")
                        cmb_origen.type_keys(origen,  with_spaces=True) #EPAM, OBRAS, FUNCIONAMIENTO
                        keyboard.send_keys('{ENTER}')
                        btn_exportar = self.sgf.main.child_window(
                            title="Exportar", auto_id="4", control_type="Button"
                        ).wait('enabled ready active', timeout=60)
                        
                        # Fecha Desde
                        ## Click on año desde
                        time.sleep(1)
                        mouse.click(coords=(205, 415))
                        keyboard.send_keys(ejercicio)
                        ## Click on mes desde
                        time.sleep(1)
                        mouse.click(coords=(185, 415))
                        keyboard.send_keys('01')
                        ## Click on día desde
                        time.sleep(1)
                        mouse.click(coords=(170, 415))
                        keyboard.send_keys('01')

                        # Fecha Hasta
                        fecha_hasta = dt.datetime(year=(int_ejercicio), month=12, day=31)
                        fecha_hasta = min(fecha_hasta, dt.datetime.now())
                        fecha_hasta = dt.datetime.strftime(fecha_hasta, '%d/%m/%Y')
                        ## Click on año hasta
                        time.sleep(1)
                        mouse.click(coords=(495, 415))
                        keyboard.send_keys(ejercicio)
                        ## Click on mes hasta
                        time.sleep(1)
                        mouse.click(coords=(470, 415))
                        keyboard.send_keys(fecha_hasta[3:5])
                        ## Click on día hasta
                        time.sleep(1)
                        mouse.click(coords=(455, 415))
                        keyboard.send_keys(fecha_hasta[0:2])

                        # Exportar
                        btn_exportar.click()
                        btn_accept = self.sgf.main.child_window(
                            title="Aceptar", auto_id="9", control_type="Button"
                        ).wait('exists enabled visible ready', timeout=360)
                        btn_accept.click()
                        time.sleep(5)
                        export_dlg_handles = findwindows.find_windows(title='Exportar')
                        if export_dlg_handles:
                            export_dlg = self.sgf.app.window_(handle=export_dlg_handles[0])

                        btn_escritorio = export_dlg.child_window(
                            title="Escritorio", control_type="TreeItem", found_index=1
                        ).wrapper_object()
                        btn_escritorio.click_input()

                        cmb_tipo = export_dlg.child_window(
                            title="Tipo:", auto_id="FileTypeControlHost", control_type="ComboBox"
                        ).wrapper_object()
                        cmb_tipo.type_keys("%{DOWN}")
                        cmb_tipo.select('Archivo ASCII separado por comas (*.csv)')

                        cmb_nombre = export_dlg.child_window(
                            title="Nombre:", auto_id="FileNameControlHost", control_type="ComboBox"
                        ).wrapper_object()
                        cmb_nombre.click_input()
                        report_name = ejercicio + ' Resumen de Rendiciones '+ origen +'.csv'
                        cmb_nombre.type_keys(report_name,  with_spaces=True)
                        btn_guardar = export_dlg.child_window(
                            title="Guardar", auto_id="1", control_type="Button"
                        ).wrapper_object()
                        btn_guardar.click()

                        # dlg_resumen_rend = self.sgf.main.child_window(
                        #     title="Informes - Resumen de Rendiciones", control_type="Window"
                        # ).wait_not('visible exists', timeout=120)

                        self.sgf.main.wait("active", timeout=120)

                        # Move file to destination
                        time.sleep(2)
                        self.move_report(
                            dir_path, report_name
                        )

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
        df["ejercicio"] = int(df.iloc[5, 2][-4:])
        df = df.tail(-16)
        df = df.loc[
            :,
            [
                "ejercicio",
                "2",
                "3",
                "6",
                "7",
                "8",
                "9",
                "10",
                "13",
                "14",
                "15",
                "16",
                "18",
                "20",
            ],
        ]
        df = df.replace(to_replace="", value=None)
        df = df.dropna(subset=["2"])
        df = df.rename(
            columns={
                "2": "programa",
                "3": "subprograma",
                "6": "proyecto",
                "7": "actividad",
                "8": "partida",
                "9": "fuente",
                "10": "org",
                "13": "credito_original",
                "14": "credito_vigente",
                "15": "comprometido",
                "16": "ordenado",
                "18": "saldo",
                "20": "pendiente",
            }
        )
        df["programa"] = df["programa"].str.zfill(2)
        df["subprograma"] = df["subprograma"].str.zfill(2)
        df["proyecto"] = df["proyecto"].str.zfill(2)
        df["actividad"] = df["actividad"].str.zfill(2)
        df["grupo"] = df["partida"].str[0] + "00"
        df["estructura"] = (
            df["programa"]
            + "-"
            + df["subprograma"]
            + "-"
            + df["proyecto"]
            + "-"
            + df["actividad"]
            + "-"
            + df["partida"]
        )
        df = df.loc[
            :,
            [
                "ejercicio",
                "estructura",
                "fuente",
                "programa",
                "subprograma",
                "proyecto",
                "actividad",
                "grupo",
                "partida",
                "org",
                "credito_original",
                "credito_vigente",
                "comprometido",
                "ordenado",
                "saldo",
                "pendiente",
            ],
        ]
        to_numeric_cols = [
            "credito_original",
            "credito_vigente",
            "comprometido",
            "ordenado",
            "saldo",
            "pendiente",
        ]
        df[to_numeric_cols] = (
            df[to_numeric_cols].apply(pd.to_numeric).astype(np.float64)
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


    connect_sgf = login(
        args.username, args.password
    )
    try:
        resumen_rend_prov = ResumenRendProv(sgf=connect_sgf)
        for ejercicio in args.ejercicios:
            resumen_rend_prov.download_report(
                dir_path=save_path,
                ejercicio=str(ejercicio)
                origenes=args.origenes
            )
            resumen_rend_prov.read_csv_file()
            print(resumen_rend_prov.df)
            resumen_rend_prov.process_dataframe()
            print(resumen_rend_prov.clean_df)
    except Exception as e:
        print(f"Error al iniciar sesión: {e}")
    finally:
        resumen_rend_prov.logout()


# --------------------------------------------------
if __name__ == "__main__":
    main()
    # From /invicofapy

    # poetry run python -m src.sgf.handlers.resumen_rend_prov
