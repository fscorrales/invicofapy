#!/usr/bin/env python3
"""

Author : Fernando Corrales <fscpython@gmail.com>

Date   : 02-may-2025

Purpose: Connect to SGF
"""

__all__ = [
    "ConnectSGF",
    "login",
    "logout",
    "SGFReportManager",
]

import argparse
import asyncio
import datetime as dt
import io
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import pandas as pd

from pywinauto import keyboard, WindowSpecification
from pywinauto.application import Application
from pywinauto.timings import TimeoutError

# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Connect to SGF",
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

    args = parser.parse_args()

    if args.username is None or args.password is None:
        from ...config import settings

        args.username = settings.SGF_USERNAME
        args.password = settings.SGF_PASSWORD
        if args.username is None or args.password is None:
            parser.error("Both --username and --password are required.")

    return args


# --------------------------------------------------
@dataclass
class ConnectSGF:
    app: Application
    main: WindowSpecification


# --------------------------------------------------
def login(
    username: str, password: str) -> ConnectSGF:
    """Login to SGF"""

    app_path = r"\\ipvfiles\SISTEMAS\Pagos\Pagos.exe"
    app = Application(backend='uia').start(app_path)
    try:
        time.sleep(3)
        main = app.window(title_re=".*Sistema de Gestión Financiera.*")
        if not main.is_maximized():
            main.maximize()
        cmb_user = main.child_window(
            auto_id="1", control_type="ComboBox", found_index = 0
        ).wait('exists enabled visible ready')
        input_password = main.child_window(
            auto_id="2", control_type="Edit", found_index = 0
        ).wrapper_object()
        cmb_user.type_keys(username)
        input_password.type_keys(password)
        btn_accept = main.child_window(
            title="Aceptar", auto_id="4", control_type="Button"
        ).wrapper_object()
        btn_accept.click()
        main.child_window(
        title="La contraseña no es válida. Vuelva a intentarlo", 
        auto_id="65535", control_type="Text").wait_not('exists visible enabled', timeout=1)
    except TimeoutError:
        print("No se pudo conectar al SGF. Verifique sus credenciales")
        close_button = main.child_window(title="Cerrar", control_type="Button", found_index=0)
        close_button.click()
        btn_cancel = main.child_window(
            title="Cancelar", auto_id="3", control_type="Button"
        ).wrapper_object()
        btn_cancel.click()
    # except Exception as e:
    #     print(f"Ocurrió un error: {e}, {type(e)}")
    #     self.quit()

    return ConnectSGF(
        app = app, main = main
    )

    # try:
    #     "Open SIIF webpage"
    #     await page.goto("https://siif.cgpc.gob.ar/mainSiif/faces/login.jspx")
    #     "Login with credentials"
    #     await page.locator("id=pt1:it1::content").fill(username)
    #     await page.locator("id=pt1:it2::content").fill(password)
    #     btn_connect = page.locator("id=pt1:cb1")
    #     await btn_connect.click()
    #     await page.wait_for_load_state("networkidle")
    # except Exception as e:
    #     print(f"Ocurrio un error: {e}")

    # return ConnectSIIF(
    #     browser=browser, context=context, home_page=page, reports_page=None
    # )


# --------------------------------------------------
def read_xls_file(file_path: Path) -> pd.DataFrame:
    """Read xls file"""
    try:
        with open(file_path, "rb") as f:
            file_bytes = f.read()
        # Convertir a DataFrame en memoria
        df = pd.read_excel(
            io.BytesIO(file_bytes),
            index_col=None,
            header=None,
            na_filter=False,
            dtype=str,
        )
        df.columns = [str(x) for x in range(df.shape[1])]
        return df

    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return None


# --------------------------------------------------
def logout() -> None:
    keyboard.send_keys('%s')

# --------------------------------------------------
@dataclass
class SGFReportManager(ABC):
    sgf: ConnectSGF = None
    df: pd.DataFrame = None
    clean_df: pd.DataFrame = None

    # --------------------------------------------------
    def login(
        self, username: str, password: str
    ) -> ConnectSGF:
        self.sgf = login(
            username=username,
            password=password,
        )
        return self.sgf

    # --------------------------------------------------
    # async def go_to_reports(self) -> None:
    #     await go_to_reports(connect=self.siif)

    # --------------------------------------------------
    @abstractmethod
    def go_to_specific_report(self) -> None:
        """Go to specific report"""
        pass

    # --------------------------------------------------
    @abstractmethod
    def process_dataframe(self, dataframe: pd.DataFrame = None) -> pd.DataFrame:
        """ "Transform read xls file"""
        pass

    # --------------------------------------------------
    def logout(self) -> None:
        logout()


# --------------------------------------------------
def main():
    """Make a jazz noise here"""

    args = get_args()

    connect_sgf= login(args.username, args.password)
    logout()


# --------------------------------------------------
if __name__ == "__main__":
    # asyncio.run(main())
    main()
    # From /invicofapy
    # poetry run python -m src.sgf.handlers.connect_sgf
