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
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from pywinauto import WindowSpecification, keyboard
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
def logout(window: WindowSpecification = None) -> None:
    try:
        if window:
            print("Activando ventana antes de logout")
            window.set_focus()  # Asegura que la ventana tenga foco
            time.sleep(1)  # Espera breve para evitar errores de timing

        print("Enviando Alt+S")
        keyboard.send_keys("%s")  # Alt+S o el atajo que uses para salir
        time.sleep(1)
    except Exception as e:
        print(f"Error en logout: {e}")


# --------------------------------------------------
@dataclass
class ConnectSGF:
    app: Application
    main: WindowSpecification

    # --------------------------------------------------
    def __enter__(self):
        return self  # Devuelve el objeto para ser usado en el bloque with

    # --------------------------------------------------
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            logout(window=self.main)
        except Exception as e:
            print(f"Error al cerrar sesión: {e}")

    # --------------------------------------------------
    def quit(self):
        self.__exit__(None, None, None)


# --------------------------------------------------
def login(username: str, password: str) -> ConnectSGF:
    """Login to SGF"""

    app_path = r"\\ipvfiles\SISTEMAS\Pagos\Pagos.exe"
    app = Application(backend="uia").start(app_path)
    try:
        time.sleep(3)
        main = app.window(title_re=".*Sistema de Gestión Financiera.*")
        if not main.is_maximized():
            main.maximize()
        cmb_user = main.child_window(
            auto_id="1", control_type="ComboBox", found_index=0
        ).wait("exists enabled visible ready")
        input_password = main.child_window(
            auto_id="2", control_type="Edit", found_index=0
        ).wrapper_object()
        cmb_user.type_keys(username)
        input_password.type_keys(password)
        btn_accept = main.child_window(
            title="Aceptar", auto_id="4", control_type="Button"
        ).wrapper_object()
        btn_accept.click()
        main.child_window(
            title="La contraseña no es válida. Vuelva a intentarlo",
            auto_id="65535",
            control_type="Text",
        ).wait_not("exists visible enabled", timeout=1)
    except TimeoutError:
        print("No se pudo conectar al SGF. Verifique sus credenciales")
        close_button = main.child_window(
            title="Cerrar", control_type="Button", found_index=0
        )
        close_button.click()
        btn_cancel = main.child_window(
            title="Cancelar", auto_id="3", control_type="Button"
        ).wrapper_object()
        btn_cancel.click()
    except Exception as e:
        print(f"Ocurrió un error durante el login: {e}")
        raise  # <- Muy importante
    # except Exception as e:
    #     print(f"Ocurrió un error: {e}, {type(e)}")
    #     self.quit()

    return ConnectSGF(app=app, main=main)

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
def read_csv_file(file_path: Path) -> pd.DataFrame:
    """Read csv file"""
    try:
        df = pd.read_csv(
            file_path,
            index_col=None,
            header=None,
            na_filter=False,
            dtype=str,
            encoding="ISO-8859-1",
        )
        df.columns = [str(x) for x in range(df.shape[1])]
        return df
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return None


# --------------------------------------------------
@dataclass
class SGFReportManager(ABC):
    sgf: ConnectSGF = None
    df: pd.DataFrame = None
    clean_df: pd.DataFrame = None

    # --------------------------------------------------
    def login(self, username: str, password: str) -> ConnectSGF:
        self.sgf = login(
            username=username,
            password=password,
        )
        return self.sgf

    # --------------------------------------------------
    def move_report(self, dir_path: Path, name: str):
        if not isinstance(dir_path, Path):
            dir_path = Path(dir_path)
        old_file_path = Path(os.path.join(r"D:\Users\fcorrales\Desktop", name))
        new_file_path = dir_path / name

        # Crear las carpetas necesarias
        dir_path.mkdir(parents=True, exist_ok=True)

        for _ in range(10):  # Máximo 10 intentos (~5 segundos)
            if old_file_path.exists():
                break
            time.sleep(0.5)
        else:
            raise FileNotFoundError(
                f"No se encontró el archivo descargado en: {old_file_path}"
            )

        while not os.path.exists(old_file_path):
            time.sleep(1)
            while self.is_locked(old_file_path):
                time.sleep(1)

        if os.path.isfile(old_file_path):
            if os.path.isfile(new_file_path):
                os.remove(new_file_path)
            os.rename(old_file_path, new_file_path)
        else:
            raise ValueError("%s isn't a file!" % old_file_path)

    # --------------------------------------------------
    @abstractmethod
    def process_dataframe(self, dataframe: pd.DataFrame = None) -> pd.DataFrame:
        """ "Transform read csv file"""
        pass

    # --------------------------------------------------
    def read_csv_file(self, file_path: Path) -> pd.DataFrame:
        """Read xls file"""
        try:
            self.df = read_csv_file(file_path=file_path)
            return self.df

        except Exception as e:
            print(f"Error al leer el archivo: {e}")
            return None

    # def from_external_report(self, csv_path:str) -> pd.DataFrame:
    #     """"Read from csv SGF's report"""
    #     df = self.read_csv(csv_path, names = list(range(0,70)))
    #     read_title = df['1'].iloc[0][0:32]
    #     if read_title == self._REPORT_TITLE:
    #         self.df = df
    #         self.transform_df()
    #     else:
    #         # Future exception raise
    #         pass
    #     return self.df

    # --------------------------------------------------
    def logout(self) -> None:
        logout(window=self.main)


# --------------------------------------------------
def main():
    """Make a jazz noise here"""

    args = get_args()

    with login(args.username, args.password) as conn:
        print(f"Connected to SGF as {args.username}")
        time.sleep(3)  # o lo que necesites
        pass
        # Here you can add more operations with the SGF connection

    # Al salir del bloque, se ejecuta logout automáticamente
    print("Sesión cerrada")

    # connect_sgf = login(args.username, args.password)
    # logout()


# --------------------------------------------------
if __name__ == "__main__":
    # asyncio.run(main())
    main()
    # From /invicofapy
    # poetry run python -m src.sgf.handlers.connect_sgf
