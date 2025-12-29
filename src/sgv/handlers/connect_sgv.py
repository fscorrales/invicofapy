#!/usr/bin/env python3
"""

Author : Fernando Corrales <fscpython@gmail.com>

Date   : 29-dic-2025

Purpose: Connect to SGV
"""

__all__ = [
    "ConnectSGV",
    "login",
    "logout",
    "go_to_reports",
    "SIIFReportManager",
]

import argparse
import asyncio
import datetime as dt
import io
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import pandas as pd
from playwright._impl._browser import Browser, BrowserContext, Page
from playwright.async_api import Download, Playwright, async_playwright


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Connect to SGV",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-u",
        "--username",
        help="Username for SGV access",
        metavar="username",
        type=str,
        default=None,
    )

    parser.add_argument(
        "-p",
        "--password",
        help="Password for SGV access",
        metavar="password",
        type=str,
        default=None,
    )

    args = parser.parse_args()

    if args.username is None or args.password is None:
        from ...config import settings

        args.username = settings.SGV_USERNAME
        args.password = settings.SGV_PASSWORD
        if args.username is None or args.password is None:
            parser.error("Both --username and --password are required.")

    return args


# --------------------------------------------------
@dataclass
class ConnectSGV:
    browser: Browser = None
    context: BrowserContext = None
    home_page: Page = None
    reports_page: Page = None


# --------------------------------------------------
class ReportCategory(str, Enum):
    Gastos = "SUB - SISTEMA DE CONTROL DE GASTOS"
    Recursos = "SUB - SISTEMA DE CONTROL de RECURSOS"
    Contabilidad = "SUB - SISTEMA DE CONTABILIDAD PATRIMONIAL"
    Formulacion = "SUB - SISTEMA DE FORMULACION PRESUPUESTARIA"
    Clasificadores = "SUB - SISTEMA DE CLASIFICADORES"


# --------------------------------------------------
async def login(
    username: str, password: str, playwright: Playwright = None, headless: bool = False
) -> ConnectSGV:
    if playwright is None:
        playwright = await async_playwright().start()

    browser = await playwright.chromium.launch(
        headless=headless, args=["--start-maximized"]
    )
    context = await browser.new_context(no_viewport=True)
    page = await context.new_page()

    try:
        "Open SGV webpage"
        await page.goto("https://gv.invico.gov.ar/login.aspx")
        "Login with credentials"
        input_username, input_password, btn_connect = await page.locator(
            "table//input"
        ).all()
        await input_username.fill(username)
        await input_password.fill(password)
        # btn_connect = page.locator("id=pt1:cb1")
        await btn_connect.click()
        await page.wait_for_load_state("networkidle")
    except Exception as e:
        print(f"Ocurrio un error: {e}")

    return ConnectSGV(
        browser=browser, context=context, home_page=page, reports_page=None
    )


# --------------------------------------------------
async def go_to_reports(connect: ConnectSGV) -> None:
    try:
        btn_reports = connect.home_page.locator("id=pt1:cb12")
        await btn_reports.wait_for()
        await btn_reports.click()
        await connect.home_page.wait_for_load_state("networkidle")
        # New Tab generated
        async with connect.context.expect_page() as new_page_info:
            btn_ver_reportes = connect.home_page.locator("id=pt1:cb14")
            await btn_ver_reportes.wait_for()
            await btn_ver_reportes.click()  # Opens a new tab
        connect.reports_page = await new_page_info.value
        all_pages = connect.context.pages
    except Exception as e:
        print(f"Ocurrio un error: {e}")
        await logout(connect)


# --------------------------------------------------
async def go_back_to_reports_list(connect: ConnectSGV) -> None:
    try:
        btn_volver = connect.reports_page.locator("xpath=//div[@id='pt1:btnVolver']")
        await btn_volver.click()
        await connect.reports_page.wait_for_load_state("networkidle")
    except Exception as e:
        print(f"Ocurrio un error: {e}")
        await logout(connect)


# --------------------------------------------------
async def read_xls_file(file_path: Path) -> pd.DataFrame:
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
async def logout(connect: ConnectSGV) -> None:
    await connect.home_page.locator("id=pt1:pt_np1:pt_cni1").click()
    await connect.home_page.wait_for_load_state("networkidle")


# --------------------------------------------------
@dataclass
class SIIFReportManager(ABC):
    siif: ConnectSGV = None
    download: Download = None
    df: pd.DataFrame = None
    clean_df: pd.DataFrame = None

    # --------------------------------------------------
    async def login(
        self, username: str, password: str, playwright: Playwright, headless: bool
    ) -> ConnectSGV:
        self.siif = await login(
            username=username,
            password=password,
            playwright=playwright,
            headless=headless,
        )
        return self.siif

    # --------------------------------------------------
    async def go_to_reports(self) -> None:
        await go_to_reports(connect=self.siif)

    # --------------------------------------------------
    @abstractmethod
    async def go_to_specific_report(self) -> None:
        """Go to specific report"""
        pass

    # --------------------------------------------------
    @abstractmethod
    async def download_report(
        self, ejercicio: str = str(dt.datetime.now().year)
    ) -> Download:
        """Download report from SIIF"""
        pass

    # --------------------------------------------------
    async def go_back_to_reports_list(self) -> None:
        await go_back_to_reports_list(connect=self.siif)

    # --------------------------------------------------
    @abstractmethod
    async def process_dataframe(self, dataframe: pd.DataFrame = None) -> pd.DataFrame:
        """ "Transform read xls file"""
        pass

    # --------------------------------------------------
    async def read_xls_file(self, file_path: Path = None) -> pd.DataFrame:
        """Read xls file"""
        try:
            if file_path is None:
                if self.download is None:
                    raise ValueError(
                        "No se ha leído el archivo. 'self.download' es None."
                    )
                file_path = await self.download.path()
            self.df = await read_xls_file(file_path=file_path)
            return self.df

        except Exception as e:
            print(f"Error al leer el archivo: {e}")
            return None

    # --------------------------------------------------
    async def save_xls_file(self, save_path: Path, file_name: str) -> None:
        """Save xls file"""
        try:
            # Wait for the download process to complete and save the downloaded file somewhere
            if self.download is None:
                raise ValueError(
                    "No se ha descargado ningún archivo. 'self.download' es None."
                )

            # Definir la ruta de guardado
            file_path = os.path.join(save_path, file_name)

            # Si el archivo ya existe, eliminarlo antes de guardar el nuevo
            if os.path.isfile(file_path):
                os.remove(file_path)

            # Guardar el archivo descargado
            await self.download.save_as(file_path)
            print(f"Reporte descargado en: {file_path}")
        except Exception as e:
            print(f"Error al descargar el reporte: {e}")
            # await self.logout()

    # --------------------------------------------------
    async def select_report_module(self, module: ReportCategory) -> None:
        try:
            await self.siif.reports_page.click(
                "xpath=//select[@id='pt1:socModulo::content']"
            )
            cmb_modulo = self.siif.reports_page.locator(
                "xpath=//select[@id='pt1:socModulo::content']"
            )
            await cmb_modulo.select_option(value=module.value)
            await self.siif.reports_page.wait_for_load_state("networkidle")

        except Exception as e:
            print(f"Error al seleccionar el módulo de reportes: {e}")

    # --------------------------------------------------
    async def select_specific_report_by_id(self, report_id: str) -> None:
        try:
            input_filter = self.siif.reports_page.locator(
                "input[id='_afrFilterpt1_afr_pc1_afr_tableReportes_afr_c1::content']"
            )
            await input_filter.clear()
            await input_filter.fill(report_id)
            await input_filter.press("Enter")
            btn_siguiente = self.siif.reports_page.locator(
                "div[id='pt1:pc1:btnSiguiente']"
            )
            await self.siif.reports_page.wait_for_load_state("networkidle")
            await btn_siguiente.click()
        except Exception as e:
            print(f"Error al seleccionar el módulo de reportes: {e}")

    # --------------------------------------------------
    async def logout(self) -> None:
        await logout(connect=self.siif)


# --------------------------------------------------
async def main():
    """Make a jazz noise here"""

    args = get_args()

    async with async_playwright() as p:
        connect_sgv = await login(
            args.username, args.password, playwright=p, headless=False
        )
        # await go_to_reports(connect=connect_sgv)
        # await logout(connect=connect_sgv)


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy
    # poetry run python -m src.sgv.handlers.connect_sgv
