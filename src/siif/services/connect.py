#!/usr/bin/env python3
"""

Author : Fernando Corrales <fscpython@gamail.com>

Date   : 13-feb-2025

Purpose: Connect to SIIF
"""

__all__ = [
    "ConnectSIIF",
    "login",
    "logout",
    "go_to_reports",
    "select_report_module",
    "select_specific_report_by_id",
]

import argparse
import asyncio
import time
from dataclasses import dataclass
from enum import Enum

from playwright._impl._browser import Browser, BrowserContext, Page
from playwright.async_api import Playwright, async_playwright


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Connect to SIIF",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("username", metavar="username", help="Username for SIIF access")

    parser.add_argument("password", metavar="password", help="Password for SIIF access")

    return parser.parse_args()


# --------------------------------------------------
@dataclass
class ConnectSIIF:
    browser: Browser = None
    context: BrowserContext = None
    home_page: Page = None
    reports_page: Page = None


# --------------------------------------------------
class ReportCategory(Enum):
    Gastos = "SUB - SISTEMA DE CONTROL DE GASTOS"
    Recursos = "SUB - SISTEMA DE CONTROL de RECURSOS"
    Contabilidad = "SUB - SISTEMA DE CONTABILIDAD PATRIMONIAL"
    Formulacion = "SUB - SISTEMA DE FORMULACION PRESUPUESTARIA"
    Clasificadores = "SUB - SISTEMA DE CLASIFICADORES"


# --------------------------------------------------
async def login(
    username: str, password: str, playwright: Playwright = None, headless: bool = False
) -> ConnectSIIF:
    if playwright is None:
        playwright = await async_playwright().start()

    browser = await playwright.chromium.launch(
        headless=headless, args=["--start-maximized"]
    )
    context = await browser.new_context(no_viewport=True)
    page = await context.new_page()

    try:
        "Open SIIF webpage"
        await page.goto("https://siif.cgpc.gob.ar/mainSiif/faces/login.jspx")
        "Login with credentials"
        await page.locator("id=pt1:it1::content").fill(username)
        await page.locator("id=pt1:it2::content").fill(password)
        btn_connect = page.locator("id=pt1:cb1")
        await btn_connect.click()
        await page.wait_for_load_state("networkidle")
    except Exception as e:
        print(f"Ocurrio un error: {e}")

    return ConnectSIIF(
        browser=browser, context=context, home_page=page, reports_page=None
    )


# --------------------------------------------------
async def go_to_reports(connect: ConnectSIIF) -> None:
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
        print(f"La lista de paginas es: {all_pages}")
    except Exception as e:
        print(f"Ocurrio un error: {e}")
        await logout(connect)


# --------------------------------------------------
async def select_report_module(connect: ConnectSIIF, module: ReportCategory) -> None:
    try:
        await connect.reports_page.click("xpath=//select[@id='pt1:socModulo::content']")
        cmb_modulo = connect.reports_page.locator(
            "xpath=//select[@id='pt1:socModulo::content']"
        )
        await cmb_modulo.select_option(value=module.value)
        await connect.reports_page.wait_for_load_state("networkidle")

    except Exception as e:
        print(f"Error al seleccionar el módulo de reportes: {e}")
        await logout(connect)  # Cerrar sesión en caso de error


# --------------------------------------------------
async def select_specific_report_by_id(connect: ConnectSIIF, report_id: str) -> None:
    try:
        input_filter = await connect.reports_page.query_selector(
            "input[id='_afrFilterpt1_afr_pc1_afr_tableReportes_afr_c1::content']"
        )
        await input_filter.fill(report_id)
        await input_filter.press("Enter")
        btn_siguiente = await connect.reports_page.query_selector(
            "div[id='pt1:pc1:btnSiguiente']"
        )
        await connect.reports_page.wait_for_load_state("networkidle")
        await btn_siguiente.click()
    except Exception as e:
        print(f"Error al seleccionar el módulo de reportes: {e}")
        await logout(connect)  # Cerrar sesión en caso de error


# --------------------------------------------------
async def logout(connect: ConnectSIIF) -> None:
    await connect.home_page.locator("id=pt1:pt_np1:pt_cni1").click()
    await connect.home_page.wait_for_load_state("networkidle")


# --------------------------------------------------
async def main():
    """Make a jazz noise here"""

    args = get_args()

    print(args.username)
    print(args.password)

    async with async_playwright() as p:
        connect_siif = await login(
            args.username, args.password, playwright=p, headless=False
        )
        await go_to_reports(connect=connect_siif)
        await logout(connect=connect_siif)


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy
    # .venv/Scripts/python src/siif/utils/connect.py username password
