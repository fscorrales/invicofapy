#!/usr/bin/env python3
"""

Author : Fernando Corrales <fscpython@gamail.com>

Date   : 13-feb-2025

Purpose: Connect to SIIF
"""

import argparse
import asyncio
from dataclasses import dataclass

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
        # await context.close()
        # await browser.close()

    return ConnectSIIF(browser=browser, context=context, home_page=page)


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
        reports_page = await new_page_info.value
        all_pages = connect.context.pages
        print(f"La lista de paginas es: {all_pages}")
    except Exception as e:
        print(f"Ocurrio un error: {e}")


# --------------------------------------------------
async def logout(connect: ConnectSIIF) -> None:
    await connect.home_page.locator("id=pt1:pt_np1:pt_cni1").click()


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
