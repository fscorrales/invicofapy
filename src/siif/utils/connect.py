#!/usr/bin/env python3
"""

Author : Fernando Corrales <fscpython@gamail.com>

Date   : 13-feb-2025

Purpose: Connect to SIIF
"""

import argparse
import asyncio
import time
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
    page: Page = None


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

    "Open SIIF webpage"
    await page.goto("https://siif.cgpc.gob.ar/mainSiif/faces/login.jspx")

    try:
        await page.locator("id=pt1:it1::content").fill(username)
        await page.locator("id=pt1:it2::content").fill(password)
        btn_connect = page.locator("id=pt1:cb1")
        await btn_connect.click()
    except Exception as e:
        print(f"Ocurrio un error: {e}")
        # await context.close()
        # await browser.close()

    return ConnectSIIF(browser=browser, context=context, page=page)


# --------------------------------------------------
async def main():
    """Make a jazz noise here"""

    args = get_args()

    print(args.username)
    print(args.password)

    async with async_playwright() as p:
        await login(args.username, args.password, playwright=p, headless=False)
        # sleep time 10 seconds
        time.sleep(10)


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
