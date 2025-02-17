#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gamail.com>
Date   : 14-feb-2025
Purpose: Read, process and write SIIF's rf602 (Prespuesto de Gastos por Fuente) report
"""

__all__ = []

import argparse
import asyncio
import datetime as dt

from playwright._impl._browser import Browser, BrowserContext, Page
from playwright.async_api import async_playwright

from .connect import (
    ReportCategory,
    go_to_reports,
    login,
    logout,
    select_report_module,
    select_specific_report_by_id,
)


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Read, process and write SIIF's rf602",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("username", metavar="username", help="Username for SIIF access")

    parser.add_argument("password", metavar="password", help="Password for SIIF access")

    parser.add_argument(
        "-e",
        "--ejercicio",
        metavar="ejercicio",
        default=2025,
        type=int,
        help="Ejercicio to download from SIIF",
    )

    parser.add_argument(
        "-d", "--download", help="Download report from SIIF", action="store_true"
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
        help="SIIF' rf602.xls report. Must be in the same folder",
    )

    args = parser.parse_args()

    actual_year = dt.datetime.now().year
    if args.ejercicio < 2010 or args.ejercicio > actual_year:
        parser.error(
            f"--ejercicio '{args.ejercicio}' must be between 2010 and {actual_year}"
        )

    if args.file and args.download:
        parser.error("You cannot use --file and --download together. Choose one.")

    return args


# --------------------------------------------------
async def main():
    """Make a jazz noise here"""

    args = get_args()
    async with async_playwright() as p:
        connect_siif = await login(
            args.username, args.password, playwright=p, headless=False
        )
        await go_to_reports(connect=connect_siif)
        await select_report_module(connect=connect_siif, module=ReportCategory.Gastos)
        await select_specific_report_by_id(connect=connect_siif, report_id="38")
        await logout(connect=connect_siif)


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy
    # .venv/Scripts/python src/siif/services/rf602.py username password

    # python -m src.siif.services.rf602 username password
