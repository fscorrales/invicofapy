import datetime as dt
import os
from typing import Annotated

from dotenv import load_dotenv
from fastapi import Depends
from playwright.async_api import async_playwright

from ..handlers import Rf602

load_dotenv()
username = os.getenv("SIIF_USERNAME")
password = os.getenv("SIIF_PASSWORD")


class Rf602Service:
    def __init__(self) -> None:
        self.rf602 = Rf602()

    async def download_report(self, ejercicio: str = str(dt.datetime.now().year)):
        async with async_playwright() as p:
            await self.rf602.login(
                username=username, password=password, playwright=p, headless=False
            )
            await self.rf602.go_to_reports()
            await self.rf602.go_to_specific_report()
            await self.rf602.download_report(ejercicio=str(ejercicio))
            await self.rf602.read_xls_file()
            await self.rf602.process_dataframe()
        return self.rf602.clean_df


Rf602ServiceDependency = Annotated[Rf602Service, Depends()]
