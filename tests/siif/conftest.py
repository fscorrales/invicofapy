import os
from pathlib import Path

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from playwright.async_api import async_playwright

from siif.handlers.connect_siif import login, logout


@pytest.fixture(scope="session", autouse=True)
def load_env(request):
    env_path = Path(__file__).resolve().parent.parent.parent / "src" / ".env"
    load_dotenv(env_path)


@pytest_asyncio.fixture(scope="class", autouse=True)
async def setup_and_teardown_siif(load_env):
    async with async_playwright() as p:
        siif_connection = await login(
            os.getenv("SIIF_USERNAME"),
            os.getenv("SIIF_PASSWORD"),
            playwright=p,
            headless=False,
        )
        yield siif_connection
        await logout(siif_connection)
