import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from siif.utils import login, logout


@pytest.fixture(scope="session", autouse=True)
def load_env(request):
    env_path = Path(__file__).resolve().parent.parent.parent / "src" / ".env"
    load_dotenv(env_path)


@pytest.fixture()
async def setup_and_teardown_siif(request, load_env):
    async with async_playwright() as p:
        siif_connection = await login(
            os.getenv("SIIF_USERNAME"),
            os.getenv("SIIF_PASSWORD"),
            playwright=p,
            headless=False,
        )
        request.cls.siif_connection = siif_connection
        yield
        await logout(siif_connection)
