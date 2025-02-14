import os

import pytest
from playwright.async_api import async_playwright, expect
from siif.utils import ConnectSIIF, go_to_reports, login, logout

# Configuración para ejecutar pruebas con Playwright
# @pytest.fixture(scope="function")
# async def siif_connection():
#     async with async_playwright() as playwright:
#         connect = await login(USERNAME, PASSWORD, playwright, headless=True)
#         yield connect
#         await connect.context.close()
#         await connect.browser.close()


@pytest.mark.usefixtures("load_env")
def test_example():
    assert os.getenv("SIIF_USERNAME") == "27corralesfs"


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.usefixtures("setup_and_teardown_siif")
class TestSIIFConnection:
    async def test_login_with_valid_credentials(self):
        assert self.siif_connection is not None
        await expect(
            self.siif_connection.home_page.locator("id=pt1:cb12")
        ).to_be_visible()


# @pytest.mark.usefixtures("load_env")
# @pytest.mark.asyncio(loop_scope="session")
# async def test_login():
#     async with async_playwright() as p:
#         siif_connection = await login(
#             os.getenv("SIIF_USERNAME"),
#             os.getenv("SIIF_PASSWORD"),
#             playwright=p,
#             headless=False,
#         )
#         try:
#             await expect(
#                 siif_connection.home_page.locator("id=pt1:cb12")
#             ).to_be_visible()
#         finally:
#             await logout(siif_connection)


# @pytest.mark.asyncio(loop_scope="session")
# async def test_go_to_reports(siif_connection):
#     await go_to_reports(siif_connection)
#     all_pages = siif_connection.context.pages
#     assert len(all_pages) > 1, "No se abrió la nueva pestaña de reportes."


# @pytest.mark.asyncio(loop_scope="session")
# async def test_logout(siif_connection):
#     await logout(siif_connection)
#     assert await siif_connection.home_page.locator(
#         "id=pt1:it1::content"
#     ).is_visible(), "No se redirigió a la página de login después del logout."
