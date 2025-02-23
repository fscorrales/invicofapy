import pytest
from playwright.async_api import expect

from siif.handlers.connect_siif import go_to_reports, logout

# @pytest.mark.asyncio(loop_scope="session")
# async def test_login_with_invalid_credentials():
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


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.usefixtures("setup_and_teardown_siif")
class TestSIIFConnection:
    async def test_login_with_valid_credentials(self, setup_and_teardown_siif):
        siif = setup_and_teardown_siif
        assert siif is not None
        await expect(siif.home_page.locator("id=pt1:cb12")).to_be_visible()

    async def test_go_to_reports(self, setup_and_teardown_siif):
        siif = setup_and_teardown_siif
        await go_to_reports(siif)
        all_pages = siif.context.pages
        assert len(all_pages) > 1, "No se abrió la nueva pestaña de reportes."

    async def test_logout(self, setup_and_teardown_siif):
        siif = setup_and_teardown_siif
        await logout(siif)
        assert await siif.home_page.locator("id=pt1:it1::content").is_visible(), (
            "No se redirigió a la página de login después del logout."
        )
