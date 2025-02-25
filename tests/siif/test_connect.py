from pathlib import Path

import pandas as pd
import pytest
from playwright.async_api import expect

from siif.handlers.connect_siif import go_to_reports, logout, read_xls_file

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


@pytest.mark.siif_connect
@pytest.mark.asyncio(loop_scope="class")
@pytest.mark.usefixtures("setup_and_teardown_siif")
class TestSIIFConnection:
    @pytest.mark.siif_login
    async def test_login_with_valid_credentials(self, setup_and_teardown_siif):
        siif = setup_and_teardown_siif
        assert siif is not None
        await expect(siif.home_page.locator("id=pt1:cb12")).to_be_visible()

    @pytest.mark.siif_reports
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

    @pytest.mark.siif_read_xls_file
    async def test_read_xls_file(self, tmp_path):
        # Ruta del archivo temporal
        tmp_file_path = tmp_path / "test_file.xlsx"

        # Crear un archivo XLS de prueba
        test_data = {
            "0": ["Header 1", "Header 2", "Header 3"],
            "1": ["Data 1", "Data 2", "Data 3"],
            "2": ["Data 4", "Data 5", "Data 6"],
        }
        df_test = pd.DataFrame(test_data)

        # Guardar como XLS
        df_test.to_excel(tmp_file_path, index=False, header=False, engine="openpyxl")

        # Leer el archivo XLS
        df_read = await read_xls_file(tmp_file_path)

        # Verificar que el DataFrame se cargó correctamente
        assert df_read is not None, "El DataFrame no se cargó correctamente."
        assert isinstance(df_read, pd.DataFrame), (
            "El objeto cargado no es un DataFrame."
        )

        # Verificar que el DataFrame tiene las columnas y filas esperadas
        assert not df_read.empty, "El DataFrame está vacío."
        assert df_read.shape == (3, 3), "El DataFrame no tiene el tamaño esperado."

        # Verificar que los datos son correctos
        expected_data = [
            ["Header 1", "Header 2", "Header 3"],
            ["Data 1", "Data 2", "Data 3"],
            ["Data 4", "Data 5", "Data 6"],
        ]
        df_read = df_read.T
        assert df_read.values.tolist() == expected_data, (
            "Los datos del DataFrame no coinciden con los esperados."
        )

        # Limpiar el archivo temporal
        Path(tmp_file_path).unlink()
