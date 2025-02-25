import datetime as dt

import pytest
from playwright.async_api import expect

from siif.handlers import Rf602


@pytest.mark.siif_rf602
@pytest.mark.asyncio(loop_scope="class")
@pytest.mark.usefixtures("setup_and_teardown_siif")
class TestRf602:
    @pytest.fixture(autouse=True)
    def setup_fixture(self, setup_and_teardown_siif):
        self.rf602 = Rf602(
            siif=setup_and_teardown_siif
        )  # ✅ Asigna variables sin usar __init__()

    async def test_go_to_specific_report(self):
        await self.rf602.go_to_reports()
        await self.rf602.go_to_specific_report()

        # Verificar que estamos en la página correcta
        await expect(
            self.rf602.siif.reports_page.locator("id=pt1:txtAnioEjercicio::content")
        ).to_be_visible()

    async def test_download_report(self):
        # Descargar el reporte para el año actual
        download = await self.rf602.download_report(
            ejercicio=str(dt.datetime.now().year)
        )
        assert download is not None, "No se pudo descargar el reporte."

    async def test_process_dataframe(self):
        # Leer el archivo descargado
        await self.rf602.read_xls_file()

        # Procesar el DataFrame
        processed_df = await self.rf602.process_dataframe()

        # Verificar que el DataFrame procesado no está vacío
        assert not processed_df.empty, "El DataFrame procesado está vacío."

        # Verificar que las columnas esperadas están presentes
        expected_columns = [
            "ejercicio",
            "estructura",
            "fuente",
            "programa",
            "subprograma",
            "proyecto",
            "actividad",
            "grupo",
            "partida",
            "org",
            "credito_original",
            "credito_vigente",
            "comprometido",
            "ordenado",
            "saldo",
            "pendiente",
        ]
        assert all(col in processed_df.columns for col in expected_columns), (
            "Faltan columnas en el DataFrame procesado."
        )

    async def test_save_xls_file(self, tmp_path):
        # Guardar el archivo en un directorio temporal
        save_path = tmp_path / "test_rf602.xls"
        await self.rf602.save_xls_file(save_path=tmp_path, file_name="test_rf602.xls")

        # Verificar que el archivo se ha guardado correctamente
        assert save_path.exists(), "El archivo no se ha guardado correctamente."
