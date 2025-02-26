import datetime as dt
from typing import Annotated

from fastapi import Depends
from fastapi.encoders import jsonable_encoder
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import COLLECTIONS, SIIF_PASSWORD, SIIF_USERNAME, db, logger
from ...utils import validate_and_extract_data_from_df
from ..handlers import Rf602
from ..models import Rf602ValidationOutput, StoredRf602


class Rf602Service:
    def __init__(self) -> None:
        assert (collection_name := "siif_rf602") in COLLECTIONS
        self.collection = db[collection_name]
        self.rf602 = Rf602()

    async def download_and_update(
        self, ejercicio: int = dt.datetime.now().year
    ) -> Rf602ValidationOutput:
        """Downloads a report from SIIF, processes it, validates the data,
        and stores it in MongoDB if valid.

        Args:
            ejercicio (int, optional): The fiscal year for the report. Defaults to the current year.

        Returns:
            ValidationResultRf602: A structured response containing validated data and errors.
        """
        async with async_playwright() as p:
            try:
                await self.rf602.login(
                    username=SIIF_USERNAME,
                    password=SIIF_PASSWORD,
                    playwright=p,
                    headless=False,
                )
                await self.rf602.go_to_reports()
                await self.rf602.go_to_specific_report()
                await self.rf602.download_report(ejercicio=str(ejercicio))
                await self.rf602.read_xls_file()
                df = await self.rf602.process_dataframe()

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df, model=StoredRf602, field_id="estructura"
                )

                # 🔹 Si hay registros validados, eliminar los antiguos e insertar los nuevos
                if validate_and_errors.validated:
                    await self.collection.delete_many({"ejercicio": ejercicio})
                    validated_records = jsonable_encoder(validate_and_errors.validated)
                    inserted_records = await self.collection.insert_many(
                        validated_records
                    )
                    logger.info(
                        f"Inserted {len(inserted_records.inserted_ids)} records into MongoDB."
                    )

            except ValidationError as e:
                logger.error(f"Validation Error: {e}")
            except Exception as e:
                logger.error(f"Error during report processing: {e}")
            finally:
                if hasattr(self.rf602, "logout"):
                    await self.rf602.logout()
        return validate_and_errors


Rf602ServiceDependency = Annotated[Rf602Service, Depends()]
