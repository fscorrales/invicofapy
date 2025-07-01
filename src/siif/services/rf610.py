__all__ = ["Rf610Service", "Rf610ServiceDependency"]

from dataclasses import dataclass, field
from typing import Annotated, List

from fastapi import Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import logger
from ...utils import (
    BaseFilterParams,
    RouteReturnSchema,
    validate_and_extract_data_from_df,
)
from ..handlers import Rf610
from ..repositories import Rf610RepositoryDependency
from ..schemas import Rf610Document, Rf610Params, Rf610Report


# -------------------------------------------------
@dataclass
class Rf610Service:
    repository: Rf610RepositoryDependency
    rf610: Rf610 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.rf610 = Rf610()

    # -------------------------------------------------
    async def sync_rf610_from_siif(
        self,
        username: str,
        password: str,
        params: Rf610Params = None,
    ) -> RouteReturnSchema:
        """Downloads a report from SIIF, processes it, validates the data,
        and stores it in MongoDB if valid.

        Args:
            ejercicio (int, optional): The fiscal year for the report. Defaults to the current year.

        Returns:
            RouteReturnSchema
        """
        if username is None or password is None:
            raise HTTPException(
                status_code=401,
                detail="Missing username or password",
            )
        return_schema = RouteReturnSchema()
        async with async_playwright() as p:
            try:
                await self.rf610.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rf610.go_to_reports()
                await self.rf610.go_to_specific_report()
                await self.rf610.download_report(ejercicio=str(params.ejercicio))
                await self.rf610.read_xls_file()
                df = await self.rf610.process_dataframe()

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df, model=Rf610Report, field_id="estructura"
                )

                # 🔹 Si hay registros validados, eliminar los antiguos e insertar los nuevos
                if validate_and_errors.validated:
                    logger.info(
                        f"Procesado ejercicio {str(params.ejercicio)}. Errores: {len(validate_and_errors.errors)}"
                    )
                    delete_dict = {"ejercicio": str(params.ejercicio)}
                    # Contar los instrumentos existentes antes de eliminarlos
                    deleted_count = await self.repository.count_by_fields(delete_dict)
                    await self.repository.delete_by_fields(delete_dict)
                    # await self.collection.delete_many({"ejercicio": ejercicio})
                    data_to_store = jsonable_encoder(validate_and_errors.validated)
                    inserted_records = await self.repository.save_all(data_to_store)
                    logger.info(
                        f"Inserted {len(inserted_records.inserted_ids)} records into MongoDB."
                    )
                    return_schema.deleted = deleted_count
                    return_schema.added = len(data_to_store)
                    return_schema.errors = validate_and_errors.errors

            except ValidationError as e:
                logger.error(f"Validation Error: {e}")
                raise HTTPException(
                    status_code=400, detail="Invalid response format from SIIF"
                )
            except Exception as e:
                logger.error(f"Error during report processing: {e}")
                raise HTTPException(
                    status_code=401,
                    detail="Invalid credentials or unable to authenticate",
                )
            finally:
                if hasattr(self.rf610, "logout"):
                    await self.rf610.logout()
                return return_schema

    # -------------------------------------------------
    async def get_rf610_from_db(self, params: BaseFilterParams) -> List[Rf610Document]:
        try:
            return await self.instruments.find_with_filter_params(params=params)
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rf602 from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's rf602 from the database",
            )


Rf610ServiceDependency = Annotated[Rf610Service, Depends()]
