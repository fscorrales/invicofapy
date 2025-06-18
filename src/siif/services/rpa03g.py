__all__ = ["Rpa03gService", "Rpa03gServiceDependency"]

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
from ..handlers import Rpa03g
from ..repositories import Rpa03gRepositoryDependency
from ..schemas import Rpa03gDocument, Rpa03gParams, Rpa03gReport


# -------------------------------------------------
@dataclass
class Rpa03gService:
    repository: Rpa03gRepositoryDependency
    rpa03g: Rpa03g = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.rpa03g = Rpa03g()

    # -------------------------------------------------
    async def sync_rpa03g_from_siif(
        self,
        username: str,
        password: str,
        params: Rpa03gParams = None,
    ) -> RouteReturnSchema:
        """Downloads a report from SIIF, processes it, validates the data,
        and stores it in MongoDB if valid.

        Args:
            ejercicio (int, optional): The fiscal year for the report. Defaults to the current year.

        Returns:
            RouteReturnSchema
        """
        return_schema = RouteReturnSchema()
        async with async_playwright() as p:
            try:
                await self.rpa03g.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rpa03g.go_to_reports()
                await self.rpa03g.go_to_specific_report()
                await self.rpa03g.download_report(ejercicio=str(params.ejercicio))
                await self.rpa03g.read_xls_file()
                df = await self.rpa03g.process_dataframe()

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df, model=Rpa03gReport, field_id="nro_comprobante"
                )

                # 🔹 Si hay registros validados, eliminar los antiguos e insertar los nuevos
                if validate_and_errors.validated:
                    logger.info(
                        f"Procesado ejercicio {str(params.ejercicio)}. Errores: {len(validate_and_errors.errors)}"
                    )
                    delete_dict = {
                        "ejercicio": params.ejercicio,
                        "grupo_partida": params.grupo_partida,
                    }
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
                if hasattr(self.rpa03g, "logout"):
                    await self.rpa03g.logout()
                return return_schema

    # -------------------------------------------------
    async def get_rpa03g_from_db(
        self, params: BaseFilterParams
    ) -> List[Rpa03gDocument]:
        try:
            return await self.instruments.find_with_filter_params(params=params)
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rpa03g from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's rpa03g from the database",
            )


Rpa03gServiceDependency = Annotated[Rpa03gService, Depends()]
