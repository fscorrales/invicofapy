__all__ = ["Rcg01UejpService", "Rcg01UejpServiceDependency"]

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
from ..handlers import Rcg01Uejp
from ..repositories import Rcg01UejpRepositoryDependency
from ..schemas import Rcg01UejpDocument, Rcg01UejpParams, Rcg01UejpReport


# -------------------------------------------------
@dataclass
class Rcg01UejpService:
    repository: Rcg01UejpRepositoryDependency
    rcg01_uejp: Rcg01Uejp = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.rcg01_uejp = Rcg01Uejp()

    # -------------------------------------------------
    async def sync_rcg01_uejp_from_siif(
        self,
        username: str,
        password: str,
        params: Rcg01UejpParams = None,
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
                await self.rcg01_uejp.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rcg01_uejp.go_to_reports()
                await self.rcg01_uejp.go_to_specific_report()
                await self.rcg01_uejp.download_report(ejercicio=str(params.ejercicio))
                await self.rcg01_uejp.read_xls_file()
                df = await self.rcg01_uejp.process_dataframe()

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df, model=Rcg01UejpReport, field_id="nro_comprobante"
                )

                # 🔹 Si hay registros validados, eliminar los antiguos e insertar los nuevos
                if validate_and_errors.validated:
                    logger.info(
                        f"Procesado ejercicio {str(params.ejercicio)}. Errores: {len(validate_and_errors.errors)}"
                    )
                    delete_dict = {"ejercicio": params.ejercicio}
                    # Contar los documentos existentes antes de eliminarlos
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
                if hasattr(self.rcg01_uejp, "logout"):
                    await self.rcg01_uejp.logout()
                return return_schema

    # -------------------------------------------------
    async def get_rcg01_uejp_from_db(
        self, params: BaseFilterParams
    ) -> List[Rcg01UejpDocument]:
        try:
            return await self.repository.find_with_filter_params(params=params)
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rcg01_uejp from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's rcg01_uejp from the database",
            )


Rcg01UejpServiceDependency = Annotated[Rcg01UejpService, Depends()]
