__all__ = ["Rfondo07tpService", "Rfondo07tpServiceDependency"]

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
from ..handlers import Rfondo07tp
from ..repositories import Rfondo07tpRepositoryDependency
from ..schemas import Rfondo07tpDocument, Rfondo07tpParams, Rfondo07tpReport


# -------------------------------------------------
@dataclass
class Rfondo07tpService:
    repository: Rfondo07tpRepositoryDependency
    rfondo07tp: Rfondo07tp = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.rfondo07tp = Rfondo07tp()

    # -------------------------------------------------
    async def sync_rfondo07tp_from_siif(
        self,
        username: str,
        password: str,
        params: Rfondo07tpParams = None,
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
                await self.rfondo07tp.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rfondo07tp.go_to_reports()
                await self.rfondo07tp.go_to_specific_report()
                await self.rfondo07tp.download_report(
                    ejercicio=str(params.ejercicio),
                    tipo_comprobante=str(params.tipo_comprobante.value),
                )
                await self.rfondo07tp.read_xls_file()
                df = await self.rfondo07tp.process_dataframe(
                    tipo_comprobante=params.tipo_comprobante.value
                )

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df, model=Rfondo07tpReport, field_id="nro_comprobante"
                )

                # 🔹 Si hay registros validados, eliminar los antiguos e insertar los nuevos
                if validate_and_errors.validated:
                    logger.info(
                        f"Procesado ejercicio {str(params.ejercicio)}. Errores: {len(validate_and_errors.errors)}"
                    )
                    delete_dict = {
                        "ejercicio": params.ejercicio,
                        "tipo_comprobante": params.tipo_comprobante.value,
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
                if hasattr(self.rfondo07tp, "logout"):
                    await self.rfondo07tp.logout()
                return return_schema

    # -------------------------------------------------
    async def get_rfondo07tp_from_db(
        self, params: BaseFilterParams
    ) -> List[Rfondo07tpDocument]:
        try:
            return await self.instruments.find_with_filter_params(params=params)
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rfondo07tp from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's rfondo07tp from the database",
            )


Rfondo07tpServiceDependency = Annotated[Rfondo07tpService, Depends()]
