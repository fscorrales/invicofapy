__all__ = ["Rfondo07tpService", "Rfondo07tpServiceDependency"]

import os
from dataclasses import dataclass, field
from typing import Annotated, List

import pandas as pd
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import logger
from ...utils import (
    BaseFilterParams,
    RouteReturnSchema,
    export_dataframe_as_excel_response,
)
from ..handlers import Rfondo07tp
from ..repositories import Rfondo07tpRepositoryDependency
from ..schemas import Rfondo07tpDocument, Rfondo07tpParams


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
    ) -> List[RouteReturnSchema]:
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
        return_schema = []
        ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
        async with async_playwright() as p:
            try:
                await self.rfondo07tp.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rfondo07tp.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = (
                        await self.rfondo07tp.download_and_sync_validated_to_repository(
                            ejercicio=int(ejercicio),
                            tipo_comprobante=params.tipo_comprobante,
                        )
                    )
                    return_schema.append(partial_schema)

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
            return await self.repository.find_with_filter_params(params=params)
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rfondo07tp from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's rfondo07tp from the database",
            )

    # -------------------------------------------------
    async def sync_rfondo07tp_from_sqlite(self, sqlite_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = await self.rfondo07tp.sync_validated_sqlite_to_repository(
                sqlite_path=sqlite_path
            )
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
            return return_schema

    # -------------------------------------------------
    async def export_rfondo07tp_from_db(
        self, ejercicio: int = None
    ) -> StreamingResponse:
        if ejercicio is not None:
            docs = await self.repository.get_by_fields({"ejercicio": ejercicio})
        else:
            docs = await self.repository.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        return export_dataframe_as_excel_response(
            df,
            filename=f"rfondo07tp_{ejercicio or 'all'}.xlsx",
            sheet_name="rfondo07tp",
        )


Rfondo07tpServiceDependency = Annotated[Rfondo07tpService, Depends()]
