__all__ = ["Rfondos04Service", "Rfondos04ServiceDependency"]

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
from ..handlers import Rfondos04
from ..repositories import Rfondos04RepositoryDependency
from ..schemas import Rfondos04Document, Rfondos04Params


# -------------------------------------------------
@dataclass
class Rfondos04Service:
    repository: Rfondos04RepositoryDependency
    rfondos04: Rfondos04 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.rfondos04 = Rfondos04()

    # -------------------------------------------------
    async def sync_rfondos04_from_siif(
        self,
        username: str,
        password: str,
        params: Rfondos04Params = None,
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
                await self.rfondos04.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rfondos04.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = (
                        await self.rfondos04.download_and_sync_validated_to_repository(
                            ejercicio=int(ejercicio),
                            tipo_comprobante=params.tipo_comprobante.value,
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
                if hasattr(self.rfondos04, "logout"):
                    await self.rfondos04.logout()
                return return_schema

    # -------------------------------------------------
    async def get_rfondos04_from_db(
        self, params: BaseFilterParams
    ) -> List[Rfondos04Document]:
        try:
            return await self.repository.find_with_filter_params(params=params)
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rfondos04 from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's rfondos04 from the database",
            )

    # -------------------------------------------------
    async def export_rfondos04_from_db(
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
            filename=f"rfondos04_{ejercicio or 'all'}.xlsx",
            sheet_name="rfondos04",
        )


Rfondos04ServiceDependency = Annotated[Rfondos04Service, Depends()]
