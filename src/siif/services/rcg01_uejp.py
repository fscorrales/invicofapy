__all__ = ["Rcg01UejpService", "Rcg01UejpServiceDependency"]

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
from ..handlers import Rcg01Uejp
from ..repositories import Rcg01UejpRepositoryDependency
from ..schemas import Rcg01UejpDocument, Rcg01UejpParams


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
        ejercicios = list(range(params.ejercicio_from, params.ejercicio_to + 1))
        async with async_playwright() as p:
            try:
                await self.rcg01_uejp.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rcg01_uejp.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = (
                        await self.rcg01_uejp.download_and_sync_validated_to_repository(
                            ejercicio=int(ejercicio)
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

    # -------------------------------------------------
    async def sync_rcg01_uejp_from_sqlite(self, sqlite_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = await self.rcg01_uejp.sync_validated_sqlite_to_repository(
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
    async def export_rcg01_uejp_from_db(
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
            filename=f"rcg01_uejp_{ejercicio or 'all'}.xlsx",
            sheet_name="rcg01_uejp",
        )


Rcg01UejpServiceDependency = Annotated[Rcg01UejpService, Depends()]
