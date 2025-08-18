__all__ = ["Rpa03gService", "Rpa03gServiceDependency"]

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
from ..handlers import Rpa03g
from ..repositories import Rpa03gRepositoryDependency
from ..schemas import Rpa03gDocument, Rpa03gParams


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
        grupos_partidas = list(
            range(params.grupo_partida_desde, params.grupo_partida_hasta + 1)
        )
        async with async_playwright() as p:
            try:
                await self.rpa03g.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rpa03g.go_to_reports()
                for ejercicio in ejercicios:
                    for grupo_partida in grupos_partidas:
                        partial_schema = (
                            await self.rpa03g.download_and_sync_validated_to_repository(
                                ejercicio=int(ejercicio),
                                grupo_partida=grupo_partida,
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
                if hasattr(self.rpa03g, "logout"):
                    await self.rpa03g.logout()
                return return_schema

    # -------------------------------------------------
    async def get_rpa03g_from_db(
        self, params: BaseFilterParams
    ) -> List[Rpa03gDocument]:
        try:
            return await self.repository.find_with_filter_params(params=params)
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rpa03g from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's rpa03g from the database",
            )

    # -------------------------------------------------
    async def sync_rpa03g_from_sqlite(self, sqlite_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = await self.rpa03g.sync_validated_sqlite_to_repository(
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
    async def export_rpa03g_from_db(self, ejercicio: int = None) -> StreamingResponse:
        if ejercicio is not None:
            docs = await self.repository.get_by_fields({"ejercicio": ejercicio})
        else:
            docs = await self.repository.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        return export_dataframe_as_excel_response(
            df,
            filename=f"rpa03g_{ejercicio or 'all'}.xlsx",
            sheet_name="rpa03g",
        )


Rpa03gServiceDependency = Annotated[Rpa03gService, Depends()]
