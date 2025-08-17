__all__ = ["RfpP605bService", "RfpP605bServiceDependency"]

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
from ..handlers import RfpP605b
from ..repositories import RfpP605bRepositoryDependency
from ..schemas import RfpP605bDocument, RfpP605bParams


# -------------------------------------------------
@dataclass
class RfpP605bService:
    repository: RfpP605bRepositoryDependency
    rfp_p605b: RfpP605b = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.rfp_p605b = RfpP605b()

    # -------------------------------------------------
    async def sync_rfp_p605b_from_siif(
        self,
        username: str,
        password: str,
        params: RfpP605bParams = None,
    ) -> List[RouteReturnSchema]:
        """Downloads a report from SIIF, processes it, validates the data,
        and stores it in MongoDB if valid.

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
                await self.rfp_p605b.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rfp_p605b.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = (
                        await self.rfp_p605b.download_and_sync_validated_to_repository(
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
                if hasattr(self.rfp_p605b, "logout"):
                    await self.rfp_p605b.logout()
                return return_schema

    # -------------------------------------------------
    async def get_rfp_p605b_from_db(
        self, params: BaseFilterParams
    ) -> List[RfpP605bDocument]:
        return await self.repository.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving SIIF's rfp_p605b from the database",
        )

    # -------------------------------------------------
    async def sync_rfp_p605b_from_sqlite(self, sqlite_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = await self.rfp_p605b.sync_validated_sqlite_to_repository(
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
    async def export_rfp_p605b_from_db(
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
            filename=f"rfp_p605b_{ejercicio or 'all'}.xlsx",
            sheet_name="rfp_p605b",
        )


RfpP605bServiceDependency = Annotated[RfpP605bService, Depends()]
