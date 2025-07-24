__all__ = ["Rvicon03Service", "Rvicon03ServiceDependency"]

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
from ..handlers import Rvicon03
from ..repositories import Rvicon03RepositoryDependency
from ..schemas import Rvicon03Document, Rvicon03Params


# -------------------------------------------------
@dataclass
class Rvicon03Service:
    repository: Rvicon03RepositoryDependency
    rvicon03: Rvicon03 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.rvicon03 = Rvicon03()

    # -------------------------------------------------
    async def sync_rvicon03_from_siif(
        self,
        username: str,
        password: str,
        params: Rvicon03Params = None,
    ) -> RouteReturnSchema:
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
        return_schema = RouteReturnSchema()
        ejercicios = list(range(params.ejercicio_from, params.ejercicio_to + 1))
        async with async_playwright() as p:
            try:
                await self.rvicon03.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rvicon03.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = (
                        await self.rvicon03.download_and_sync_validated_to_repository(
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
                if hasattr(self.rvicon03, "logout"):
                    await self.rvicon03.logout()
                return return_schema

    # -------------------------------------------------
    async def get_rvicon03_from_db(
        self, params: BaseFilterParams
    ) -> List[Rvicon03Document]:
        return await self.repository.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving SIIF's rvicon03 from the database",
        )

    # -------------------------------------------------
    async def sync_rvicon03_from_sqlite(self, sqlite_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            self.rvicon03.sync_validated_sqlite_to_repository(sqlite_path=sqlite_path)
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
    async def export_rvicon03_from_db(self, ejercicio: int = None) -> StreamingResponse:
        if ejercicio is not None:
            docs = await self.repository.get_by_fields({"ejercicio": ejercicio})
        else:
            docs = await self.repository.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        return export_dataframe_as_excel_response(
            df,
            filename=f"rvicon03_{ejercicio or 'all'}.xlsx",
            sheet_name="rvicon03",
        )


Rvicon03ServiceDependency = Annotated[Rvicon03Service, Depends()]
