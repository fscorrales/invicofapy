__all__ = ["Rdeu012Service", "Rdeu012ServiceDependency"]

import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, List

import pandas as pd
from dateutil.relativedelta import relativedelta
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
from ..handlers import Rdeu012
from ..repositories import Rdeu012RepositoryDependency
from ..schemas import Rdeu012Document, Rdeu012Params


# -------------------------------------------------
@dataclass
class Rdeu012Service:
    repository: Rdeu012RepositoryDependency
    rdeu012: Rdeu012 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.rdeu012 = Rdeu012()

    # -------------------------------------------------
    async def sync_rdeu012_from_siif(
        self,
        username: str,
        password: str,
        params: Rdeu012Params = None,
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

        # Convertimos a datetime
        start = datetime.strptime(params.mes_from, "%Y%m")
        end = datetime.strptime(params.mes_to, "%Y%m")

        if start > end:
            raise ValueError("mes_from no puede ser mayor que mes_to")

        meses = []
        current = start
        while current <= end:
            meses.append(int(current.strftime("%Y%m")))
            current += relativedelta(months=1)

        async with async_playwright() as p:
            try:
                await self.rdeu012.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rdeu012.go_to_reports()
                for mes in meses:
                    partial_schema = (
                        await self.rdeu012.download_and_sync_validated_to_repository(
                            mes=str(mes)
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
                if hasattr(self.rdeu012, "logout"):
                    await self.rdeu012.logout()
                return return_schema

    # -------------------------------------------------
    async def get_rdeu012_from_db(
        self, params: BaseFilterParams
    ) -> List[Rdeu012Document]:
        return await self.repository.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving SIIF's rdeu012 from the database",
        )

    # -------------------------------------------------
    async def sync_rdeu012_from_sqlite(self, sqlite_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = await self.rdeu012.sync_validated_sqlite_to_repository(
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
    async def export_rdeu012_from_db(self, ejercicio: int = None) -> StreamingResponse:
        if ejercicio is not None:
            docs = await self.repository.get_by_fields({"ejercicio": ejercicio})
        else:
            docs = await self.repository.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        return export_dataframe_as_excel_response(
            df,
            filename=f"rdeu012_{ejercicio or 'all'}.xlsx",
            sheet_name="rdeu012",
        )


Rdeu012ServiceDependency = Annotated[Rdeu012Service, Depends()]
