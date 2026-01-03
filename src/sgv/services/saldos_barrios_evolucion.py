__all__ = ["SaldosBarriosEvolucionService", "SaldosBarriosEvolucionServiceDependency"]

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
from ..handlers import SaldosBarriosEvolucion
from ..repositories import SaldosBarriosEvolucionRepositoryDependency
from ..schemas import SaldosBarriosEvolucionDocument, SaldosBarriosEvolucionParams


# -------------------------------------------------
@dataclass
class SaldosBarriosEvolucionService:
    repository: SaldosBarriosEvolucionRepositoryDependency
    saldos_barrios: SaldosBarriosEvolucion = field(
        init=False
    )  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.saldos_barrios = SaldosBarriosEvolucion()

    # -------------------------------------------------
    async def sync_saldos_barrios_evolucion_from_sgv(
        self,
        username: str,
        password: str,
        params: SaldosBarriosEvolucionParams = None,
    ) -> List[RouteReturnSchema]:
        """Downloads a report from SGV, processes it, validates the data,
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
                await self.saldos_barrios.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                # await self.saldos_barrios.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = await self.saldos_barrios.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio)
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
                if hasattr(self.saldos_barrios, "logout"):
                    await self.saldos_barrios.logout()
                return return_schema

    # -------------------------------------------------
    async def get_saldos_barrios_evolucion_from_db(
        self, params: BaseFilterParams
    ) -> List[SaldosBarriosEvolucionDocument]:
        return await self.repository.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving SGV's SaldosBarriosEvolucion from the database",
        )

    # -------------------------------------------------
    async def sync_saldos_barrios_evolucion_from_sqlite(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = (
                await self.saldos_barrios.sync_validated_sqlite_to_repository(
                    sqlite_path=sqlite_path
                )
            )
        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400, detail="Invalid response format from SGV"
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
    async def export_saldos_barrios_evolucion_from_db(
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
            filename=f"InformeEvolucionDeSaldosPorBarrio_{ejercicio or 'all'}.xlsx",
            sheet_name="InformeEvolucionDeSaldosPorBarrio",
        )


SaldosBarriosEvolucionServiceDependency = Annotated[
    SaldosBarriosEvolucionService, Depends()
]
