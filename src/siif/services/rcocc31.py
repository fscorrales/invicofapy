__all__ = ["Rcocc31Service", "Rcocc31ServiceDependency"]

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
    export_multiple_dataframes_to_excel,
)
from ..handlers import Rcocc31
from ..repositories import Rcocc31RepositoryDependency
from ..schemas import Rcocc31Document, Rcocc31Params


# -------------------------------------------------
@dataclass
class Rcocc31Service:
    repository: Rcocc31RepositoryDependency
    rcocc31: Rcocc31 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.rcocc31 = Rcocc31()

    # -------------------------------------------------
    async def sync_rcocc31_from_siif(
        self,
        username: str,
        password: str,
        params: Rcocc31Params = None,
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
                await self.rcocc31.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rcocc31.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = (
                        await self.rcocc31.download_and_sync_validated_to_repository(
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
                if hasattr(self.rcocc31, "logout"):
                    await self.rcocc31.logout()
                return return_schema

    # -------------------------------------------------
    async def get_rcocc31_from_db(
        self, params: BaseFilterParams
    ) -> List[Rcocc31Document]:
        return await self.repository.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving SIIF's rcocc31 from the database",
        )

    # -------------------------------------------------
    async def sync_rcocc31_from_sqlite(self, sqlite_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = await self.rcocc31.sync_validated_sqlite_to_repository(
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
    async def export_rcocc31_from_db(self, ejercicio: int = None) -> StreamingResponse:
        if ejercicio is not None:
            docs = await self.repository.get_by_fields({"ejercicio": ejercicio})
        else:
            docs = await self.repository.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        df = df.sort_values(by=["cta_contable", "ejercicio", "fecha", "nro_entrada"])

        # Agrupar por cta_contable
        df_sheet_pairs = [
            (
                group_df,
                f"cta_{cta.replace('-', '_')[:31]}",
            )  # limitar nombre hoja a 31 caracteres
            for cta, group_df in df.groupby("cta_contable")
        ]

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=df_sheet_pairs,
            filename=f"rccocc31_{ejercicio or 'all'}.xlsx",
            upload_to_google_sheets=False,
        )


Rcocc31ServiceDependency = Annotated[Rcocc31Service, Depends()]
