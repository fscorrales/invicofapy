__all__ = ["Rf610Service", "Rf610ServiceDependency"]

import os
from dataclasses import dataclass, field
from io import BytesIO
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
    sanitize_dataframe_for_json,
)
from ..handlers import Rf610
from ..repositories import Rf610RepositoryDependency
from ..schemas import Rf610Document, Rf610Params


# -------------------------------------------------
@dataclass
class Rf610Service:
    repository: Rf610RepositoryDependency
    rf610: Rf610 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.rf610 = Rf610()

    # -------------------------------------------------
    async def sync_rf610_from_siif(
        self,
        username: str,
        password: str,
        params: Rf610Params = None,
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
                await self.rf610.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rf610.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = (
                        await self.rf610.download_and_sync_validated_to_repository(
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
                if hasattr(self.rf610, "logout"):
                    await self.rf610.logout()
                return return_schema

    # -------------------------------------------------
    async def get_rf610_from_db(self, params: BaseFilterParams) -> List[Rf610Document]:
        try:
            return await self.repository.find_with_filter_params(params=params)
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rf610 from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's rf610 from the database",
            )

    # -------------------------------------------------
    async def sync_rf610_from_sqlite(self, sqlite_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = await self.rf610.sync_validated_sqlite_to_repository(
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
    async def export_rf610_from_db(self, ejercicio: int = None) -> StreamingResponse:
        try:
            # 1️⃣ Obtenemos los documentos
            if ejercicio is not None:
                docs = await self.repository.get_by_fields({"ejercicio": ejercicio})
            else:
                docs = await self.repository.get_all()

            if not docs:
                raise HTTPException(
                    status_code=404, detail="No se encontraron registros"
                )

            # 2️⃣ Convertimos a DataFrame
            df = sanitize_dataframe_for_json(pd.DataFrame(docs))
            df = df.drop(columns=["_id"])

            # # 3️⃣ Subimos a Google Sheets si se solicita
            # if upload_to_google_sheets:
            #     gs_service = GoogleSheets()
            #     gs_service.to_google_sheets(
            #         df=df,
            #         spreadsheet_key="1KKeeoop_v_Nf21s7eFp4sS6SmpxRZQ9DPa1A5wVqnZ0",
            #         wks_name="control_ejecucion_anual_db",
            #     )

            # 3️⃣ Escribimos a un buffer Excel en memoria
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="rf610")

            buffer.seek(0)

            # 4️⃣ Devolvemos StreamingResponse
            file_name = f"rf610_{ejercicio or 'all'}.xlsx"
            headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
            return StreamingResponse(
                buffer,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers=headers,
            )
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rf610 from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's rf610 from the database",
            )


Rf610ServiceDependency = Annotated[Rf610Service, Depends()]
