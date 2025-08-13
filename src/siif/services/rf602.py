__all__ = ["Rf602Service", "Rf602ServiceDependency"]

import os
from dataclasses import dataclass, field
from io import BytesIO
from typing import Annotated, List

import pandas as pd
from fastapi import Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import logger
from ...utils import (
    BaseFilterParams,
    RouteReturnSchema,
    sanitize_dataframe_for_json,
    validate_and_extract_data_from_df,
)
from ..handlers import Rf602
from ..repositories import Rf602RepositoryDependency
from ..schemas import Rf602Document, Rf602Params, Rf602Report


# -------------------------------------------------
@dataclass
class Rf602Service:
    repository: Rf602RepositoryDependency
    rf602: Rf602 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.rf602 = Rf602()

    # -------------------------------------------------
    async def sync_rf602_from_siif(
        self,
        username: str,
        password: str,
        params: Rf602Params = None,
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
                await self.rf602.login(
                    username=username,
                    password=password,
                    playwright=p,
                    headless=False,
                )
                await self.rf602.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = (
                        await self.rf602.download_and_sync_validated_to_repository(
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
                if hasattr(self.rf602, "logout"):
                    await self.rf602.logout()
                return return_schema

    # -------------------------------------------------
    async def get_rf602_from_db(self, params: BaseFilterParams) -> List[Rf602Document]:
        try:
            return await self.repository.find_with_filter_params(params=params)
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rf602 from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's rf602 from the database",
            )

    # -------------------------------------------------
    async def sync_rf602_from_sqlite(self, sqlite_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = await self.rf602.sync_validated_sqlite_to_repository(sqlite_path=sqlite_path)
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
    async def export_rf602_from_db(self, ejercicio: int = None) -> StreamingResponse:
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
                df.to_excel(writer, index=False, sheet_name="rf602")

            buffer.seek(0)

            # 4️⃣ Devolvemos StreamingResponse
            file_name = f"rf602_{ejercicio or 'all'}.xlsx"
            headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
            return StreamingResponse(
                buffer,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers=headers,
            )
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rf602 from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's rf602 from the database",
            )


Rf602ServiceDependency = Annotated[Rf602Service, Depends()]
