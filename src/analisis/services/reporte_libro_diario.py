#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Libro Diario
Data required:
    - SIIF rvicon03
    - SIIF rcocc31
Google Sheet:
    - https://docs.google.com/spreadsheets/d/18eKGv_JTmeolG029g-LpUIxGPPhqRN0KUVhU44q8Tw0
"""

__all__ = ["ReporteLibroDiarioService", "ReporteLibroDiarioServiceDependency"]

from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, List

import numpy as np
import pandas as pd
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import logger
from ...siif.handlers import (
    Rcocc31,
    Rvicon03,
    login,
    logout,
)
from ...utils import (
    RouteReturnSchema,
    export_multiple_dataframes_to_excel,
)
from ..handlers import (
    get_siif_rcocc31,
    get_siif_rvicon03,
)
from ..schemas.reporte_libro_diario import (
    ReporteLibroDiarioParams,
    ReporteLibroDiarioSyncParams,
)


# --------------------------------------------------
@dataclass
class ReporteLibroDiarioService:
    siif_rcocc31_handler: Rcocc31 = field(init=False)  # No se pasa como argumento
    siif_rvicon03_handler: Rvicon03 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_reporte_libro_diario_from_source(
        self,
        params: ReporteLibroDiarioSyncParams = None,
    ) -> List[RouteReturnSchema]:
        """Downloads a report from all sources, processes it, validates the data,
        and stores it in MongoDB if valid.

        Args:
            ejercicio (int, optional): The fiscal year for the report. Defaults to the current year.

        Returns:
            RouteReturnSchema
        """
        if params.siif_username is None or params.siif_password is None:
            raise HTTPException(
                status_code=401,
                detail="Missing username or password",
            )
        return_schema = []
        async with async_playwright() as p:
            connect_siif = await login(
                username=params.siif_username,
                password=params.siif_password,
                playwright=p,
                headless=False,
            )
            try:
                # 🔹Rvicon03
                self.siif_rvicon03_handler = Rvicon03(siif=connect_siif)
                await self.siif_rvicon03_handler.go_to_reports()
                partial_schema = await self.siif_rvicon03_handler.download_and_sync_validated_to_repository(
                    ejercicio=params.ejercicio,
                )
                return_schema.append(partial_schema)

                # 🔹 Rcocc31
                self.siif_rcocc31_handler = Rcocc31(siif=connect_siif)
                cuentas_contables = (
                    await get_siif_rvicon03(ejercicio=params.ejercicio)["cta_contable"]
                    .unique()
                    .tolist()
                )
                print(cuentas_contables)
                for cta_contable in cuentas_contables:
                    partial_schema = await self.siif_rcocc31_handler.download_and_sync_validated_to_repository(
                        ejercicio=params.ejercicio,
                        cta_contable=cta_contable,
                    )
                    return_schema.append(partial_schema)

            except ValidationError as e:
                logger.error(f"Validation Error: {e}")
                raise HTTPException(status_code=400, detail="Invalid response format")
            except Exception as e:
                logger.error(f"Error during report processing: {e}")
                raise HTTPException(
                    status_code=401,
                    detail="Invalid credentials or unable to authenticate",
                )
            finally:
                try:
                    await logout(connect=connect_siif)
                except Exception as e:
                    logger.warning(f"Logout falló o browser ya cerrado: {e}")
                return return_schema

    # -------------------------------------------------
    async def export_libro_diario_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ReporteLibroDiarioParams = None,
    ) -> StreamingResponse:
        reporte_libro_diario_docs = await get_siif_rcocc31(ejercicio=params.ejercicio)

        if not reporte_libro_diario_docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (
                    pd.DataFrame(reporte_libro_diario_docs),
                    "libro_diario_db",
                ),
            ],
            filename="reporte_libro_diario.xlsx",
            spreadsheet_key="18eKGv_JTmeolG029g-LpUIxGPPhqRN0KUVhU44q8Tw0",
            upload_to_google_sheets=upload_to_google_sheets,
        )


ReporteLibroDiarioServiceDependency = Annotated[ReporteLibroDiarioService, Depends()]
