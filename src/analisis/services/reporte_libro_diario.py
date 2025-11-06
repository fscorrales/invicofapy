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
    async def sync_libro_diario_from_source(
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
                cuentas_contables = await get_siif_rvicon03(ejercicio=params.ejercicio)
                cuentas_contables = cuentas_contables["cta_contable"].unique()
                logger.info(f"Se Bajaran las siguientes cuentas contables: {cuentas_contables}")
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

    # --------------------------------------------------
    async def generate_libro_diario(self, ejercicio: int) -> pd.DataFrame:
        libro_diario_df = await get_siif_rcocc31(ejercicio=ejercicio)

        if libro_diario_df.empty:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        libro_diario_df["nro_entrada"] = pd.to_numeric(libro_diario_df["nro_entrada"], errors="coerce")
        libro_diario_df = libro_diario_df.sort_values(
            ["nro_entrada", "debitos", "creditos", "cta_contable"], 
            ascending=[True, False, False, True]
        )
        libro_diario_df["nro_entrada"] = libro_diario_df["nro_entrada"].astype(str)
        libro_diario_df = libro_diario_df.loc[
            :,
            [
                "ejercicio",
                "mes",
                "fecha",
                "fecha_aprobado",
                "nro_entrada",
                "nro_original",
                "cta_contable",
                "tipo_comprobante",
                "debitos",
                "creditos",
                "saldo",
                "auxiliar_1",
                "auxiliar_2",
            ],
        ]

        return libro_diario_df

    # -------------------------------------------------
    async def export_all_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ReporteLibroDiarioParams = None,
    ) -> StreamingResponse:

        ultimos_ejercicios = list(range(params.ejercicio-1, params.ejercicio + 1))

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (
                    pd.DataFrame(
                        await get_siif_rvicon03(
                            filters={"ejercicio": {"$in": ultimos_ejercicios}}
                        )
                    ),
                    "sumas_y_saldos_db",
                ),
                (
                    await self.generate_libro_diario(ejercicio=params.ejercicio),
                    "libro_diario_db",
                ),
            ],
            filename="reportes_libro_diario.xlsx",
            spreadsheet_key="18eKGv_JTmeolG029g-LpUIxGPPhqRN0KUVhU44q8Tw0",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # -------------------------------------------------
    async def export_libro_diario_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ReporteLibroDiarioParams = None,
    ) -> StreamingResponse:

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (
                    await self.generate_libro_diario(ejercicio=params.ejercicio),
                    "libro_diario_db",
                ),
            ],
            filename="reporte_libro_diario.xlsx",
            spreadsheet_key="18eKGv_JTmeolG029g-LpUIxGPPhqRN0KUVhU44q8Tw0",
            upload_to_google_sheets=upload_to_google_sheets,
        )


ReporteLibroDiarioServiceDependency = Annotated[ReporteLibroDiarioService, Depends()]
