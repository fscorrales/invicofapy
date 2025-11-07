#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control Banco SIIF vs SSCC (Banco Real)
Data required:
    - SIIF rvicon03
    - SIIF rcocc31
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1CRQjzIVzHKqsZE8_E1t8aRQDfWfZALhbe64WcxHiSM4
"""

__all__ = ["ControlBancoService", "ControlBancoServiceDependency"]

from dataclasses import dataclass, field
from typing import Annotated, List

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
from ...sscc.services import BancoINVICOServiceDependency, CtasCtesServiceDependency
from ...utils import (
    RouteReturnSchema,
    export_multiple_dataframes_to_excel,
)
from ..handlers import (
    get_banco_invico_unified_cta_cte,
    get_siif_rcocc31,
    get_siif_rvicon03,
)
from ..schemas.control_banco import (
    ControlBancoParams,
    ControlBancoSyncParams,
)


# --------------------------------------------------
@dataclass
class ControlBancoService:
    siif_rcocc31_handler: Rcocc31 = field(init=False)  # No se pasa como argumento
    siif_rvicon03_handler: Rvicon03 = field(init=False)  # No se pasa como argumento
    sscc_banco_invico_service: BancoINVICOServiceDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency

    # -------------------------------------------------
    async def sync_control_banco_from_source(
        self,
        params: ControlBancoSyncParams = None,
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
                logger.info(
                    f"Se Bajaran las siguientes cuentas contables: {cuentas_contables}"
                )
                for cta_contable in cuentas_contables:
                    partial_schema = await self.siif_rcocc31_handler.download_and_sync_validated_to_repository(
                        ejercicio=params.ejercicio,
                        cta_contable=cta_contable,
                    )
                    return_schema.append(partial_schema)

                # 🔹Banco INVICO
                partial_schema = (
                    await self.sscc_banco_invico_service.sync_banco_invico_from_sscc(
                        username=params.sscc_username,
                        password=params.sscc_password,
                        params=params,
                    )
                )
                return_schema.extend(partial_schema)

                # 🔹Ctas Ctes
                partial_schema = (
                    await self.sscc_ctas_ctes_service.sync_ctas_ctes_from_excel(
                        excel_path=params.ctas_ctes_excel_path,
                    )
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
    async def generate_banco_siif(self, ejercicio: int) -> pd.DataFrame:
        df = await get_siif_rcocc31(ejercicio=ejercicio)

        if df.empty:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        # Solo incluimos los registros que tienen movimientos en la cuenta 1112-2-6
        df = df.loc[
            df["nro_entrada"].isin(
                df.loc[df["cta_contable"] == "1112-2-6"]["nro_entrada"].unique()
            )
        ]

        df["nro_entrada"] = pd.to_numeric(df["nro_entrada"], errors="coerce")
        df = df.sort_values(
            ["nro_entrada", "debitos", "creditos", "cta_contable"],
            ascending=[True, False, False, True],
        )
        df["nro_entrada"] = df["nro_entrada"].astype(str)
        df = df.loc[
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

        return df

    # --------------------------------------------------
    async def generate_banco_debitos(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        df = await get_banco_invico_unified_cta_cte(ejercicio=ejercicio)
        return df

    # -------------------------------------------------
    async def export_all_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ControlBancoParams = None,
    ) -> StreamingResponse:
        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (
                    await self.generate_banco_debitos(ejercicio=params.ejercicio),
                    "sscc_db",
                ),
                (
                    await self.generate_banco_siif(ejercicio=params.ejercicio),
                    "siif_db",
                ),
            ],
            filename="control_banco.xlsx",
            spreadsheet_key="1CRQjzIVzHKqsZE8_E1t8aRQDfWfZALhbe64WcxHiSM4",
            upload_to_google_sheets=upload_to_google_sheets,
        )


ControlBancoServiceDependency = Annotated[ControlBancoService, Depends()]
