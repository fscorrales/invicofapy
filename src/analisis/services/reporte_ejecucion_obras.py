#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Reporte Ejecución Obras
Data required:
    - Icaro (CARGA, ESTRUCTURAS, OBRAS, PROVEEDORES)
    - SIIF rf610
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1NgOT665gNX53IdsXvAhlNPLslVJd5SG--geJm7aDtUA (Ejecución Obras)

"""

__all__ = [
    "ReporteEjecucionObrasService",
    "ReporteEjecucionObrasServiceDependency",
]

import os
from dataclasses import dataclass, field
from typing import Annotated, List

import pandas as pd
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import logger
from ...icaro.handlers import IcaroMongoMigrator
from ...siif.handlers import (
    Rf602,
    Rf610,
    login,
    logout,
)
from ...utils import (
    GoogleExportResponse,
    RouteReturnSchema,
    export_multiple_dataframes_to_excel,
    get_r_icaro_path,
    upload_multiple_dataframes_to_google_sheets,
)
from ..handlers import get_full_icaro_carga_desc, get_siif_ppto_gto_con_desc
from ..repositories.reporte_modulos_basicos import (
    ReporteModulosBasicosIcaroRepositoryDependency,
)
from ..schemas.reporte_ejecucion_obras import (
    ReporteEjecucionObrasParams,
    ReporteEjecucionObrasSyncParams,
)


# --------------------------------------------------
@dataclass
class ReporteEjecucionObrasService:
    reporte_mod_bas_icaro_repo: ReporteModulosBasicosIcaroRepositoryDependency
    siif_rf610_handler: Rf610 = field(init=False)  # No se pasa como argumento
    siif_rf602_handler: Rf602 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_ejecucion_obras_from_source(
        self,
        params: ReporteEjecucionObrasSyncParams = None,
    ) -> List[RouteReturnSchema]:
        """Downloads a report from SIIF, processes it, validates the data,
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
                ejercicios = list(
                    range(params.ejercicio_desde, params.ejercicio_hasta + 1)
                )

                # 🔹 RF610
                self.siif_rf610_handler = Rf610(siif=connect_siif)
                await self.siif_rf610_handler.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rf610_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio)
                    )
                    return_schema.append(partial_schema)

                # 🔹 RF602
                self.siif_rf602_handler = Rf602(siif=connect_siif)
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rf602_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio)
                    )
                    return_schema.append(partial_schema)

                # 🔹 Icaro
                path = os.path.join(get_r_icaro_path(), "ICARO.sqlite")
                migrator = IcaroMongoMigrator(sqlite_path=path)
                return_schema.append(await migrator.migrate_carga())
                return_schema.append(await migrator.migrate_estructuras())
                return_schema.append(await migrator.migrate_proveedores())
                return_schema.append(await migrator.migrate_obras())

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
    async def generate_all(
        self, params: ReporteEjecucionObrasParams = None
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Reporte Ejecucion Obras Icaro
            partial_schema = await self.generate_reporte_ejecucion_obras_icaro(
                params=params
            )
            return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Reporte Ejecucion Obras",
            )
        except Exception as e:
            logger.error(f"Error in compute_all: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in compute_all",
            )
        finally:
            return return_schema

    # --------------------------------------------------
    async def _build_dataframes_to_export(
        self,
        params: ReporteEjecucionObrasParams = None,
    ) -> list[tuple[pd.DataFrame, str]]:
        ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))

        # control_banco_docs = await self.control_banco_repo.find_by_filter(
        #     filters={"ejercicio": {"$in": ejercicios}},
        # )

        # if not control_banco_docs:
        #     raise HTTPException(status_code=404, detail="No se encontraron registros")

        icaro = pd.DataFrame()
        siif = pd.DataFrame()

        for ejercicio in ejercicios:
            icaro = pd.concat(
                [
                    icaro,
                    await self.generate_reporte_ejecucion_obras_icaro(
                        ejercicio=ejercicio
                    ),
                ],
                ignore_index=True,
            )
            siif = pd.concat(
                [
                    siif,
                    await get_siif_ppto_gto_con_desc(ejercicio=ejercicio),
                ],
                ignore_index=True,
            )

        return [
            (siif, "bd_siif"),
            (icaro, "bd_icaro"),
        ]

    # -------------------------------------------------
    async def export_all_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ReporteEjecucionObrasParams = None,
    ) -> StreamingResponse:
        df_sheet_pairs = await self._build_dataframes_to_export(params)

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=df_sheet_pairs,
            filename="ejecucion_obras.xlsx",
            spreadsheet_key="1NgOT665gNX53IdsXvAhlNPLslVJd5SG--geJm7aDtUA",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # -------------------------------------------------
    async def export_all_from_db_to_google(
        self,
        params: ReporteEjecucionObrasParams = None,
    ) -> GoogleExportResponse:
        df_sheet_pairs = await self._build_dataframes_to_export(params)

        return upload_multiple_dataframes_to_google_sheets(
            df_sheet_pairs=df_sheet_pairs,
            spreadsheet_key="1NgOT665gNX53IdsXvAhlNPLslVJd5SG--geJm7aDtUA",
            title="Ejecución Obras",
        )

    # --------------------------------------------------
    async def generate_reporte_ejecucion_obras_icaro(
        self,
        ejercicio: int,
    ) -> pd.DataFrame:
        df = await get_full_icaro_carga_desc(
            ejercicio=ejercicio,
            es_desc_siif=False,
            es_neto_pa6=True,
            es_ejercicio_to=False,
        )

        if df.empty:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        df["estructura"] = df["actividad"] + "-" + df["partida"]
        df = (
            df.groupby(
                [
                    "ejercicio",
                    "estructura",
                    "fuente",
                    "desc_programa",
                    "desc_subprograma",
                    "desc_proyecto",
                    "desc_actividad",
                    "desc_obra",
                ]
            )
            .importe.sum()
            .to_frame()
            .reset_index()
        )
        df = df.rename(
            columns={
                "desc_programa": "nro_desc_programa",
                "desc_subprograma": "nro_desc_subprograma",
                "desc_proyecto": "nro_desc_proyecto",
                "desc_actividad": "nro_desc_actividad",
            }
        )
        df["nro_programa"] = df["estructura"].str[0:2]
        df["nro_subprograma"] = df["estructura"].str[0:5]
        df["nro_proyecto"] = df["estructura"].str[0:8]
        df["nro_actividad"] = df["estructura"].str[0:11]
        df["nro_partida"] = df["estructura"].str[12:15]
        df["desc_programa"] = df["nro_desc_programa"].str[5:]
        df["desc_subprograma"] = df["nro_desc_subprograma"].str[5:]
        df["desc_proyecto"] = df["nro_desc_proyecto"].str[5:]
        df["desc_actividad"] = df["nro_desc_actividad"].str[5:]
        return df


ReporteEjecucionObrasServiceDependency = Annotated[
    ReporteEjecucionObrasService, Depends()
]
