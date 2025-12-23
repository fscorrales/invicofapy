#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Reporte Ejecución Obras
Data required:
    - Icaro
    - SIIF rf602
    - SIIF rf610
    - SIIF ri102
    - SIIF rfp_p605b
    - SSCC ctas_ctes (manual data)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1hJyBOkA8sj5otGjYGVOzYViqSpmv_b4L8dXNju_GJ5Q
    - https://docs.google.com/spreadsheets/d/1AYeTncc1ewP8Duj13t7o6HCwAHNEWILRMNQiZHAs82I

"""

__all__ = [
    "ReporteFormulacionPresupuestoService",
    "ReporteFormulacionPresupuestoServiceDependency",
]

import datetime as dt
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
    RfpP605b,
    Ri102,
    login,
    logout,
)
from ...siif.services import PlanillometroHistServiceDependency
from ...utils import (
    RouteReturnSchema,
    export_multiple_dataframes_to_excel,
    get_r_icaro_path,
)
from ..handlers import (
    get_icaro_planillometro_contabilidad,
    get_siif_desc_pres,
    get_siif_rf602,
    get_siif_rfp_p605b,
    get_siif_ri102,
)
from ..schemas.reporte_formulacion_presupuesto import (
    ReporteFormulacionPresupuestoParams,
    ReporteFormulacionPresupuestoSyncParams,
)


# --------------------------------------------------
@dataclass
class ReporteFormulacionPresupuestoService:
    planillometro_hist_service: PlanillometroHistServiceDependency
    siif_rf602_handler: Rf602 = field(init=False)  # No se pasa como argumento
    siif_rf610_handler: Rf610 = field(init=False)  # No se pasa como argumento
    siif_ri102_handler: Ri102 = field(init=False)  # No se pasa como argumento
    siif_rfp_p605b_handler: RfpP605b = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_formulacion_presupuesto_from_source(
        self,
        username: str,
        password: str,
        params: ReporteFormulacionPresupuestoSyncParams = None,
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
        async with async_playwright() as p:
            connect_siif = await login(
                username=username,
                password=password,
                playwright=p,
                headless=False,
            )
            try:
                # 🔹 RF610
                self.siif_rf610_handler = Rf610(siif=connect_siif)
                await self.siif_rf610_handler.go_to_reports()
                partial_schema = await self.siif_rf610_handler.download_and_sync_validated_to_repository(
                    ejercicio=int(params.ejercicio)
                )
                return_schema.append(partial_schema)

                # 🔹 RF602
                self.siif_rf602_handler = Rf602(siif=connect_siif)
                partial_schema = await self.siif_rf602_handler.download_and_sync_validated_to_repository(
                    ejercicio=int(params.ejercicio)
                )
                return_schema.append(partial_schema)

                # 🔹 Ri102
                self.siif_ri102_handler = Ri102(siif=connect_siif)
                partial_schema = await self.siif_ri102_handler.download_and_sync_validated_to_repository(
                    ejercicio=int(params.ejercicio)
                )
                return_schema.append(partial_schema)

                # 🔹 Rfp_p605b
                self.siif_rfp_p605b_handler = RfpP605b(siif=connect_siif)
                partial_schema = await self.siif_rfp_p605b_handler.download_and_sync_validated_to_repository(
                    ejercicio=int(params.ejercicio) + 1
                )
                return_schema.append(partial_schema)

                # 🔹 Icaro
                path = os.path.join(get_r_icaro_path(), "ICARO.sqlite")
                migrator = IcaroMongoMigrator(sqlite_path=path)
                return_schema.append(await migrator.migrate_carga())
                return_schema.append(await migrator.migrate_estructuras())
                return_schema.append(await migrator.migrate_proveedores())

                # 🔹 Planillometro Histórico (PATRICIA)
                partial_schema = await self.planillometro_hist_service.sync_planillometro_hist_from_excel(
                    excel_path=params.planillometro_hist_excel_path,
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
    async def export_all_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ReporteFormulacionPresupuestoParams = None,
    ) -> StreamingResponse:
        ejercicio_actual = dt.datetime.now().year
        ultimos_ejercicios = list(range(ejercicio_actual - 2, ejercicio_actual + 2))
        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (
                    pd.DataFrame(
                        await get_siif_ri102(
                            filters={"ejercicio": {"$in": ultimos_ejercicios}}
                        )
                    ),
                    "siif_recursos_cod",
                ),
                (
                    await self.generate_siif_pres_with_desc(ejercicio=params.ejercicio),
                    "siif_ejec_gastos",
                ),
                (
                    await get_icaro_planillometro_contabilidad(
                        ejercicio=params.ejercicio,
                        ultimos_ejercicios=5,
                        include_pa6=True,
                    ),
                    "planillometro_contabilidad",
                ),
                (
                    pd.DataFrame(
                        await get_siif_rfp_p605b(
                            filters={"ejercicio": {"$in": ultimos_ejercicios}}
                        )
                    ),
                    "siif_carga_form_gastos",
                ),
            ],
            filename="reportes_formulacion_presupuesto.xlsx",
            spreadsheet_key="1hJyBOkA8sj5otGjYGVOzYViqSpmv_b4L8dXNju_GJ5Q",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # --------------------------------------------------
    async def generate_siif_pres_with_desc(self, ejercicio: int) -> pd.DataFrame:
        ultimos_ejercicios = list(range(ejercicio - 3, ejercicio + 1))
        df = await get_siif_rf602(filters={"ejercicio": {"$in": ultimos_ejercicios}})
        df = df.sort_values(by=["ejercicio", "estructura"], ascending=[False, True])
        df = df.merge(
            await get_siif_desc_pres(ejercicio_to=ejercicio),
            how="left",
            on="estructura",
            copy=False,
        )
        df.drop(
            labels=[
                "org",
                "pendiente",
                "subprograma",
                "proyecto",
                "actividad",
            ],
            axis=1,
            inplace=True,
        )

        df["programa"] = df["programa"].astype(int)
        df["fuente"] = df["fuente"].astype(int)

        first_cols = [
            "ejercicio",
            "estructura",
            "partida",
            "fuente",
            "desc_programa",
            "desc_subprograma",
            "desc_proyecto",
            "desc_actividad",
            "programa",
            "grupo",
        ]
        df = df.loc[:, first_cols].join(df.drop(first_cols, axis=1))

        df = pd.DataFrame(df)
        df.reset_index(drop=True, inplace=True)
        return df

    # -------------------------------------------------
    async def export_planillometro_contabilidad(
        self,
        upload_to_google_sheets: bool = True,
        params: ReporteFormulacionPresupuestoParams = None,
    ) -> StreamingResponse:
        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (
                    await get_icaro_planillometro_contabilidad(
                        ejercicio=params.ejercicio,
                        ultimos_ejercicios=5,
                        include_pa6=True,
                    ),
                    "planillometro_contabilidad",
                ),
            ],
            filename="planillometro_contabilidad_formulacion_presupuesto.xlsx",
            spreadsheet_key="1hJyBOkA8sj5otGjYGVOzYViqSpmv_b4L8dXNju_GJ5Q",
            upload_to_google_sheets=upload_to_google_sheets,
        )


ReporteFormulacionPresupuestoServiceDependency = Annotated[
    ReporteFormulacionPresupuestoService, Depends()
]
