#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Reporte Ejecución Obras
Data required:
    - Icaro (CARGA, ESTRUCTURAS, OBRAS, PROVEEDORES)
    - SIIF rf610
    - SIIF ri102
    - SGV Saldos Barrios Evolución
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1Hmb7xmzhZBoicnL5_tN7mr1kOj-r3gw8lCkPErR8Xd4 (Planillometro Contabilidad)

"""

__all__ = [
    "ReportePlanillometroContabilidadService",
    "ReportePlanillometroContabilidadServiceDependency",
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
from ...sgv.handlers import login as sgv_login
from ...sgv.handlers import logout as sgv_logout
from ...sgv.handlers.saldos_barrios_evolucion import SaldosBarriosEvolucion
from ...siif.handlers import (
    Rf602,
    Rf610,
    login,
    logout,
)
from ...siif.services import PlanillometroHistServiceDependency
from ...utils import (
    GoogleExportResponse,
    RouteReturnSchema,
    export_multiple_dataframes_to_excel,
    get_r_icaro_path,
    upload_multiple_dataframes_to_google_sheets,
)
from ..handlers import (
    get_icaro_planillometro_contabilidad,
    get_sgv_saldos_barrios_evolucion,
)
from ..repositories.reporte_modulos_basicos import (
    ReporteModulosBasicosIcaroRepositoryDependency,
)
from ..schemas.reporte_planillometro_contabilidad import (
    ReportePlanillometroContabildadParams,
    ReportePlanillometroContabilidadSyncParams,
)


# --------------------------------------------------
@dataclass
class ReportePlanillometroContabilidadService:
    reporte_mod_bas_icaro_repo: ReporteModulosBasicosIcaroRepositoryDependency
    planillometro_hist_service: PlanillometroHistServiceDependency
    sgv_saldos_barrios_evolucion_handler: SaldosBarriosEvolucion = field(
        init=False
    )  # No se pasa como argumento
    siif_rf610_handler: Rf610 = field(init=False)  # No se pasa como argumento
    siif_rf602_handler: Rf602 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_planillometro_from_source(
        self,
        params: ReportePlanillometroContabilidadSyncParams = None,
    ) -> List[RouteReturnSchema]:
        """Downloads a report from SIIF, processes it, validates the data,
        and stores it in MongoDB if valid.

        Args:
            ejercicio (int, optional): The fiscal year for the report. Defaults to the current year.

        Returns:
            RouteReturnSchema
        """
        if (
            params.siif_username is None
            or params.siif_password is None
            or params.sgv_username is None
            or params.sgv_password is None
        ):
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

                # 🔹 SGV Barrios Evolución
                connect_sgv = await sgv_login(
                    username=params.sgv_username,
                    password=params.sgv_password,
                    playwright=p,
                    headless=False,
                )
                self.sgv_saldos_barrios_evolucion_handler = SaldosBarriosEvolucion(
                    sgv=connect_sgv
                )
                for ejercicio in ejercicios:
                    partial_schema = await self.sgv_saldos_barrios_evolucion_handler.download_and_sync_validated_to_repository(
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
                    await sgv_logout(connect=connect_sgv)
                except Exception as e:
                    logger.warning(f"Logout falló o browser ya cerrado: {e}")
                return return_schema

    # # -------------------------------------------------
    # async def generate_all(
    #     self, params: ReporteModulosBasicosIcaroParams
    # ) -> List[RouteReturnSchema]:
    #     """
    #     Compute all controls for the given params.
    #     """
    #     return_schema = []
    #     try:
    #         # 🔹 Control Anual
    #         partial_schema = await self.generate_reporte_modulos_basicos_icaro(
    #             params=params
    #         )
    #         return_schema.append(partial_schema)

    #     except ValidationError as e:
    #         logger.error(f"Validation Error: {e}")
    #         raise HTTPException(
    #             status_code=400,
    #             detail="Invalid response format from Reporte Módulos Básicos",
    #         )
    #     except Exception as e:
    #         logger.error(f"Error in compute_all: {e}")
    #         raise HTTPException(
    #             status_code=500,
    #             detail="Error in compute_all",
    #         )
    #     finally:
    #         return return_schema

    # --------------------------------------------------
    async def _build_dataframes_to_export(
        self,
        params: ReportePlanillometroContabildadParams,
    ) -> list[tuple[pd.DataFrame, str]]:
        ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))

        # control_banco_docs = await self.control_banco_repo.find_by_filter(
        #     filters={"ejercicio": {"$in": ejercicios}},
        # )

        # if not control_banco_docs:
        #     raise HTTPException(status_code=404, detail="No se encontraron registros")

        # siif = pd.DataFrame()
        # sscc = pd.DataFrame()

        # for ejercicio in ejercicios:
        #     siif = pd.concat(
        #         [siif, await self.generate_banco_siif(ejercicio=ejercicio)],
        #         ignore_index=True,
        #     )
        #     sscc = pd.concat(
        #         [sscc, await self.generate_banco_sscc(ejercicio=ejercicio)],
        #         ignore_index=True,
        #     )

        planillometro = await get_icaro_planillometro_contabilidad(
            ejercicio=ejercicios[-1],
            ultimos_ejercicios=5,
            include_pa6=False,
            desagregar_desc_subprog=False,
        )
        planillometro["alta"] = planillometro["alta"].astype(str)
        planillometro = planillometro.rename(
            columns={
                "desc_programa": "desc_prog",
                "desc_proyecto": "desc_proy",
                "desc_actividad": "desc_act",
            }
        )

        sgv = await get_sgv_saldos_barrios_evolucion()
        sgv["ejercicio"] = sgv["ejercicio"].astype(str)
        sgv["cod_barrio"] = sgv["cod_barrio"].astype(int)
        sgv = sgv.sort_values(by=["ejercicio", "cod_barrio"], ascending=[True, True])

        # icaro = await get_icaro_planillometro_contabilidad(
        #     ejercicio=ejercicios[-1],
        #     ultimos_ejercicios=5,
        #     include_pa6=False,
        #     incluir_desc_subprog = False,
        #     incluir_obras_desagregadas=True,
        #     agregar_acum_2008 = False,
        # )

        return [
            # (pd.DataFrame(control_banco_docs), "siif_vs_sscc_db"),
            # (sscc, "sscc_db"),
            (planillometro, "bd_planillometro"),
            (sgv, "bd_recuperos"),
            # (icaro, "icaro_planillometro_new"),
        ]

    # -------------------------------------------------
    async def export_all_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ReportePlanillometroContabildadParams = None,
    ) -> StreamingResponse:
        df_sheet_pairs = await self._build_dataframes_to_export(params)

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=df_sheet_pairs,
            filename="planillometro_contabilidad.xlsx",
            spreadsheet_key="1Hmb7xmzhZBoicnL5_tN7mr1kOj-r3gw8lCkPErR8Xd4",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # -------------------------------------------------
    async def export_all_from_db_to_google(
        self,
        params: ReportePlanillometroContabildadParams = None,
    ) -> GoogleExportResponse:
        df_sheet_pairs = await self._build_dataframes_to_export(params)

        return upload_multiple_dataframes_to_google_sheets(
            df_sheet_pairs=df_sheet_pairs,
            spreadsheet_key="1Hmb7xmzhZBoicnL5_tN7mr1kOj-r3gw8lCkPErR8Xd4",
            title="Planillometros",
        )


ReportePlanillometroContabilidadServiceDependency = Annotated[
    ReportePlanillometroContabilidadService, Depends()
]
