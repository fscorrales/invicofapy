#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Reporte Ejecución de Gastos
Data required:
    - SIIF rf610
    - SIIF ri102
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1SRmgep84KGJNj_nKxiwXLe28gVUiIu2Uha4j_C7BzeU (Ejecución Gastos)
"""

__all__ = [
    "ReporteEjecucionGastosService",
    "ReporteEjecucionGastosServiceDependency",
]


from dataclasses import dataclass, field
from typing import Annotated, List

import pandas as pd
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import logger
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
    upload_multiple_dataframes_to_google_sheets,
)
from ..handlers import (
    get_siif_desc_pres,
    get_siif_rf602,
)
from ..schemas.reporte_ejecucion_gastos import (
    ReporteEjecucionGastosParams,
    ReporteEjecucionGastosSyncParams,
)


# --------------------------------------------------
@dataclass
class ReporteEjecucionGastosService:
    siif_rf610_handler: Rf610 = field(init=False)  # No se pasa como argumento
    siif_rf602_handler: Rf602 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_ejecucion_gastos_from_source(
        self,
        params: ReporteEjecucionGastosSyncParams = None,
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

                # # 🔹 Icaro
                # path = os.path.join(get_r_icaro_path(), "ICARO.sqlite")
                # migrator = IcaroMongoMigrator(sqlite_path=path)
                # return_schema.append(await migrator.migrate_carga())
                # return_schema.append(await migrator.migrate_estructuras())
                # return_schema.append(await migrator.migrate_proveedores())
                # return_schema.append(await migrator.migrate_obras())

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
    async def _build_dataframes_to_export(
        self,
        params: ReporteEjecucionGastosParams = None,
    ) -> list[tuple[pd.DataFrame, str]]:
        ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))

        # control_banco_docs = await self.control_banco_repo.find_by_filter(
        #     filters={"ejercicio": {"$in": ejercicios}},
        # )

        # if not control_banco_docs:
        #     raise HTTPException(status_code=404, detail="No se encontraron registros")

        # siif = pd.DataFrame()
        siif_ejec_gastos = pd.DataFrame()

        for ejercicio in ejercicios:
            siif_ejec_gastos = pd.concat(
                [
                    siif_ejec_gastos,
                    await self.generate_siif_pres_with_desc(ejercicio=ejercicio),
                ],
                ignore_index=True,
            )
        #     sscc = pd.concat(
        #         [sscc, await self.generate_banco_sscc(ejercicio=ejercicio)],
        #         ignore_index=True,
        #     )

        # planillometro = await get_icaro_planillometro_contabilidad(
        #     ejercicio=ejercicios[-1],
        #     ultimos_ejercicios=5,
        #     include_pa6=False,
        #     incluir_desc_subprog=False,
        # )
        # planillometro["alta"] = planillometro["alta"].astype(str)
        # planillometro = planillometro.rename(
        #     columns={
        #         "desc_programa": "desc_prog",
        #         "desc_proyecto": "desc_proy",
        #         "desc_actividad": "desc_act",
        #     }
        # )

        siif_ejec_gastos = await self.generate_siif_pres_with_desc(
            ejercicio=ejercicios[-1]
        )

        return [
            # (pd.DataFrame(control_banco_docs), "siif_vs_sscc_db"),
            # (sscc, "sscc_db"),
            # (planillometro, "bd_planillometro"),
            (siif_ejec_gastos, "siif_ejec_gastos"),
        ]

    # -------------------------------------------------
    async def export_all_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ReporteEjecucionGastosParams = None,
    ) -> StreamingResponse:
        df_sheet_pairs = await self._build_dataframes_to_export(params)

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=df_sheet_pairs,
            filename="planillometros.xlsx",
            spreadsheet_key="1SRmgep84KGJNj_nKxiwXLe28gVUiIu2Uha4j_C7BzeU",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # -------------------------------------------------
    async def export_all_from_db_to_google(
        self,
        params: ReporteEjecucionGastosParams = None,
    ) -> GoogleExportResponse:
        df_sheet_pairs = await self._build_dataframes_to_export(params)

        return upload_multiple_dataframes_to_google_sheets(
            df_sheet_pairs=df_sheet_pairs,
            spreadsheet_key="1SRmgep84KGJNj_nKxiwXLe28gVUiIu2Uha4j_C7BzeU",
            title="Ejecución Gastos",
        )

    # --------------------------------------------------
    async def generate_siif_pres_with_desc(self, ejercicio: int) -> pd.DataFrame:
        df = await get_siif_rf602(ejercicio=ejercicio)

        if df.empty:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

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

        # df["programa"] = df["programa"].astype(int)
        # df["fuente"] = df["fuente"].astype(int)

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


ReporteEjecucionGastosServiceDependency = Annotated[
    ReporteEjecucionGastosService, Depends()
]
