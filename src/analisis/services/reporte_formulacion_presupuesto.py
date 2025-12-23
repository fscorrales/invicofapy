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

import numpy as np
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
    get_icaro_carga,
    get_icaro_estructuras_desc,
    get_icaro_proveedores,
    get_planillometro_hist,
    get_siif_desc_pres,
    get_siif_rf602,
    get_siif_rfp_p605b,
    get_siif_ri102,
    generate_icaro_carga_desc,
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
                    await self.generate_planillometro_contabilidad(
                        ejercicio=params.ejercicio, ultimos_ejercicios=5, include_pa6=True
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

    # --------------------------------------------------
    async def generate_planillometro_contabilidad(
        self,
        ejercicio: int = None,
        es_desc_siif: bool = True,
        incluir_desc_subprog: bool = True,
        ultimos_ejercicios: str = "All",
        desagregar_partida: bool = True,
        agregar_acum_2008: bool = True,
        date_up_to: dt.date = None,
        include_pa6: bool = False,
    ):
        df = await generate_icaro_carga_desc(
            ejercicio=ejercicio, es_desc_siif=es_desc_siif
        )
        df.sort_values(["actividad", "partida", "fuente"], inplace=True)

        # Grupos de columnas
        group_cols = ["desc_programa"]
        if incluir_desc_subprog:
            group_cols = group_cols + ["desc_subprograma"]
        group_cols = group_cols + ["desc_proyecto", "desc_actividad", "actividad"]
        if desagregar_partida:
            group_cols = group_cols + ["partida"]

        # Eliminamos aquellos ejercicios anteriores a 2009
        df = df.loc[df.ejercicio.astype(int) >= 2009]

        # Incluimos PA6 (ultimo ejercicio)
        if include_pa6:
            df = df.loc[df.ejercicio.astype(int) < int(ejercicio)]
            df_last = await generate_icaro_carga_desc(
                ejercicio=ejercicio, es_desc_siif=es_desc_siif, 
                es_ejercicio_to=False,  es_neto_pa6=False
            )
            df = pd.concat([df, df_last], axis=0)

        # Filtramos hasta una fecha máxima
        if date_up_to:
            date_up_to = np.datetime64(date_up_to)
            df = df.loc[df["fecha"] <= date_up_to]

        # Agregamos ejecución acumulada de Patricia
        if agregar_acum_2008:
            df_acum_2008 = await get_planillometro_hist()
            df_acum_2008["ejercicio"] = 2008
            df_acum_2008["avance"] = 1
            df_acum_2008["desc_obra"] = df_acum_2008["desc_actividad"]
            df_acum_2008 = df_acum_2008.rename(columns={"acum_2008": "importe"})
            df["estructura"] = df["actividad"] + "-" + df["partida"]
            df_dif = df_acum_2008.loc[
                df_acum_2008["estructura"].isin(df["estructura"].unique().tolist())
            ]
            df_dif = df_dif.drop(
                columns=[
                    "desc_programa",
                    "desc_subprograma",
                    "desc_proyecto",
                    "desc_actividad",
                ]
            )
            if incluir_desc_subprog:
                columns_to_merge = [
                    "estructura",
                    "desc_programa",
                    "desc_subprograma",
                    "desc_proyecto",
                    "desc_actividad",
                ]
            else:
                columns_to_merge = [
                    "estructura",
                    "desc_programa",
                    "desc_proyecto",
                    "desc_actividad",
                ]
            df_dif = pd.merge(
                df_dif,
                df.loc[:, columns_to_merge].drop_duplicates(),
                on=["estructura"],
                how="left",
            )
            df = df.drop(columns=["estructura"])
            df_acum_2008 = df_acum_2008.loc[
                ~df_acum_2008["estructura"].isin(df_dif["estructura"].unique().tolist())
            ]
            df_acum_2008 = pd.concat([df_acum_2008, df_dif])
            df = pd.concat([df, df_acum_2008])

        # Ejercicio alta
        df_alta = df.groupby(group_cols).ejercicio.min().reset_index()
        df_alta.rename(columns={"ejercicio": "alta"}, inplace=True)

        df_ejercicios = df.copy()
        if ultimos_ejercicios != "All":
            ejercicios = int(ultimos_ejercicios)
            ejercicios = df_ejercicios.sort_values(
                "ejercicio", ascending=False
            ).ejercicio.unique()[0:ejercicios]
            # df_anos = df_anos.loc[df_anos.ejercicio.isin(ejercicios)]
        else:
            ejercicios = df_ejercicios.sort_values(
                "ejercicio", ascending=False
            ).ejercicio.unique()

        # Ejercicio actual
        df_ejec_actual = df.copy()
        df_ejec_actual = df_ejec_actual.loc[df_ejec_actual.ejercicio.isin(ejercicios)]
        df_ejec_actual = (
            df_ejec_actual.groupby(group_cols + ["ejercicio"])
            .importe.sum()
            .reset_index()
        )
        df_ejec_actual.rename(columns={"importe": "ejecucion"}, inplace=True)

        # Ejecucion Acumulada
        df_acum = pd.DataFrame()
        for ejercicio in ejercicios:
            df_ejercicio = df.copy()
            df_ejercicio = df_ejercicio.loc[
                df_ejercicio.ejercicio.astype(int) <= int(ejercicio)
            ]
            df_ejercicio["ejercicio"] = ejercicio
            df_ejercicio = (
                df_ejercicio.groupby(group_cols + ["ejercicio"])
                .importe.sum()
                .reset_index()
            )
            df_ejercicio.rename(columns={"importe": "acum"}, inplace=True)
            df_acum = pd.concat([df_acum, df_ejercicio])

        # Obras en curso
        df_curso = pd.DataFrame()
        for ejercicio in ejercicios:
            df_ejercicio = df.copy()
            df_ejercicio = df_ejercicio.loc[
                df_ejercicio.ejercicio.astype(int) <= int(ejercicio)
            ]
            df_ejercicio["ejercicio"] = ejercicio
            obras_curso = df_ejercicio.groupby(["desc_obra"]).avance.max().to_frame()
            obras_curso = (
                obras_curso.loc[obras_curso.avance < 1].reset_index().desc_obra
            )
            df_ejercicio = (
                df_ejercicio.loc[df_ejercicio.desc_obra.isin(obras_curso)]
                .groupby(group_cols + ["ejercicio"])
                .importe.sum()
                .reset_index()
            )
            df_ejercicio.rename(columns={"importe": "en_curso"}, inplace=True)
            df_curso = pd.concat([df_curso, df_ejercicio])

        # Obras terminadas anterior
        df_term_ant = pd.DataFrame()
        for ejercicio in ejercicios:
            df_ejercicio = df.copy()
            df_ejercicio = df_ejercicio.loc[
                df_ejercicio.ejercicio.astype(int) < int(ejercicio)
            ]
            df_ejercicio["ejercicio"] = ejercicio
            obras_term_ant = df_ejercicio.groupby(["desc_obra"]).avance.max().to_frame()
            obras_term_ant = (
                obras_term_ant.loc[obras_term_ant.avance == 1].reset_index().desc_obra
            )
            df_ejercicio = (
                df_ejercicio.loc[df_ejercicio.desc_obra.isin(obras_term_ant)]
                .groupby(group_cols + ["ejercicio"])
                .importe.sum()
                .reset_index()
            )
            df_ejercicio.rename(columns={"importe": "terminadas_ant"}, inplace=True)
            df_term_ant = pd.concat([df_term_ant, df_ejercicio])

        df = pd.merge(df_alta, df_acum, on=group_cols, how="left")
        df = pd.merge(df, df_ejec_actual, on=group_cols + ["ejercicio"], how="left")
        cols = df.columns.tolist()
        penultima_col = cols.pop(-2)  # Elimina la penúltima columna y la guarda
        cols.append(penultima_col)  # Agrega la penúltima columna al final
        df = df[cols]  # Reordena las columnas
        df = pd.merge(df, df_curso, on=group_cols + ["ejercicio"], how="left")
        df = pd.merge(df, df_term_ant, on=group_cols + ["ejercicio"], how="left")
        df = df.fillna(0)
        df["terminadas_actual"] = df.acum - df.en_curso - df.terminadas_ant
        df["actividad"] = df["actividad"] + "-" + df["partida"]
        df = df.rename(columns={"actividad": "estructura"})
        df = df.drop(columns=["partida"])

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
                    await self.generate_planillometro_contabilidad(
                        ejercicio=params.ejercicio, ultimos_ejercicios=5, include_pa6=True
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
