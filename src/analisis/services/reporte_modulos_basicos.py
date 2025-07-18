#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Reporte Ejecución Obras
Data required:
    - Icaro (CARGA, ESTRUCTURAS, OBRAS, PROVEEDORES)
    - SIIF rf610
Google Sheet:
    - https://docs.google.com/spreadsheets/d/195qPSga7cU1kx3z2-gadEWNC2eupdkbR-rb-O8SPWuA

"""

__all__ = ["ReporteModulosBasicosService", "ReporteModulosBasicosServiceDependency"]

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
from ...icaro.handlers import IcaroMongoMigrator
from ...siif.handlers import (
    Rf610,
    login,
    logout,
)
from ...utils import (
    BaseFilterParams,
    GoogleSheets,
    RouteReturnSchema,
    get_r_icaro_path,
    sanitize_dataframe_for_json,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..handlers import (
    get_icaro_carga,
    get_icaro_estructuras_desc,
    get_icaro_obras,
    get_icaro_proveedores,
    get_siif_desc_pres,
)
from ..repositories.reporte_modulos_basicos import (
    ReporteModulosBasicosIcaroRepositoryDependency,
)
from ..schemas.reporte_modulos_basicos import (
    ReporteModulosBasicosIcaroDocument,
    ReporteModulosBasicosIcaroParams,
    ReporteModulosBasicosIcaroReport,
)


# --------------------------------------------------
@dataclass
class ReporteModulosBasicosService:
    reporte_mod_bas_icaro_repo: ReporteModulosBasicosIcaroRepositoryDependency
    siif_rf610_handler: Rf610 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_modulos_basicos_from_source(
        self,
        username: str,
        password: str,
        params: ReporteModulosBasicosIcaroParams = None,
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
                partial_schema = await self.siif_rf610_handler.download_and_sync_validated_to_repository(
                    ejercicio=int(params.ejercicio)
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
        self, params: ReporteModulosBasicosIcaroParams
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control Anual
            partial_schema = await self.generate_reporte_modulos_basicos_icaro(
                params=params
            )
            return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Reporte Módulos Básicos",
            )
        except Exception as e:
            logger.error(f"Error in compute_all: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in compute_all",
            )
        finally:
            return return_schema

    # -------------------------------------------------
    async def export_all_from_db(
        self, upload_to_google_sheets: bool = True
    ) -> StreamingResponse:
        try:
            # 1️⃣ Obtenemos los documentos
            reporte_mod_bas_icaro_docs = await self.reporte_mod_bas_icaro_repo.get_all()

            if not reporte_mod_bas_icaro_docs:
                raise HTTPException(
                    status_code=404, detail="No se encontraron registros"
                )

            # 2️⃣ Convertimos a DataFrame
            reporte_mod_bas_icaro_df = (
                sanitize_dataframe_for_json(pd.DataFrame(reporte_mod_bas_icaro_docs))
                if reporte_mod_bas_icaro_docs
                else pd.DataFrame()
            )

            # 3️⃣ Subimos a Google Sheets si se solicita
            if upload_to_google_sheets:
                gs_service = GoogleSheets()
                if not reporte_mod_bas_icaro_df.empty:
                    reporte_mod_bas_icaro_df.drop(columns=["_id"], inplace=True)
                    gs_service.to_google_sheets(
                        df=reporte_mod_bas_icaro_df,
                        spreadsheet_key="195qPSga7cU1kx3z2-gadEWNC2eupdkbR-rb-O8SPWuA",
                        wks_name="mod_basicos",
                    )

            # 4️⃣ Escribimos a un buffer Excel en memoria
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                if not reporte_mod_bas_icaro_df.empty:
                    reporte_mod_bas_icaro_df.to_excel(
                        writer, index=False, sheet_name="modulos_basicos_icaro"
                    )

            buffer.seek(0)

            # 5️⃣ Devolvemos StreamingResponse
            file_name = "modulos_basicos.xlsx"
            headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
            return StreamingResponse(
                buffer,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers=headers,
            )
        except Exception as e:
            logger.error(f"Error retrieving Modulos Básicos from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving Modulos Básicos from the database",
            )

    # --------------------------------------------------
    async def generate_reporte_modulos_basicos_icaro(
        self, params: ReporteModulosBasicosIcaroParams
    ) -> RouteReturnSchema:
        return_schema = RouteReturnSchema()
        try:
            filters = {}
            filters["partida"] = {"$in": ["421", "422"]}

            if params.es_ejercicio_to:
                filters["ejercicio"] = {"$lt": params.ejercicio}
            else:
                filters["ejercicio"] = params.ejercicio

            if params.es_neto_pa6:
                filters["tipo"] = {"$ne": "PA6"}
            else:
                filters["tipo"] = {"$ne": "REG"}

            df = await get_icaro_carga(filters=filters)
            df = df.loc[df["actividad"].str.startswith("29")]
            df_obras = await get_icaro_obras()
            df_obras = df_obras.loc[
                :, ["desc_obra", "localidad", "norma_legal", "info_adicional"]
            ]
            df = df.merge(df_obras, how="left", on="desc_obra", copy=False)
            if params.es_desc_siif:
                df["estructura"] = df["actividad"] + "-" + df["partida"]
                df = df.merge(
                    await get_siif_desc_pres(ejercicio_to=params.ejercicio),
                    how="left",
                    on="estructura",
                    copy=False,
                )
                df.drop(labels=["estructura"], axis="columns", inplace=True)
            else:
                df = df.merge(
                    await get_icaro_estructuras_desc(),
                    how="left",
                    on="actividad",
                    copy=False,
                )
            prov = await get_icaro_proveedores()
            prov = prov.loc[:, ["cuit", "desc_proveedor"]]
            prov.drop_duplicates(subset=["cuit"], inplace=True)
            prov.rename(columns={"desc_proveedor": "proveedor"}, inplace=True)
            df = df.merge(prov, how="left", on="cuit", copy=False)
            df.reset_index(drop=True, inplace=True)
            # 🔹 Validar datos usando Pydantic
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=ReporteModulosBasicosIcaroReport,
                field_id="id_carga",
            )

            return_schema = await sync_validated_to_repository(
                repository=self.reporte_mod_bas_icaro_repo,
                validation=validate_and_errors,
                delete_filter={"ejercicio": params.ejercicio}
                if not params.es_ejercicio_to
                else None,
                title="Control de Ejecución Icaro Módulos Básicos",
                logger=logger,
                label=f"Control de Ejecución Icaro Módulos Básicos ejercicio {'hasta' if params.es_ejercicio_to else ''}: {params.ejercicio}",
            )

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Reporte Ejecución Icaro Módulos Básicos",
            )
        except Exception as e:
            logger.error(f"Error in generate_reporte_icaro_modulos_basicos: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in generate_reporte_icaro_modulos_basicos",
            )
        finally:
            return return_schema

    # # --------------------------------------------------
    # async def compute_reporte_avance_icaro_mod_basicos(
    #     self,
    #     params: ControlCompletoParams,
    # ) -> RouteReturnSchema:
    #     # return_schema = RouteReturnSchema()
    #     try:
    #         df = await self.compute_icaro_modulos_basicos(
    #             es_desc_siif=params.es_desc_siif
    #         )
    #         if params.por_convenio:
    #             df = df.loc[df.fuente == "11"]
    #         df.sort_values(
    #             ["actividad", "partida", "desc_obra", "fuente"], inplace=True
    #         )
    #         group_cols = [
    #             "desc_programa",
    #             "desc_proyecto",
    #             "desc_actividad",
    #             "actividad",
    #             "partida",
    #             "fuente",
    #             "localidad",
    #             "norma_legal",
    #             "desc_obra",
    #         ]
    #         # Ejercicio alta
    #         df_alta = df.groupby(group_cols).ejercicio.min().reset_index()
    #         df_alta.rename(columns={"ejercicio": "alta"}, inplace=True)
    #         # Ejecucion Total
    #         df_total = df.groupby(group_cols).importe.sum().reset_index()
    #         df_total.rename(columns={"importe": "ejecucion_total"}, inplace=True)
    #         # Obras en curso
    #         obras_curso = df.groupby(["desc_obra"]).avance.max().to_frame()
    #         obras_curso = obras_curso.loc[obras_curso.avance < 1].reset_index().obra
    #         df_curso = (
    #             df.loc[df.desc_obra.isin(obras_curso)]
    #             .groupby(group_cols)
    #             .importe.sum()
    #             .reset_index()
    #         )
    #         df_curso.rename(columns={"importe": "en_curso"}, inplace=True)
    #         # Obras terminadas anterior
    #         df_prev = df.loc[df.ejercicio.astype(int) < int(params.ejercicio)]
    #         obras_term_ant = df_prev.loc[df_prev.avance == 1].obra
    #         df_term_ant = (
    #             df_prev.loc[df_prev.desc_obra.isin(obras_term_ant)]
    #             .groupby(group_cols)
    #             .importe.sum()
    #             .reset_index()
    #         )
    #         df_term_ant.rename(columns={"importe": "terminadas_ant"}, inplace=True)
    #         # Pivoteamos en funcion de...
    #         if params.por_convenio:
    #             df_pivot = df.loc[:, group_cols + ["info_adicional", "importe"]]
    #             df_pivot = df_pivot.pivot_table(
    #                 index=group_cols,
    #                 columns="info_adicional",
    #                 values="importe",
    #                 aggfunc="sum",
    #                 fill_value=0,
    #             )
    #             df_pivot.reset_index(inplace=True)
    #         else:
    #             df_pivot = df.loc[:, group_cols + ["ejercicio", "importe"]]
    #             df_pivot = df_pivot.pivot_table(
    #                 index=group_cols,
    #                 columns="ejercicio",
    #                 values="importe",
    #                 aggfunc="sum",
    #                 fill_value=0,
    #             )
    #             df_pivot = df_pivot.reset_index()

    #         # Agrupamos todo
    #         df = pd.merge(df_alta, df_pivot, how="left", on=group_cols)
    #         df = pd.merge(df, df_total, how="left", on=group_cols)
    #         df = pd.merge(df, df_curso, how="left", on=group_cols)
    #         df = pd.merge(df, df_term_ant, how="left", on=group_cols)
    #         df.fillna(0, inplace=True)
    #         df["terminadas_actual"] = (
    #             df.ejecucion_total - df.en_curso - df.terminadas_ant
    #         )
    #         df = pd.DataFrame(df)
    #         df.reset_index(drop=True, inplace=True)
    #         df.rename(columns={"": "Sin Convenio"}, inplace=True)
    #         # 🔹 Validar datos usando Pydantic
    #         validate_and_errors = validate_and_extract_data_from_df(
    #             dataframe=df, model=ControlAnualReport, field_id="estructura"
    #         )

    #         return_schema = await sync_validated_to_repository(
    #             repository=self.control_anual_repo,
    #             validation=validate_and_errors,
    #             delete_filter={"ejercicio": params.ejercicio},
    #             title="Control de Ejecución Icaro Módulos Básicos",
    #             logger=logger,
    #             label=f"Control de Ejecución Icaro Módulos Básicos ejercicio {params.ejercicio}",
    #         )
    #         # return_schema.append(partial_schema)

    #     except ValidationError as e:
    #         logger.error(f"Validation Error: {e}")
    #         raise HTTPException(
    #             status_code=400,
    #             detail="Invalid response format from Reporte Ejecución Icaro Módulos Básicos",
    #         )
    #     except Exception as e:
    #         logger.error(f"Error in compute_reporte_icaro_mod_basicos: {e}")
    #         raise HTTPException(
    #             status_code=500,
    #             detail="Error in compute_reporte_icaro_mod_basicos",
    #         )
    #     finally:
    #         return return_schema

    # -------------------------------------------------
    async def get_reporte_modulos_basicos_icaro_db(
        self, params: BaseFilterParams
    ) -> List[ReporteModulosBasicosIcaroDocument]:
        try:
            return await self.reporte_mod_bas_icaro_repo.find_with_filter_params(
                params=params
            )
        except Exception as e:
            logger.error(
                f"Error retrieving Reporte Ejecución Icaro Módulos Básicos from database: {e}"
            )
            raise HTTPException(
                status_code=500,
                detail="Error retrieving Reporte Ejecución Icaro Módulos Básicos from the database",
            )

    # -------------------------------------------------
    async def export_reporte_modulos_basicos_icaro_from_db(
        self, upload_to_google_sheets: bool = True
    ) -> StreamingResponse:
        try:
            # 1️⃣ Obtenemos los documentos
            docs = await self.reporte_mod_bas_icaro_repo.get_all()

            if not docs:
                raise HTTPException(
                    status_code=404, detail="No se encontraron registros"
                )

            # 2️⃣ Convertimos a DataFrame
            df = sanitize_dataframe_for_json(pd.DataFrame(docs))
            df = df.drop(columns=["_id"])

            # 3️⃣ Subimos a Google Sheets si se solicita
            if upload_to_google_sheets:
                gs_service = GoogleSheets()
                gs_service.to_google_sheets(
                    df=df,
                    spreadsheet_key="195qPSga7cU1kx3z2-gadEWNC2eupdkbR-rb-O8SPWuA",
                    wks_name="mod_basicos",
                )

            # 4️⃣ Escribimos a un buffer Excel en memoria
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="control_ejecucion_anual")

            buffer.seek(0)

            # 5️⃣ Devolvemos StreamingResponse
            file_name = "reporte_ejecucion_icaro_modulos_basicos.xlsx"
            headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
            return StreamingResponse(
                buffer,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers=headers,
            )
        except Exception as e:
            logger.error(
                f"Error retrieving Reporte Ejecución Icaro Módulos Básicos from database: {e}"
            )
            raise HTTPException(
                status_code=500,
                detail="Error retrieving Reporte Ejecución Icaro Módulos Básicos from the database",
            )


ReporteModulosBasicosServiceDependency = Annotated[
    ReporteModulosBasicosService, Depends()
]
