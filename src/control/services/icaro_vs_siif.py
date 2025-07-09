#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Icaro vs SIIF budget execution
Data required:
    - Icaro
    - SIIF rf602
    - SIIF rf610
    - SIIF gto_rpa03g
    - SIIF rcg01_uejp
    - SIIF rfondo07tp
    - SSCC ctas_ctes (manual data)
"""

__all__ = ["IcaroVsSIIFService", "IcaroVsSIIFServiceDependency"]

import datetime as dt
import os
from dataclasses import dataclass, field
from io import BytesIO
from typing import Annotated, List, Union

import pandas as pd
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import logger
from ...icaro.handlers import IcaroMongoMigrator
from ...icaro.repositories import CargaRepositoryDependency
from ...siif.handlers import (
    JoinComprobantesGtosGpoPart,
    Rcg01Uejp,
    Rf602,
    Rf610,
    Rfondo07tp,
    Rpa03g,
    login,
    logout,
)
from ...siif.repositories import (
    Rf602RepositoryDependency,
    Rf610RepositoryDependency,
)
from ...siif.schemas import GrupoPartidaSIIF
from ...utils import (
    BaseFilterParams,
    GoogleSheets,
    RouteReturnSchema,
    get_r_icaro_path,
    sanitize_dataframe_for_json,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..repositories.icaro_vs_siif import (
    ControlAnualRepositoryDependency,
    ControlComprobantesRepositoryDependency,
)
from ..schemas.icaro_vs_siif import (
    ControlAnualDocument,
    ControlAnualReport,
    ControlCompletoParams,
    ControlComprobantesDocument,
    ControlComprobantesReport,
)


# --------------------------------------------------
@dataclass
class IcaroVsSIIFService:
    control_anual_repo: ControlAnualRepositoryDependency
    control_comprobantes_repo: ControlComprobantesRepositoryDependency
    siif_rf602_repo: Rf602RepositoryDependency
    siif_rf602_handler: Rf602 = field(init=False)  # No se pasa como argumento
    siif_rf610_repo: Rf610RepositoryDependency
    siif_rf610_handler: Rf610 = field(init=False)  # No se pasa como argumento
    siif_rcg01_uejp_handler: Rcg01Uejp = field(init=False)  # No se pasa como argumento
    siif_rpa03g_handler: Rpa03g = field(init=False)  # No se pasa como argumento
    siif_rfondo07tp_handler: Rfondo07tp = field(init=False)  # No se pasa como argumento
    icaro_carga_repo: CargaRepositoryDependency

    # -------------------------------------------------
    async def sync_icaro_vs_siif_from_source(
        self,
        username: str,
        password: str,
        params: ControlCompletoParams = None,
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
                # 🔹 RF602
                self.siif_rf602_handler = Rf602(siif=connect_siif)
                await self.siif_rf602_handler.go_to_reports()
                partial_schema = await self.siif_rf602_handler.download_and_sync_validated_to_repository(
                    ejercicio=int(params.ejercicio)
                )
                return_schema.append(partial_schema)

                # 🔹 RF610
                self.siif_rf610_handler = Rf610(siif=connect_siif)
                partial_schema = await self.siif_rf610_handler.download_and_sync_validated_to_repository(
                    ejercicio=int(params.ejercicio)
                )
                return_schema.append(partial_schema)

                # 🔹 Rcg01Uejp
                self.siif_rcg01_uejp_handler = Rcg01Uejp(siif=connect_siif)
                partial_schema = await self.siif_rcg01_uejp_handler.download_and_sync_validated_to_repository(
                    ejercicio=int(params.ejercicio)
                )
                return_schema.append(partial_schema)

                # 🔹 Rpa03g
                self.siif_rpa03g_handler = Rpa03g(siif=connect_siif)
                for grupo in [g.value for g in GrupoPartidaSIIF]:
                    partial_schema = await self.siif_rpa03g_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(params.ejercicio), grupo_partida=grupo
                    )
                    return_schema.append(partial_schema)

                # 🔹 Rfondo07tp
                self.siif_rfondo07tp_handler = Rfondo07tp(siif=connect_siif)
                partial_schema = await self.siif_rfondo07tp_handler.download_and_sync_validated_to_repository(
                    ejercicio=int(params.ejercicio)
                )
                return_schema.append(partial_schema)

                # 🔹 Icaro Carga
                path = os.path.join(get_r_icaro_path(), "ICARO.sqlite")
                migrator = IcaroMongoMigrator(sqlite_path=path)
                return_schema.append(await migrator.migrate_carga())

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

    async def compute_all(
        self, params: ControlCompletoParams
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control Anual
            partial_schema = await self.compute_control_anual(params=params)
            return_schema.append(partial_schema)

            # 🔹 Control Comprobantes
            partial_schema = await self.compute_control_comprobantes(params=params)
            return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Icaro vs SIIF",
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
            control_anual_docs = await self.control_anual_repo.get_all()
            control_comprobantes_docs = await self.control_comprobantes_repo.get_all()

            if not control_anual_docs and not control_comprobantes_docs:
                raise HTTPException(
                    status_code=404, detail="No se encontraron registros"
                )

            # 2️⃣ Convertimos a DataFrame
            control_anual_df = (
                sanitize_dataframe_for_json(
                    pd.DataFrame([doc.model_dump() for doc in control_anual_docs])
                )
                if control_anual_docs
                else pd.DataFrame()
            )
            control_comprobantes_df = (
                sanitize_dataframe_for_json(
                    pd.DataFrame(
                        [doc.model_dump() for doc in control_comprobantes_docs]
                    )
                )
                if control_comprobantes_docs
                else pd.DataFrame()
            )

            # 3️⃣ Subimos a Google Sheets si se solicita
            if upload_to_google_sheets:
                gs_service = GoogleSheets()
                if not control_anual_df.empty:
                    await gs_service.to_google_sheets(
                        df=control_anual_df,
                        spreadsheet_key="1KKeeoop_v_Nf21s7eFp4sS6SmpxRZQ9DPa1A5wVqnZ0",
                        worksheet_name="control_ejecucion_anual_db",
                    )
                if not control_comprobantes_df.empty:
                    await gs_service.to_google_sheets(
                        df=control_comprobantes_df,
                        spreadsheet_key="1KKeeoop_v_Nf21s7eFp4sS6SmpxRZQ9DPa1A5wVqnZ0",
                        worksheet_name="control_comprobantes_db",
                    )

            # 4️⃣ Escribimos a un buffer Excel en memoria
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                if not control_anual_df.empty:
                    control_anual_df.to_excel(
                        writer, index=False, sheet_name="control_ejecucion_anual"
                    )
                if not control_comprobantes_df.empty:
                    control_comprobantes_df.to_excel(
                        writer, index=False, sheet_name="control_comprobantes"
                    )

            buffer.seek(0)

            # 5️⃣ Devolvemos StreamingResponse
            file_name = "icaro_vs_siif.xlsx"
            headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
            return StreamingResponse(
                buffer,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers=headers,
            )
        except Exception as e:
            logger.error(
                f"Error retrieving Icaro's Control de Ejecución Anual from database: {e}"
            )
            raise HTTPException(
                status_code=500,
                detail="Error retrieving Icaro's Control de Ejecución Anual from the database",
            )

    # --------------------------------------------------
    async def get_icaro_carga(self, ejercicio: int = None) -> pd.DataFrame:
        """
        Get the icaro_carga data from the repository.
        """
        try:
            filters = {
                "tipo__ne": "PA6",
            }
            if ejercicio is not None:
                filters["ejercicio"] = ejercicio
            docs = await self.icaro_carga_repo.find_by_filter(filters=filters)
            df = pd.DataFrame(docs)
            df["estructura"] = df.actividad + "-" + df.partida
            return df
        except Exception as e:
            logger.error(f"Error retrieving Icaro's Carga Data from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving Icaro's Carga Data from the database",
            )

    # --------------------------------------------------
    async def get_siif_comprobantes(self):
        df = await JoinComprobantesGtosGpoPart().from_mongo()
        df = df.loc[
            (df["partida"].isin(["421", "422"]))
            | (
                (df["partida"] == "354")
                & (~df["cuit"].isin(["30500049460", "30632351514", "20231243527"]))
            )
        ]
        return df

    # --------------------------------------------------
    async def get_siif_rf602(self, ejercicio: int = None) -> pd.DataFrame:
        """
        Get the rf602 data from the repository.
        """
        try:
            filters = {
                "$or": [
                    {"partida": {"$in": ["421", "422"]}},
                    {"estructura": "01-00-00-03-354"},
                ]
            }
            if ejercicio is not None:
                filters["ejercicio"] = ejercicio
            docs = await self.siif_rf602_repo.find_by_filter(filters=filters)
            df = pd.DataFrame(docs)
            return df
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rf602 from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's rf602 from the database",
            )

        # df = pd.DataFrame(docs)
        # return df

    async def get_siif_desc_pres(
        self, ejercicio_to: Union[int, List] = int(dt.datetime.now().year)
    ) -> pd.DataFrame:
        """
        Get the rf610 data from the repository.
        """

        if ejercicio_to is None:
            docs = await self.siif_rf610_repo.get_all()
        elif isinstance(ejercicio_to, list):
            docs = await self.siif_rf610_repo.find_by_filter(
                filters={
                    "ejercicio__in": ejercicio_to,
                }
            )
        else:
            docs = await self.siif_rf610_repo.find_by_filter(
                filters={
                    "ejercicio__lte": int(ejercicio_to),
                }
            )

        df = pd.DataFrame(docs)
        df.sort_values(
            by=["ejercicio", "estructura"], inplace=True, ascending=[False, True]
        )
        # Programas únicos
        df_prog = df.loc[:, ["programa", "desc_programa"]]
        df_prog.drop_duplicates(subset=["programa"], inplace=True, keep="first")
        # Subprogramas únicos
        df_subprog = df.loc[:, ["programa", "subprograma", "desc_subprograma"]]
        df_subprog.drop_duplicates(
            subset=["programa", "subprograma"], inplace=True, keep="first"
        )
        # Proyectos únicos
        df_proy = df.loc[:, ["programa", "subprograma", "proyecto", "desc_proyecto"]]
        df_proy.drop_duplicates(
            subset=["programa", "subprograma", "proyecto"], inplace=True, keep="first"
        )
        # Actividades únicos
        df_act = df.loc[
            :,
            [
                "estructura",
                "programa",
                "subprograma",
                "proyecto",
                "actividad",
                "desc_actividad",
            ],
        ]
        df_act.drop_duplicates(subset=["estructura"], inplace=True, keep="first")
        # Merge all
        df = df_act.merge(df_prog, how="left", on="programa", copy=False)
        df = df.merge(
            df_subprog, how="left", on=["programa", "subprograma"], copy=False
        )
        df = df.merge(
            df_proy, how="left", on=["programa", "subprograma", "proyecto"], copy=False
        )
        df["desc_programa"] = df.programa + " - " + df.desc_programa
        df["desc_subprograma"] = df.subprograma + " - " + df.desc_subprograma
        df["desc_proyecto"] = df.proyecto + " - " + df.desc_proyecto
        df["desc_actividad"] = df.actividad + " - " + df.desc_actividad
        df.drop(
            labels=["programa", "subprograma", "proyecto", "actividad"],
            axis=1,
            inplace=True,
        )
        return df

    # --------------------------------------------------
    async def compute_control_anual(
        self, params: ControlCompletoParams
    ) -> RouteReturnSchema:
        # return_schema = RouteReturnSchema()
        try:
            group_by = ["ejercicio", "estructura", "fuente"]
            icaro = await self.get_icaro_carga(ejercicio=params.ejercicio)
            icaro = icaro.groupby(group_by)["importe"].sum()
            icaro = icaro.reset_index(drop=False)
            icaro = icaro.rename(columns={"importe": "ejecucion_icaro"})
            siif = await self.get_siif_rf602(ejercicio=params.ejercicio)
            siif = siif.loc[:, group_by + ["ordenado"]]
            siif = siif.rename(columns={"ordenado": "ejecucion_siif"})
            df = pd.merge(siif, icaro, how="outer", on=group_by, copy=False)
            df = df.fillna(0)
            df["diferencia"] = df["ejecucion_siif"] - df["ejecucion_icaro"]
            logger.info(df.head())
            df = df.merge(
                await self.get_siif_desc_pres(ejercicio_to=params.ejercicio),
                how="left",
                on="estructura",
                copy=False,
            )
            df = df.loc[(df["diferencia"] < -0.1) | (df["diferencia"] > 0.1)]
            df = df.reset_index(drop=True)
            df["fuente"] = pd.to_numeric(df["fuente"], errors="coerce")
            df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
            logger.info(df.head())
            # 🔹 Validar datos usando Pydantic
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df, model=ControlAnualReport, field_id="estructura"
            )

            partial_schema = await sync_validated_to_repository(
                repository=self.control_anual_repo,
                validation=validate_and_errors,
                delete_filter={"ejercicio": params.ejercicio},
                title="Control de Ejecución Anual ICARO vs SIIF",
                logger=logger,
                label=f"Control de Ejecución Anual ICARO vs SIIF ejercicio {params.ejercicio}",
            )
            # return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control Anual ICARO vs SIIF",
            )
        except Exception as e:
            logger.error(f"Error in compute_control_anual: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in compute_control_anual",
            )
        finally:
            return partial_schema

    # -------------------------------------------------
    async def get_control_anual_from_db(
        self, params: BaseFilterParams
    ) -> List[ControlAnualDocument]:
        try:
            return await self.control_anual_repo.find_with_filter_params(params=params)
        except Exception as e:
            logger.error(
                f"Error retrieving Icaro's Control de Ejecución Anual from database: {e}"
            )
            raise HTTPException(
                status_code=500,
                detail="Error retrieving Icaro's Control de Ejecución Anual from the database",
            )

    # -------------------------------------------------
    async def export_control_anual_from_db(
        self, upload_to_google_sheets: bool = True
    ) -> StreamingResponse:
        try:
            # 1️⃣ Obtenemos los documentos
            docs = await self.control_anual_repo.get_all()

            if not docs:
                raise HTTPException(
                    status_code=404, detail="No se encontraron registros"
                )

            # 2️⃣ Convertimos a DataFrame
            df = pd.DataFrame([doc.model_dump() for doc in docs])
            df = sanitize_dataframe_for_json(df)

            # 3️⃣ Subimos a Google Sheets si se solicita
            if upload_to_google_sheets:
                gs_service = GoogleSheets()
                await gs_service.to_google_sheets(
                    df=df,
                    spreadsheet_key="1KKeeoop_v_Nf21s7eFp4sS6SmpxRZQ9DPa1A5wVqnZ0",
                    worksheet_name="control_ejecucion_anual_db",
                )

            # 4️⃣ Escribimos a un buffer Excel en memoria
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="control_ejecucion_anual")

            buffer.seek(0)

            # 5️⃣ Devolvemos StreamingResponse
            file_name = "icaro_vs_siif_control_anual.xlsx"
            headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
            return StreamingResponse(
                buffer,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers=headers,
            )
        except Exception as e:
            logger.error(
                f"Error retrieving Icaro's Control de Ejecución Anual from database: {e}"
            )
            raise HTTPException(
                status_code=500,
                detail="Error retrieving Icaro's Control de Ejecución Anual from the database",
            )

    # --------------------------------------------------
    async def compute_control_comprobantes(
        self, params: ControlCompletoParams
    ) -> RouteReturnSchema:
        return_schema = []
        try:
            select = [
                "ejercicio",
                "nro_comprobante",
                "fuente",
                "importe",
                "mes",
                "cta_cte",
                "cuit",
                "partida",
            ]
            siif = await self.get_siif_comprobantes()
            siif.loc[
                (siif.clase_reg == "REG") & (siif.nro_fondo.isnull()), "clase_reg"
            ] = "CYO"
            siif = siif.loc[:, select + ["clase_reg"]]
            siif = siif.rename(
                columns={
                    "nro_comprobante": "siif_nro",
                    "clase_reg": "siif_tipo",
                    "fuente": "siif_fuente",
                    "importe": "siif_importe",
                    "mes": "siif_mes",
                    "cta_cte": "siif_cta_cte",
                    "cuit": "siif_cuit",
                    "partida": "siif_partida",
                }
            )
            icaro = await self.get_icaro_carga(ejercicio=params.ejercicio)
            icaro = icaro.loc[:, select + ["tipo"]]
            icaro = icaro.loc[icaro["tipo"] != "PA6"]
            icaro = icaro.rename(
                columns={
                    "nro_comprobante": "icaro_nro",
                    "tipo": "icaro_tipo",
                    "fuente": "icaro_fuente",
                    "importe": "icaro_importe",
                    "mes": "icaro_mes",
                    "cta_cte": "icaro_cta_cte",
                    "cuit": "icaro_cuit",
                    "partida": "icaro_partida",
                }
            )
            df = pd.merge(
                siif,
                icaro,
                how="outer",
                left_on=["ejercicio", "siif_nro"],
                right_on=["ejercicio", "icaro_nro"],
            )
            df["err_nro"] = df.siif_nro != df.icaro_nro
            df["err_tipo"] = df.siif_tipo != df.icaro_tipo
            df["err_mes"] = df.siif_mes != df.icaro_mes
            df["err_partida"] = df.siif_partida != df.icaro_partida
            df["err_fuente"] = df.siif_fuente != df.icaro_fuente
            df["siif_importe"] = df["siif_importe"].fillna(0)
            df["icaro_importe"] = df["icaro_importe"].fillna(0)
            df["err_importe"] = (df.siif_importe - df.icaro_importe).abs()
            df["err_importe"] = df["err_importe"] > 0.1
            df["err_cta_cte"] = df.siif_cta_cte != df.icaro_cta_cte
            df["err_cuit"] = df.siif_cuit != df.icaro_cuit
            df = df.loc[
                (
                    df.err_nro
                    + df.err_tipo
                    + df.err_mes
                    + df.err_partida
                    + df.err_fuente
                    + df.err_importe
                    + df.err_cta_cte
                    + df.err_cuit
                )
                > 0
            ]
            df = df.loc[
                :,
                [
                    "ejercicio",
                    "siif_nro",
                    "icaro_nro",
                    "err_nro",
                    "siif_tipo",
                    "icaro_tipo",
                    "err_tipo",
                    "siif_fuente",
                    "icaro_fuente",
                    "err_fuente",
                    "siif_importe",
                    "icaro_importe",
                    "err_importe",
                    "siif_mes",
                    "icaro_mes",
                    "err_mes",
                    "siif_cta_cte",
                    "icaro_cta_cte",
                    "err_cta_cte",
                    "siif_cuit",
                    "icaro_cuit",
                    "err_cuit",
                    "siif_partida",
                    "icaro_partida",
                    "err_partida",
                ],
            ]

            # 🔹 Validar datos usando Pydantic
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df, model=ControlComprobantesReport, field_id="siif_nro"
            )
            return_schema = await sync_validated_to_repository(
                repository=self.control_comprobantes_repo,
                validation=validate_and_errors,
                delete_filter={"ejercicio": params.ejercicio},
                title="Control de Comprobantes ICARO vs SIIF",
                logger=logger,
                label=f"Control de Comprobantes ICARO vs SIIF ejercicio {params.ejercicio}",
            )
        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control Comprobantes ICARO vs SIIF",
            )
        except Exception as e:
            logger.error(f"Error in compute_control_comprobantes: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in compute_control_comprobantes",
            )
        finally:
            return return_schema

    # -------------------------------------------------
    async def get_control_comprobantes_from_db(
        self, params: BaseFilterParams
    ) -> List[ControlComprobantesDocument]:
        try:
            return await self.control_comprobantes_repo.find_with_filter_params(
                params=params
            )
        except Exception as e:
            logger.error(
                f"Error retrieving Icaro's Control de Comprobantes from database: {e}"
            )
            raise HTTPException(
                status_code=500,
                detail="Error retrieving Icaro's Control de Comprobantes from the database",
            )

    # # --------------------------------------------------
    # def control_pa6(self):
    #     siif_fdos = self.siif_rfondo07tp.copy()
    #     siif_fdos = siif_fdos.loc[
    #         :, ['ejercicio', 'nro_fondo', 'mes', 'ingresos', 'saldo']
    #     ]
    #     siif_fdos['nro_fondo'] = siif_fdos['nro_fondo'].str.zfill(5) + '/' + siif_fdos.ejercicio.str[-2:]
    #     siif_fdos = siif_fdos.rename(columns={
    #         'nro_fondo':'siif_nro_fondo',
    #         'mes':'siif_mes_pa6',
    #         'ingresos':'siif_importe_pa6',
    #         'saldo':'siif_saldo_pa6'
    #     })

    #     select = [
    #         'ejercicio','nro_comprobante', 'fuente', 'importe',
    #         'mes', 'cta_cte', 'cuit'
    #     ]

    #     siif_gtos = self.import_siif_comprobantes().copy()
    #     siif_gtos = siif_gtos.loc[siif_gtos['clase_reg'] == 'REG']
    #     siif_gtos = siif_gtos.loc[:, select + ['nro_fondo', 'clase_reg']]
    #     siif_gtos['nro_fondo'] = siif_gtos['nro_fondo'].str.zfill(5) + '/' + siif_gtos.ejercicio.str[-2:]
    #     siif_gtos = siif_gtos.rename(columns={
    #             'nro_fondo':'siif_nro_fondo',
    #             'cta_cte':'siif_cta_cte',
    #             'cuit':'siif_cuit',
    #             'clase_reg':'siif_tipo',
    #             'fuente':'siif_fuente',
    #             'nro_comprobante':'siif_nro_reg',
    #             'importe':'siif_importe_reg',
    #             'mes':'siif_mes_reg',
    #     })

    #     icaro = self.icaro_carga.copy()
    #     icaro = icaro.loc[:, select + ['tipo']]
    #     icaro = icaro.rename(columns={
    #             'mes':'icaro_mes',
    #             'nro_comprobante':'icaro_nro',
    #             'tipo':'icaro_tipo',
    #             'importe':'icaro_importe',
    #             'cuit':'icaro_cuit',
    #             'cta_cte':'icaro_cta_cte',
    #             'fuente':'icaro_fuente'
    #     })

    #     icaro_pa6 = icaro.loc[icaro['icaro_tipo'] == 'PA6']
    #     icaro_pa6 = icaro_pa6.loc[:, ['icaro_mes', 'icaro_nro', 'icaro_importe']]
    #     icaro_pa6 = icaro_pa6.rename(columns={
    #             'icaro_mes':'icaro_mes_pa6',
    #             'icaro_nro':'icaro_nro_fondo',
    #             'icaro_importe':'icaro_importe_pa6',
    #     })

    #     icaro_reg = icaro.loc[icaro['icaro_tipo'] != 'PA6']
    #     icaro_reg = icaro_reg.rename(columns={
    #             'icaro_mes':'icaro_mes_reg',
    #             'icaro_nro':'icaro_nro_reg',
    #             'icaro_importe':'icaro_importe_reg',
    #     })

    #     df = pd.merge(
    #         siif_fdos, siif_gtos, how='left',
    #         on=['ejercicio','siif_nro_fondo'], copy=False
    #     )
    #     df = pd.merge(
    #         df, icaro_pa6, how='outer',
    #         left_on = 'siif_nro_fondo',
    #         right_on = 'icaro_nro_fondo'
    #     )
    #     df = pd.merge(
    #         df, icaro_reg, how='left',
    #         left_on = ['ejercicio', 'siif_nro_reg'],
    #         right_on = ['ejercicio', 'icaro_nro_reg']
    #     )
    #     df = df.fillna(0)
    #     df['err_nro_fondo'] = df.siif_nro_fondo != df.icaro_nro_fondo
    #     df['err_mes_pa6'] = df.siif_mes_pa6 != df.icaro_mes_pa6
    #     df['siif_importe_pa6'] = df['siif_importe_pa6'].fillna(0)
    #     df['icaro_importe_pa6'] = df['icaro_importe_pa6'].fillna(0)
    #     df['err_importe_pa6'] = (df.siif_importe_pa6 - df.icaro_importe_pa6).abs()
    #     df['err_importe_pa6'] = (df['err_importe_pa6'] > 0.1)
    #     # df['err_importe_pa6'] = ~np.isclose((df.siif_importe_pa6 - df.icaro_importe_pa6), 0)
    #     df['err_nro_reg'] = df.siif_nro_reg != df.icaro_nro_reg
    #     df['err_mes_reg'] = df.siif_mes_reg != df.icaro_mes_reg
    #     df['siif_importe_reg'] = df['siif_importe_reg'].fillna(0)
    #     df['icaro_importe_reg'] = df['icaro_importe_reg'].fillna(0)
    #     df['err_importe_reg'] = (df.siif_importe_reg - df.icaro_importe_reg).abs()
    #     df['err_importe_reg'] = (df['err_importe_reg'] > 0.1)
    #     #df['err_importe_reg'] = ~np.isclose((df.siif_importe_reg - df.icaro_importe_reg), 0)
    #     df['err_tipo'] = df.siif_tipo != df.icaro_tipo
    #     df['err_fuente'] = df.siif_fuente != df.icaro_fuente
    #     df['err_cta_cte'] = df.siif_cta_cte != df.icaro_cta_cte
    #     df['err_cuit'] = df.siif_cuit != df.icaro_cuit
    #     df = df.loc[:, ['ejercicio',
    #         'siif_nro_fondo', 'icaro_nro_fondo', 'err_nro_fondo',
    #         'siif_mes_pa6', 'icaro_mes_pa6', 'err_mes_pa6',
    #         'siif_importe_pa6', 'icaro_importe_pa6', 'err_importe_pa6',
    #         'siif_nro_reg', 'icaro_nro_reg', 'err_nro_reg',
    #         'siif_mes_reg', 'icaro_mes_reg', 'err_mes_reg',
    #         'siif_importe_reg', 'icaro_importe_reg', 'err_importe_reg',
    #         'siif_tipo', 'icaro_tipo', 'err_tipo',
    #         'siif_fuente', 'icaro_fuente', 'err_fuente',
    #         'siif_cta_cte', 'icaro_cta_cte', 'err_cta_cte',
    #         'siif_cuit', 'icaro_cuit', 'err_cuit'
    #     ]]
    #     df = df.loc[(
    #         df.err_nro_fondo + df.err_mes_pa6 + df.err_importe_pa6 +
    #         df.err_nro_reg + df.err_mes_reg + df.err_importe_reg +
    #         df.err_fuente + df.err_tipo + df.err_cta_cte +
    #         df.err_cuit
    #     ) > 0]
    #     df = df.sort_values(
    #         by=['err_nro_fondo','err_importe_pa6',
    #         'err_nro_reg', 'err_importe_reg',
    #         'err_fuente', 'err_cta_cte', 'err_cuit',
    #         'err_tipo', 'err_mes_pa6', 'err_mes_reg'],
    #         ascending=False
    #     )
    #     return df


IcaroVsSIIFServiceDependency = Annotated[IcaroVsSIIFService, Depends()]
