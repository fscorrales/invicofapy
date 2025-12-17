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

__all__ = ["ControlIcaroVsSIIFService", "ControlIcaroVsSIIFServiceDependency"]

import datetime as dt
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

# from ...icaro.repositories import CargaRepositoryDependency
from ...siif.handlers import (
    Rcg01Uejp,
    Rf602,
    Rf610,
    Rfondo07tp,
    Rpa03g,
    login,
    logout,
)

# from ...siif.repositories import (
#     Rf602RepositoryDependency,
#     Rf610RepositoryDependency,
#     Rfondo07tpRepositoryDependency,
# )
from ...siif.schemas import GrupoPartidaSIIF, TipoComprobanteSIIF
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
    get_siif_comprobantes_gtos_joined,
    get_siif_desc_pres,
    get_siif_rf602,
    get_siif_rfondo07tp,
)
from ..repositories.control_icaro_vs_siif import (
    ControlAnualRepositoryDependency,
    ControlComprobantesRepositoryDependency,
    ControlPa6RepositoryDependency,
)
from ..schemas.control_icaro_vs_siif import (
    ControlAnualDocument,
    ControlAnualReport,
    ControlCompletoParams,
    ControlCompletoSyncParams,
    ControlComprobantesDocument,
    ControlComprobantesReport,
    ControlPa6Document,
    ControlPa6Report,
)


# --------------------------------------------------
@dataclass
class ControlIcaroVsSIIFService:
    control_anual_repo: ControlAnualRepositoryDependency
    control_comprobantes_repo: ControlComprobantesRepositoryDependency
    control_pa6_repo: ControlPa6RepositoryDependency
    siif_rf602_handler: Rf602 = field(init=False)  # No se pasa como argumento
    siif_rf610_handler: Rf610 = field(init=False)  # No se pasa como argumento
    siif_rcg01_uejp_handler: Rcg01Uejp = field(init=False)  # No se pasa como argumento
    siif_rpa03g_handler: Rpa03g = field(init=False)  # No se pasa como argumento
    siif_rfondo07tp_handler: Rfondo07tp = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_icaro_vs_siif_from_source(
        self,
        params: ControlCompletoSyncParams = None,
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
                # 🔹 RF602
                self.siif_rf602_handler = Rf602(siif=connect_siif)
                await self.siif_rf602_handler.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rf602_handler.download_and_sync_validated_to_repository(
                        ejercicio=ejercicio
                    )
                    return_schema.append(partial_schema)

                # 🔹 RF610
                self.siif_rf610_handler = Rf610(siif=connect_siif)
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rf610_handler.download_and_sync_validated_to_repository(
                        ejercicio=ejercicio
                    )
                    return_schema.append(partial_schema)

                # 🔹 Rcg01Uejp
                self.siif_rcg01_uejp_handler = Rcg01Uejp(siif=connect_siif)
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rcg01_uejp_handler.download_and_sync_validated_to_repository(
                        ejercicio=ejercicio
                    )
                    return_schema.append(partial_schema)

                # 🔹 Rpa03g
                self.siif_rpa03g_handler = Rpa03g(siif=connect_siif)
                for ejercicio in ejercicios:
                    for grupo in [g.value for g in GrupoPartidaSIIF]:
                        partial_schema = await self.siif_rpa03g_handler.download_and_sync_validated_to_repository(
                            ejercicio=ejercicio, grupo_partida=grupo
                        )
                        return_schema.append(partial_schema)

                # 🔹 Rfondo07tp
                self.siif_rfondo07tp_handler = Rfondo07tp(siif=connect_siif)
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rfondo07tp_handler.download_and_sync_validated_to_repository(
                        ejercicio=ejercicio
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

            # 🔹 Control PA6
            partial_schema = await self.compute_control_pa6(params=params)
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
            ejercicio_actual = dt.datetime.now().year
            ultimos_ejercicios = list(range(ejercicio_actual - 2, ejercicio_actual + 1))
            # 1️⃣ Obtenemos los documentos
            control_anual_docs = await self.control_anual_repo.find_by_filter(
                {"ejercicio": {"$in": ultimos_ejercicios}}
            )
            control_comprobantes_docs = (
                await self.control_comprobantes_repo.find_by_filter(
                    {"ejercicio": {"$in": ultimos_ejercicios}}
                )
            )
            control_pa6_repo = await self.control_pa6_repo.find_by_filter(
                {"ejercicio": {"$in": ultimos_ejercicios}}
            )

            if (
                not control_anual_docs
                and not control_comprobantes_docs
                and not control_pa6_repo
            ):
                raise HTTPException(
                    status_code=404, detail="No se encontraron registros"
                )

            # 2️⃣ Convertimos a DataFrame
            control_anual_df = (
                sanitize_dataframe_for_json(pd.DataFrame(control_anual_docs))
                if control_anual_docs
                else pd.DataFrame()
            )
            control_comprobantes_df = (
                sanitize_dataframe_for_json(pd.DataFrame(control_comprobantes_docs))
                if control_comprobantes_docs
                else pd.DataFrame()
            )
            control_pa6_df = (
                sanitize_dataframe_for_json(pd.DataFrame(control_pa6_repo))
                if control_pa6_repo
                else pd.DataFrame()
            )

            # 3️⃣ Subimos a Google Sheets si se solicita
            if upload_to_google_sheets:
                gs_service = GoogleSheets()
                if not control_anual_df.empty:
                    control_anual_df.drop(columns=["_id"], inplace=True)
                gs_service.to_google_sheets(
                    df=control_anual_df,
                    spreadsheet_key="1KKeeoop_v_Nf21s7eFp4sS6SmpxRZQ9DPa1A5wVqnZ0",
                    wks_name="control_ejecucion_anual_db",
                )
                if not control_comprobantes_df.empty:
                    control_comprobantes_df.drop(columns=["_id"], inplace=True)
                gs_service.to_google_sheets(
                    df=control_comprobantes_df,
                    spreadsheet_key="1KKeeoop_v_Nf21s7eFp4sS6SmpxRZQ9DPa1A5wVqnZ0",
                    wks_name="control_comprobantes_db",
                )
                if not control_pa6_df.empty:
                    control_pa6_df.drop(columns=["_id"], inplace=True)
                gs_service.to_google_sheets(
                    df=control_pa6_df,
                    spreadsheet_key="1KKeeoop_v_Nf21s7eFp4sS6SmpxRZQ9DPa1A5wVqnZ0",
                    wks_name="control_pa6_db",
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
                if not control_pa6_df.empty:
                    control_pa6_df.to_excel(
                        writer, index=False, sheet_name="control_pa6"
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
    async def get_siif_comprobantes(self, ejercicio: int = None) -> pd.DataFrame:
        df = await get_siif_comprobantes_gtos_joined(ejercicio=ejercicio)
        df = df.loc[
            (df["partida"].isin(["421", "422"]))
            | (
                (df["partida"] == "354")
                & (~df["cuit"].isin(["30500049460", "30632351514", "20231243527"]))
            )
        ]
        return df

    # --------------------------------------------------
    async def compute_control_anual(
        self, params: ControlCompletoParams
    ) -> RouteReturnSchema:
        return_schema = RouteReturnSchema()
        try:
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                group_by = ["ejercicio", "estructura", "fuente"]
                icaro = await get_icaro_carga(
                    ejercicio=ejercicio, filters={"tipo__ne": "PA6"}
                )
                icaro["estructura"] = icaro.actividad + "-" + icaro.partida
                icaro = icaro.groupby(group_by)["importe"].sum()
                icaro = icaro.reset_index(drop=False)
                icaro = icaro.rename(columns={"importe": "ejecucion_icaro"})
                siif = await get_siif_rf602(
                    ejercicio=ejercicio,
                    filters={
                        "$or": [
                            {"partida": {"$in": ["421", "422"]}},
                            {"estructura": "01-00-00-03-354"},
                        ]
                    },
                )
                siif = siif.loc[:, group_by + ["ordenado"]]
                siif = siif.rename(columns={"ordenado": "ejecucion_siif"})
                df = pd.merge(siif, icaro, how="outer", on=group_by, copy=False)
                df = df.fillna(0)
                df["diferencia"] = df["ejecucion_siif"] - df["ejecucion_icaro"]
                # logger.info(df.head())
                df = df.merge(
                    await get_siif_desc_pres(ejercicio_to=ejercicio),
                    how="left",
                    on="estructura",
                    copy=False,
                )
                df = df.loc[(df["diferencia"] < -0.1) | (df["diferencia"] > 0.1)]
                df = df.reset_index(drop=True)
                df["fuente"] = pd.to_numeric(df["fuente"], errors="coerce")
                df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
                # logger.info(df.head())
                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df, model=ControlAnualReport, field_id="estructura"
                )

                partial_schema = await sync_validated_to_repository(
                    repository=self.control_anual_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title="Control de Ejecución Anual ICARO vs SIIF",
                    logger=logger,
                    label=f"Control de Ejecución Anual ICARO vs SIIF ejercicio {ejercicio}",
                )
                return_schema.append(partial_schema)

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
            df = sanitize_dataframe_for_json(pd.DataFrame(docs))
            df = df.drop(columns=["_id"])

            # 3️⃣ Subimos a Google Sheets si se solicita
            if upload_to_google_sheets:
                gs_service = GoogleSheets()
                gs_service.to_google_sheets(
                    df=df,
                    spreadsheet_key="1KKeeoop_v_Nf21s7eFp4sS6SmpxRZQ9DPa1A5wVqnZ0",
                    wks_name="control_ejecucion_anual_db",
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
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
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
                siif = await self.get_siif_comprobantes(ejercicio=ejercicio)
                # logger.info(f"siif_gtos.shape: {siif.shape} - siif_gtos.head: {siif.head()}")
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
                icaro = await get_icaro_carga(
                    ejercicio=ejercicio, filters={"tipo__ne": "PA6"}
                )
                icaro = icaro.loc[:, select + ["tipo"]]
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
                cols = list(ControlComprobantesReport.model_fields.keys())
                df = df[cols]

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df, model=ControlComprobantesReport, field_id="siif_nro"
                )
                partial_schema = await sync_validated_to_repository(
                    repository=self.control_comprobantes_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title="Control de Comprobantes ICARO vs SIIF",
                    logger=logger,
                    label=f"Control de Comprobantes ICARO vs SIIF ejercicio {ejercicio}",
                )
                return_schema.append(partial_schema)

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

    # --------------------------------------------------
    async def compute_control_pa6(
        self, params: ControlCompletoParams
    ) -> RouteReturnSchema:
        return_schema = []
        try:
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                siif_fdos = await get_siif_rfondo07tp(
                    ejercicio=ejercicio,
                    filters={
                        "tipo_comprobante": TipoComprobanteSIIF.adelanto_contratista.value
                    },
                )
                siif_fdos = siif_fdos.loc[
                    :, ["ejercicio", "nro_fondo", "mes", "ingresos", "saldo"]
                ]
                siif_fdos["nro_fondo"] = (
                    siif_fdos["nro_fondo"].str.zfill(5) + "/" + siif_fdos.mes.str[-2:]
                )
                siif_fdos = siif_fdos.rename(
                    columns={
                        "nro_fondo": "siif_nro_fondo",
                        "mes": "siif_mes_pa6",
                        "ingresos": "siif_importe_pa6",
                        "saldo": "siif_saldo_pa6",
                    }
                )
                siif_fdos.dropna(subset=["siif_nro_fondo"], inplace=True)

                select = [
                    "ejercicio",
                    "nro_comprobante",
                    "fuente",
                    "importe",
                    "mes",
                    "cta_cte",
                    "cuit",
                ]

                siif_gtos = await self.get_siif_comprobantes(ejercicio=ejercicio)
                siif_gtos = siif_gtos.loc[siif_gtos["clase_reg"] == "REG"]
                siif_gtos = siif_gtos.loc[:, select + ["nro_fondo", "clase_reg"]]
                siif_gtos["nro_fondo"] = (
                    siif_gtos["nro_fondo"].str.zfill(5) + "/" + siif_gtos.mes.str[-2:]
                )
                siif_gtos = siif_gtos.rename(
                    columns={
                        "nro_fondo": "siif_nro_fondo",
                        "cta_cte": "siif_cta_cte",
                        "cuit": "siif_cuit",
                        "clase_reg": "siif_tipo",
                        "fuente": "siif_fuente",
                        "nro_comprobante": "siif_nro_reg",
                        "importe": "siif_importe_reg",
                        "mes": "siif_mes_reg",
                    }
                )
                siif_gtos.dropna(subset=["siif_nro_fondo"], inplace=True)

                icaro = await get_icaro_carga(ejercicio=ejercicio)
                icaro = icaro.loc[:, select + ["tipo"]]
                icaro = icaro.rename(
                    columns={
                        "mes": "icaro_mes",
                        "nro_comprobante": "icaro_nro",
                        "tipo": "icaro_tipo",
                        "importe": "icaro_importe",
                        "cuit": "icaro_cuit",
                        "cta_cte": "icaro_cta_cte",
                        "fuente": "icaro_fuente",
                    }
                )

                icaro_pa6 = icaro.loc[icaro["icaro_tipo"] == "PA6"]
                icaro_pa6 = icaro_pa6.loc[
                    :, ["ejercicio", "icaro_mes", "icaro_nro", "icaro_importe"]
                ]
                icaro_pa6 = icaro_pa6.rename(
                    columns={
                        "icaro_mes": "icaro_mes_pa6",
                        "icaro_nro": "icaro_nro_fondo",
                        "icaro_importe": "icaro_importe_pa6",
                    }
                )

                icaro_reg = icaro.loc[icaro["icaro_tipo"] != "PA6"]
                icaro_reg = icaro_reg.rename(
                    columns={
                        "icaro_mes": "icaro_mes_reg",
                        "icaro_nro": "icaro_nro_reg",
                        "icaro_importe": "icaro_importe_reg",
                    }
                )

                df = pd.merge(
                    siif_fdos,
                    siif_gtos,
                    how="left",
                    on=["ejercicio", "siif_nro_fondo"],
                    copy=False,
                )

                df = pd.merge(
                    df,
                    icaro_pa6,
                    how="outer",
                    left_on=["ejercicio", "siif_nro_fondo"],
                    right_on=["ejercicio", "icaro_nro_fondo"],
                )

                df = pd.merge(
                    df,
                    icaro_reg,
                    how="left",
                    left_on=["ejercicio", "siif_nro_reg"],
                    right_on=["ejercicio", "icaro_nro_reg"],
                )

                # df = df.fillna(0)
                df["err_nro_fondo"] = (df.siif_nro_fondo != df.icaro_nro_fondo) & ~(
                    df.siif_nro_fondo.isna() & df.icaro_nro_fondo.isna()
                )
                df["err_mes_pa6"] = (df.siif_mes_pa6 != df.icaro_mes_pa6) & ~(
                    df.siif_mes_pa6.isna() & df.icaro_mes_pa6.isna()
                )
                df["siif_importe_pa6"] = df["siif_importe_pa6"].fillna(0)
                df["icaro_importe_pa6"] = df["icaro_importe_pa6"].fillna(0)
                df["err_importe_pa6"] = (df.siif_importe_pa6 - df.icaro_importe_pa6).abs()
                df["err_importe_pa6"] = df["err_importe_pa6"] > 0.1
                # df['err_importe_pa6'] = ~np.isclose((df.siif_importe_pa6 - df.icaro_importe_pa6), 0)
                df["err_nro_reg"] = (df.siif_nro_reg != df.icaro_nro_reg) & ~(
                    df.siif_nro_reg.isna() & df.icaro_nro_reg.isna()
                )
                df["err_mes_reg"] = (df.siif_mes_reg != df.icaro_mes_reg) & ~(
                    df.siif_mes_reg.isna() & df.icaro_mes_reg.isna()
                )
                df["siif_importe_reg"] = df["siif_importe_reg"].fillna(0)
                df["icaro_importe_reg"] = df["icaro_importe_reg"].fillna(0)
                df["err_importe_reg"] = (df.siif_importe_reg - df.icaro_importe_reg).abs()
                df["err_importe_reg"] = df["err_importe_reg"] > 0.1
                # df['err_importe_reg'] = ~np.isclose((df.siif_importe_reg - df.icaro_importe_reg), 0)
                df["err_tipo"] = (df.siif_tipo != df.icaro_tipo) & ~(
                    df.siif_tipo.isna() & df.icaro_tipo.isna()
                )
                df["err_fuente"] = (df.siif_fuente != df.icaro_fuente) & ~(
                    df.siif_fuente.isna() & df.icaro_fuente.isna()
                )
                df["err_cta_cte"] = (df.siif_cta_cte != df.icaro_cta_cte) & ~(
                    df.siif_cta_cte.isna() & df.icaro_cta_cte.isna()
                )
                df["err_cuit"] = (df.siif_cuit != df.icaro_cuit) & ~(
                    df.siif_cuit.isna() & df.icaro_cuit.isna()
                )
                cols = list(ControlPa6Report.model_fields.keys())
                df = df[cols]
                df = df.loc[
                    (
                        df.err_nro_fondo
                        + df.err_mes_pa6
                        + df.err_importe_pa6
                        + df.err_nro_reg
                        + df.err_mes_reg
                        + df.err_importe_reg
                        + df.err_fuente
                        + df.err_tipo
                        + df.err_cta_cte
                        + df.err_cuit
                    )
                    > 0
                ]

                df = df.sort_values(
                    by=[
                        "err_nro_fondo",
                        "err_importe_pa6",
                        "err_nro_reg",
                        "err_importe_reg",
                        "err_fuente",
                        "err_cta_cte",
                        "err_cuit",
                        "err_tipo",
                        "err_mes_pa6",
                        "err_mes_reg",
                    ],
                    ascending=False,
                )

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df, model=ControlPa6Report
                )
                return_schema = await sync_validated_to_repository(
                    repository=self.control_pa6_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title="Control de PA6 ICARO vs SIIF",
                    logger=logger,
                    label=f"Control de PA6 ICARO vs SIIF ejercicio {ejercicio}",
                )
        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control PA6 ICARO vs SIIF",
            )
        except Exception as e:
            logger.error(f"Error in compute_control_pa6: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in compute_control_pa6",
            )
        finally:
            return return_schema

    # -------------------------------------------------
    async def get_control_pa6_from_db(
        self, params: BaseFilterParams
    ) -> List[ControlPa6Document]:
        try:
            return await self.control_pa6_repo.find_with_filter_params(params=params)
        except Exception as e:
            logger.error(f"Error retrieving Icaro's Control de PA6 from database: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving Icaro's Control de PA6 from the database",
            )


ControlIcaroVsSIIFServiceDependency = Annotated[ControlIcaroVsSIIFService, Depends()]
