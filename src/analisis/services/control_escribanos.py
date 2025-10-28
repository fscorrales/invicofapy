#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: SIIF Haberes vs SSCC
Data required:
    - SIIF rcocc31 (2113-2-9 Escribanos)
    - SGF Resumen de Rendiciones
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1Tz3uvUGBL8ZDSFsYRBP8hgIis-hlhs_sQ6V5bI4LaTg
"""

__all__ = ["ControlEscribanosService", "ControlEscribanosServiceDependency"]

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
from ...sgf.services import ResumenRendProvServiceDependency
from ...siif.handlers import (
    Rcocc31,
    login,
    logout,
)
from ...siif.repositories import (
    Rcocc31RepositoryDependency,
)
from ...sscc.services import BancoINVICOServiceDependency, CtasCtesServiceDependency
from ...utils import (
    RouteReturnSchema,
    export_multiple_dataframes_to_excel,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..handlers import (
    get_banco_invico_unified_cta_cte,
    get_resumen_rend_prov_with_desc,
    get_siif_rcocc31,
)
from ..repositories.control_escribanos import (
    ControlEscribanosSGFvsSSCCRepositoryDependency,
    ControlEscribanosSIIFvsSGFRepositoryDependency,
)
from ..schemas.control_escribanos import (
    ControlEscribanosParams,
    ControlEscribanosSGFvsSSCCReport,
    ControlEscribanosSIIFvsSGFReport,
    ControlEscribanosSyncParams,
)


# --------------------------------------------------
@dataclass
class ControlEscribanosService:
    control_sgf_vs_sscc_repo: ControlEscribanosSGFvsSSCCRepositoryDependency
    control_siif_vs_sgf_repo: ControlEscribanosSIIFvsSGFRepositoryDependency
    sscc_banco_invico_service: BancoINVICOServiceDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency
    siif_rcocc31_repo: Rcocc31RepositoryDependency
    siif_rcocc31_handler: Rcocc31 = field(init=False)  # No se pasa como argumento
    sgf_resumend_rend_prov_service: ResumenRendProvServiceDependency

    # -------------------------------------------------
    async def sync_control_escribanos_from_source(
        self,
        params: ControlEscribanosSyncParams = None,
    ) -> List[RouteReturnSchema]:
        """Downloads a report from all sources, processes it, validates the data,
        and stores it in MongoDB if valid.

        Args:
            ejercicio (int, optional): The fiscal year for the report. Defaults to the current year.

        Returns:
            RouteReturnSchema
        """
        if (
            params.siif_username is None
            or params.siif_password is None
            or params.sscc_username is None
            or params.sscc_password is None
            or params.sgf_username is None
            or params.sgf_password is None
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
                # 🔹 Rcocc31
                self.siif_rcocc31_handler = Rcocc31(siif=connect_siif)
                await self.siif_rcocc31_handler.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rcocc31_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio), cta_contable="2113-2-9"
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

                # 🔹Resumen Rendicion Proveedores
                partial_schema = await self.sgf_resumend_rend_prov_service.sync_resumen_rend_prov_from_sgf(
                    username=params.sgf_username,
                    password=params.sgf_password,
                    params=params,
                )
                return_schema.extend(partial_schema)

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
    async def compute_all(
        self, params: ControlEscribanosParams
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control Escribanos
            partial_schema = await self.compute_control_sgf_vs_sscc(
                params=params, only_diff=True
            )
            return_schema.extend(partial_schema)
            partial_schema = await self.compute_control_siif_vs_sgf(
                params=params, only_importe_bruto=False, only_diff=True
            )
            return_schema.extend(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control de Escribanos",
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
        ejercicio_actual = datetime.now().year
        ultimos_ejercicios = list(range(ejercicio_actual - 2, ejercicio_actual + 1))
        control_siif_vs_sgf_docs = await self.control_siif_vs_sgf_repo.find_by_filter(
            {"ejercicio": {"$in": ultimos_ejercicios}}
        )
        control_sgf_vs_sscc_docs = await self.control_sgf_vs_sscc_repo.find_by_filter(
            {"ejercicio": {"$in": ultimos_ejercicios}}
        )

        if not control_siif_vs_sgf_docs and not control_sgf_vs_sscc_docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        siif = pd.DataFrame()
        sscc = pd.DataFrame()
        sgf = pd.DataFrame()
        for ejercicio in ultimos_ejercicios:
            df = await self.generate_siif_escribanos(ejercicio=ejercicio)
            siif = pd.concat([siif, df], ignore_index=True)
            df = await self.generate_banco_escribanos(ejercicio=ejercicio)
            sscc = pd.concat([sscc, df], ignore_index=True)
            df = await self.generate_sgf_escribanos(ejercicio=ejercicio)
            sgf = pd.concat([sgf, df], ignore_index=True)

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (pd.DataFrame(control_siif_vs_sgf_docs), "new_siif_vs_sgf_db"),
                (pd.DataFrame(control_sgf_vs_sscc_docs), "new_sgf_vs_sscc_db"),
                (siif, "new_siif_db"),
                (sscc, "new_sscc_db"),
                (sgf, "new_sgf_db"),
            ],
            filename="control_escribanos.xlsx",
            spreadsheet_key="1Tz3uvUGBL8ZDSFsYRBP8hgIis-hlhs_sQ6V5bI4LaTg",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # --------------------------------------------------
    async def generate_siif_escribanos(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        filters = {
            "auxiliar_1__in": ["245", "310"],
            "tipo_comprobante": {"$ne": "APE"},
            "cta_contable": "2113-2-9",
        }
        df = await get_siif_rcocc31(ejercicio=ejercicio, filters=filters)
        df = df.rename(
            columns={
                "auxiliar_1": "cuit",
                "creditos": "carga_fei",
                "debitos": "pagos_fei",
            }
        )
        df["fei_impagos"] = df["carga_fei"] - df["pagos_fei"]
        return df

    # --------------------------------------------------
    async def siif_summarize(
        self,
        ejercicio: int = None,
        groupby_cols: List[str] = ["ejercicio", "mes", "cuit"],
    ) -> pd.DataFrame:
        """
        Summarize SIIF data for the specified groupby columns.

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by.
                Defaults to ['ejercicio', 'mes', 'cuit'].

        Returns:
            pd.DataFrame: A DataFrame summarizing the SIIF data based on the specified groupby columns.

        This method imports and processes SIIF data related to the 'Escribanos' category. It then groups
        the data by the specified columns, calculates the sum of numeric values, and returns the resulting
        DataFrame. Any missing values are filled with zeros.
        """
        df = await self.generate_siif_escribanos(ejercicio=ejercicio)
        df = df.drop(
            [
                "tipo_comprobante",
                "fecha",
                "fecha_aprobado",
                "cta_contable",
                "auxiliar_2",
                "saldo",
                "nro_entrada",
            ],
            axis=1,
        )
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        df = df.fillna(0)
        return df

    # --------------------------------------------------
    async def generate_sgf_escribanos(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        filters = {"cta_cte": "130832-08"}
        df = await get_resumen_rend_prov_with_desc(ejercicio=ejercicio, filters=filters)
        return df

    # --------------------------------------------------
    async def sgf_summarize(
        self,
        ejercicio: int = None,
        groupby_cols: List[str] = ["ejercicio", "mes", "cuit", "beneficiario"],
    ) -> pd.DataFrame:
        """
        Summarize renditions data related to the SGF

        Args:
            groupby_cols (list, optional): A list of columns to group by in the summary. Defaults to
            ['ejercicio', 'mes', 'cuit', 'beneficiario'].

        Returns:
            pd.DataFrame: Pandas DataFrame containing the cross-controlled data.

        This method imports renditions data, removes irrelevant columns, and then summarizes the
        data based on the specified grouping columns. The resulting DataFrame contains the summary
        of renditions data related to the SGF for further analysis.
        """
        filters = {}
        df = await self.generate_sgf_escribanos(ejercicio=ejercicio, filters=filters)
        df = df.drop(
            [
                "origen",
                "fecha",
                "destino",
                "libramiento_sgf",
                "seguro",
                "salud",
                "mutual",
                "otras",
                "importe_bruto",
                "gcias",
                "iibb",
                "sellos",
                "suss",
                "invico",
                "retenciones",
            ],
            axis=1,
        )
        # Rellena los valores nulos solo en la columna específica
        df["cuit"] = df["cuit"].fillna(0)
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        return df

    # --------------------------------------------------
    async def generate_banco_escribanos(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        codigos_imputacion = ["004", "034", "213", "102"]
        filters = {
            "cta_cte": "130832-08",
            "cod_imputacion": {"$nin": codigos_imputacion},
        }
        df = await get_banco_invico_unified_cta_cte(
            ejercicio=ejercicio, filters=filters
        )
        return df

    # --------------------------------------------------
    async def sscc_summarize(
        self,
        ejercicio: int = None,
        groupby_cols: List[str] = ["ejercicio", "mes", "cod_imputacion", "imputacion"],
    ) -> pd.DataFrame:
        """
        Summarize and filter bank data related to INVICO (Instituto de Vivienda de Corrientes).

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by for
                summarization. Defaults to ['ejercicio', 'mes', 'cod_imputacion', 'imputacion'].

        Returns:
            pd.DataFrame: A DataFrame containing summarized and filtered bank data related to INVICO.

        This method imports bank data related to INVICO for the specified exercise (year),
        filters out specific 'cod_imputacion' values, and summarizes the data based on the
        provided groupby columns. The resulting DataFrame contains the summarized and
        filtered bank data for further analysis.
        """
        df = self.generate_banco_escribanos(ejercicio=ejercicio)
        df = df.drop(["es_cheque"], axis=1)
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        df["importe"] = df["importe"] * -1
        df = df.rename(columns={"importe": "importe_neto"})
        return df

    # --------------------------------------------------
    async def compute_control_sgf_vs_sscc(
        self, params: ControlEscribanosParams, only_diff=False
    ) -> List[RouteReturnSchema]:
        return_schema = []
        groupby_cols = ["ejercicio", "mes"]
        try:
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                sgf = await self.sgf_summarize(groupby_cols=groupby_cols)
                sgf = sgf.set_index(groupby_cols)
                sscc = await self.sscc_summarize(groupby_cols=groupby_cols)
                sscc = sscc.set_index(groupby_cols)
                # Obtener los índices faltantes en sgf
                missing_indices = sscc.index.difference(sgf.index)
                # Reindexar el DataFrame sgf con los índices faltantes
                sgf = sgf.reindex(sgf.index.union(missing_indices))
                sscc = sscc.reindex(sgf.index)
                sgf = sgf.fillna(0)
                sscc = sscc.fillna(0)
                df = sgf.subtract(sscc)
                df = df.reset_index()
                df = df.fillna(0)
                # Reindexamos el DataFrame
                sgf = sgf.reset_index()
                df = df.reindex(columns=sgf.columns)
                if only_diff:
                    # Seleccionar solo las columnas numéricas
                    numeric_cols = df.select_dtypes(include=np.number).columns.drop(
                        "ejercicio"
                    )
                    # Filtrar el DataFrame utilizando las columnas numéricas válidas
                    # df = df[df[numeric_cols].sum(axis=1) != 0]
                    df = df[np.abs(df[numeric_cols].sum(axis=1)) > 0.01]
                    df = df.reset_index(drop=True)

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df,
                    model=ControlEscribanosSGFvsSSCCReport,
                    field_id="mes",
                )

                partial_schema = await sync_validated_to_repository(
                    repository=self.control_sgf_vs_sscc_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title=f"Control Escribanos SGF vs SSCC del ejercicio {ejercicio}",
                    logger=logger,
                    label=f"Control Escribanos SGF vs SSCC del ejercicio {ejercicio}",
                )
                return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from SGF vs SSCC Control Escribanos",
            )
        except Exception as e:
            logger.error(f"Error in SGF vs SSCC Control Escribanos: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in SGF vs SSCC Control Escribanos",
            )
        finally:
            return return_schema

    # --------------------------------------------------
    async def compute_control_siif_vs_sgf(
        self, params: ControlEscribanosParams, only_diff=False
    ) -> List[RouteReturnSchema]:
        return_schema = []
        groupby_cols = ["ejercicio", "mes", "cuit"]
        try:
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                siif = await self.siif_summarize(groupby_cols=groupby_cols).copy()
                siif = siif.set_index(groupby_cols)
                sgf = await self.sgf_summarize(groupby_cols=groupby_cols).copy()
                sgf = sgf.rename(columns={"importe_neto": "pagos_sgf"})
                sgf = sgf.set_index(groupby_cols)
                # Obtener los índices faltantes en siif
                missing_indices = sgf.index.difference(siif.index)
                # Reindexar el DataFrame siif con los índices faltantes
                siif = siif.reindex(siif.index.union(missing_indices))
                sgf = sgf.reindex(siif.index)
                siif = siif.fillna(0)
                sgf = sgf.fillna(0)
                df = siif.merge(sgf, how="outer", on=groupby_cols)
                df = df.reset_index()
                df = df.fillna(0)
                df["dif_pagos"] = df["pagos_fei"] - df["pagos_sgf"]
                if only_diff:
                    # Seleccionar solo las columnas numéricas
                    numeric_cols = df.select_dtypes(include=np.number).columns.drop(
                        "ejercicio"
                    )
                    # Filtrar el DataFrame utilizando las columnas numéricas válidas
                    # df = df[df[numeric_cols].sum(axis=1) != 0]
                    df = df[np.abs(df[numeric_cols].sum(axis=1)) > 0.01]
                    df = df.reset_index(drop=True)

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df,
                    model=ControlEscribanosSIIFvsSGFReport,
                    field_id="cuit",
                )

                partial_schema = await sync_validated_to_repository(
                    repository=self.control_siif_vs_sgf_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title=f"Control Escribanos SIIF vs SGF del ejercicio {ejercicio}",
                    logger=logger,
                    label=f"Control Escribanos SIIF vs SGF del ejercicio {ejercicio}",
                )
                return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from SIIF vs SGF Control Escribanos",
            )
        except Exception as e:
            logger.error(f"Error in SIIF vs SGF Control Escribanos: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in SIIF vs SGF Control Escribanos",
            )
        finally:
            return return_schema


ControlEscribanosServiceDependency = Annotated[ControlEscribanosService, Depends()]
