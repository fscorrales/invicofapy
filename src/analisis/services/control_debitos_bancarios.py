#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: SIIF Debitos Bancarios vs SSCC
Data required:
    - SIIF rcg01_uejp
    - SIIF rpa03g
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1i9vQ-fw_MkuHRE_YKa_diaVDu5RsiBE1UPTNAsmxLS4
"""

__all__ = ["ControlDebitosBancariosService", "ControlDebitosBancariosServiceDependency"]

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
from ...siif.handlers import (
    Rcg01Uejp,
    Rpa03g,
    login,
    logout,
)
from ...siif.repositories import (
    Rcg01UejpRepositoryDependency,
    Rpa03gRepositoryDependency,
)
from ...siif.schemas import GrupoPartidaSIIF
from ...sscc.services import BancoINVICOServiceDependency, CtasCtesServiceDependency
from ...utils import (
    RouteReturnSchema,
    export_multiple_dataframes_to_excel,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..handlers import (
    get_banco_invico_unified_cta_cte,
    get_resumen_rend_honorarios,
    get_siif_comprobantes_gtos_unified_cta_cte,
)
from ..repositories.control_debitos_bancarios import (
    ControlDebitosBancariosRepositoryDependency,
)
from ..schemas.control_debitos_bancarios import (
    ControlDebitosBancariosParams,
    ControlDebitosBancariosReport,
    ControlDebitosBancariosSyncParams,
)


# --------------------------------------------------
@dataclass
class ControlDebitosBancariosService:
    control_debitos_bancarios_repo: ControlDebitosBancariosRepositoryDependency
    sscc_banco_invico_service: BancoINVICOServiceDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency
    siif_rcg01_uejp_repo: Rcg01UejpRepositoryDependency
    siif_rcg01_uejp_handler: Rcg01Uejp = field(init=False)  # No se pasa como argumento
    siif_rpa03g_repo: Rpa03gRepositoryDependency
    siif_rpa03g_handler: Rpa03g = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_control_debitos_bancarios_from_source(
        self,
        params: ControlDebitosBancariosSyncParams = None,
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
                # 🔹 Rcg01Uejp
                self.siif_rcg01_uejp_handler = Rcg01Uejp(siif=connect_siif)
                await self.siif_rcg01_uejp_handler.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rcg01_uejp_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio)
                    )
                    return_schema.append(partial_schema)

                # 🔹 Rpa03g
                self.siif_rpa03g_handler = Rpa03g(siif=connect_siif)
                for ejercicio in ejercicios:
                    for grupo in [g.value for g in GrupoPartidaSIIF]:
                        partial_schema = await self.siif_rpa03g_handler.download_and_sync_validated_to_repository(
                            ejercicio=int(ejercicio), grupo_partida=grupo
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

    # -------------------------------------------------
    async def compute_all(
        self, params: ControlDebitosBancariosParams
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control Debitos Bancarios
            partial_schema = await self.compute_control_debitos_bancarios(params=params)
            return_schema.extend(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control de Debitos Bancarios",
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
        control_debitos_bancarios_docs = (
            await self.control_debitos_bancarios_repo.find_by_filter(
                {"ejercicio": {"$in": ultimos_ejercicios}}
            )
        )

        if not control_debitos_bancarios_docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        siif = pd.DataFrame()
        sscc = pd.DataFrame()
        for ejercicio in ultimos_ejercicios:
            df = await self.generate_siif_debitos_bancarios(ejercicio=ejercicio)
            siif = pd.concat([siif, df], ignore_index=True)
            df = await self.generate_banco_debitos(ejercicio=ejercicio)
            sscc = pd.concat([sscc, df], ignore_index=True)

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (pd.DataFrame(control_debitos_bancarios_docs), "new_siif_vs_sscc_db"),
                (siif, "new_siif_db"),
                (sscc, "new_sgf_db"),
            ],
            filename="control_debitos_bancarios.xlsx",
            spreadsheet_key="1i9vQ-fw_MkuHRE_YKa_diaVDu5RsiBE1UPTNAsmxLS4",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # --------------------------------------------------
    async def generate_siif_debitos_bancarios(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        df = await get_siif_comprobantes_gtos_unified_cta_cte(
            ejercicio=ejercicio, partidas=["355"]
        )
        df = df.reset_index()
        return df

    # --------------------------------------------------
    async def siif_summarize(
        self,
        ejercicio: int = None,
        groupby_cols: List[str] = ["ejercicio", "mes", "cta_cte"],
    ) -> pd.DataFrame:
        """
        Summarize SIIF data for the specified groupby columns.

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by.
                Defaults to ['ejercicio', 'mes', 'cta_cte']. It could also include 'fecha'

        Returns:
            pd.DataFrame: A DataFrame summarizing the SIIF data based on the specified groupby columns.

        This method imports and processes SIIF data related to the 'Debitos Bancarios' category. It then groups
        the data by the specified columns, calculates the sum of numeric values, and returns the resulting
        DataFrame. Any missing values are filled with zeros.

        Example:
            To summarize 'siif' data based on custom grouping columns:

            ```python
            siif_summary = control.siif_summarize(groupby_cols=["ejercicio", "mes"])
            ```
        """
        df = await self.generate_siif_debitos_bancarios(ejercicio=ejercicio)
        df = df.groupby(groupby_cols)["importe"].sum()
        df = df.reset_index()
        df = df.fillna(0)
        return df

    # --------------------------------------------------
    async def generate_banco_debitos(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        codigos_imputacion = ["031"]
        filters = {
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
        groupby_cols: List[str] = ["ejercicio", "mes", "cta_cte"],
    ) -> pd.DataFrame:
        """
        Summarize and filter bank data related to INVICO (Instituto de Vivienda de Corrientes).

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by for
                summarization. Defaults to ['ejercicio', 'mes', 'cta_cte'].
                It could also include 'fecha'

        Returns:
            pd.DataFrame: A DataFrame containing summarized and filtered bank data related to INVICO.

        This method imports bank data related to INVICO for the specified exercise (year),
        filters out specific 'cod_imputacion' values, and summarizes the data based on the
        provided groupby columns. The resulting DataFrame contains the summarized and
        filtered bank data for further analysis.
        """
        df = await self.generate_banco_debitos(ejercicio=ejercicio)
        df = df.groupby(groupby_cols)["importe"].sum()
        df = df.reset_index()
        df["importe"] = df["importe"] * -1
        return df

    # --------------------------------------------------
    async def compute_control_debitos_bancarios(
        self,
        params: ControlDebitosBancariosParams,
        only_diff=False,
    ) -> List[RouteReturnSchema]:
        return_schema = []
        groupby_cols = ["ejercicio", "mes", "cta_cte"]
        try:
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                siif = await self.siif_summarize(
                    ejercicio=ejercicio, groupby_cols=groupby_cols
                )
                siif = siif.rename(columns={"importe": "ejecutado_siif"})
                siif = siif.set_index(groupby_cols)
                sscc = await self.sscc_summarize(
                    ejercicio=ejercicio, groupby_cols=groupby_cols
                )
                sscc = sscc.rename(columns={"importe": "debitos_sscc"})
                sscc = sscc.set_index(groupby_cols)
                # Obtener los índices faltantes en siif
                missing_indices = sscc.index.difference(siif.index)
                # Reindexar el DataFrame siif con los índices faltantes
                siif = siif.reindex(siif.index.union(missing_indices))
                sscc = sscc.reindex(siif.index)
                siif = siif.fillna(0)
                sscc = sscc.fillna(0)
                df = siif.merge(sscc, how="outer", on=groupby_cols)
                df = df.reset_index()
                df = df.fillna(0)
                df["diferencia"] = df["ejecutado_siif"] - df["debitos_sscc"]
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
                    model=ControlDebitosBancariosReport,
                    field_id="mes",
                )

                partial_schema = await sync_validated_to_repository(
                    repository=self.control_debitos_bancarios_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title=f"Control Debitos Bancarios del ejercicio {ejercicio}",
                    logger=logger,
                    label=f"Control Debitos Bancarios del ejercicio {ejercicio}",
                )
                return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control Debitos Bancarios",
            )
        except Exception as e:
            logger.error(f"Error in Control Debitos Bancarios: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in Control Debitos Bancarios",
            )
        finally:
            return return_schema


ControlDebitosBancariosServiceDependency = Annotated[
    ControlDebitosBancariosService, Depends()
]
