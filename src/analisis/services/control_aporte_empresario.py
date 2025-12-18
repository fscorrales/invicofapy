#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control de Aporte de Empresarios (3% INVICO)
Data required:
    - SIIF rci02
    - SIIF rcocc31 (1112-2-6 Banco INVICO y 2122-1-2 Retenciones)
    - SSCC ctas_ctes (manual data)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1bZnvl9YkHC-N1HbIbnFNrqU3Iq03PG81u7fdHe_v_pw
"""

__all__ = ["ControlAporteEmpresarioService", "ControlAporteEmpresarioServiceDependency"]

from dataclasses import dataclass, field
from typing import Annotated, List

import numpy as np
import pandas as pd
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import logger
from ...siif.handlers import (
    Rci02,
    Rcocc31,
    login,
    logout,
)
from ...sscc.repositories import CtasCtesRepository
from ...sscc.services import CtasCtesServiceDependency
from ...utils import (
    GoogleExportResponse,
    RouteReturnSchema,
    export_multiple_dataframes_to_excel,
    sync_validated_to_repository,
    upload_multiple_dataframes_to_google_sheets,
    validate_and_extract_data_from_df,
)
from ..handlers import (
    get_siif_rci02_unified_cta_cte,
    get_siif_rcocc31,
)
from ..repositories.control_aporte_empresario import (
    ControlAporteEmpresarioRepositoryDependency,
)
from ..schemas.control_aporte_empresario import (
    ControlAporteEmpresarioParams,
    ControlAporteEmpresarioReport,
    ControlAporteEmpresarioSyncParams,
)


# --------------------------------------------------
@dataclass
class ControlAporteEmpresarioService:
    control_aporte_empresario_repo: ControlAporteEmpresarioRepositoryDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency
    siif_rcocc31_handler: Rcocc31 = field(init=False)  # No se pasa como argumento
    siif_rci02_handler: Rci02 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_control_aporte_empresario_from_source(
        self,
        params: ControlAporteEmpresarioSyncParams = None,
    ) -> List[RouteReturnSchema]:
        """Downloads a report from all sources, processes it, validates the data,
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
                # 🔹Rci02
                self.siif_rci02_handler = Rci02(siif=connect_siif)
                await self.siif_rci02_handler.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rci02_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio),
                    )
                    return_schema.append(partial_schema)

                # 🔹 Rcocc31
                self.siif_rcocc31_handler = Rcocc31(siif=connect_siif)
                for ejercicio in ejercicios:
                    for cta_contable in ["1112-2-6", "2122-1-2"]:
                        partial_schema = await self.siif_rcocc31_handler.download_and_sync_validated_to_repository(
                            ejercicio=int(ejercicio),
                            cta_contable=cta_contable,
                        )
                        return_schema.append(partial_schema)

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
        self, params: ControlAporteEmpresarioParams
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control Aporte Empresario
            partial_schema = await self.compute_control_aporte_empresario(
                params=params, only_diff=True
            )
            return_schema.extend(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control de Aporte Empresario",
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
        params: ControlAporteEmpresarioParams,
    ) -> list[tuple[pd.DataFrame, str]]:
        ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
        control_aporte_empresario_docs = (
            await self.control_aporte_empresario_repo.find_by_filter(
                {"ejercicio": {"$in": ejercicios}}
            )
        )

        if not control_aporte_empresario_docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        siif_recurso = pd.DataFrame()
        siif_retencion = pd.DataFrame()
        for ejercicio in ejercicios:
            df = await self.generate_siif_recursos(ejercicio=ejercicio)
            siif_recurso = pd.concat([siif_recurso, df], ignore_index=True)
            df = await self.generate_siif_retenciones(ejercicio=ejercicio)
            siif_retencion = pd.concat([siif_retencion, df], ignore_index=True)

        return [
            (
                pd.DataFrame(control_aporte_empresario_docs),
                "recursos_vs_retenciones_db",
            ),
            (siif_recurso, "recursos_db"),
            (siif_retencion, "retenciones_db"),
        ]

    # -------------------------------------------------
    async def export_all_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ControlAporteEmpresarioParams = None,
    ) -> StreamingResponse:
        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=await self._build_dataframes_to_export(params),
            filename="control_aporte_empresario.xlsx",
            spreadsheet_key="1bZnvl9YkHC-N1HbIbnFNrqU3Iq03PG81u7fdHe_v_pw",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # -------------------------------------------------
    async def export_all_from_db_to_google(
        self,
        params: ControlAporteEmpresarioParams = None,
    ) -> GoogleExportResponse:
        return upload_multiple_dataframes_to_google_sheets(
            df_sheet_pairs=await self._build_dataframes_to_export(params),
            spreadsheet_key="1bZnvl9YkHC-N1HbIbnFNrqU3Iq03PG81u7fdHe_v_pw",
            title="Control Aporte Empresario",
        )

    # --------------------------------------------------
    async def generate_siif_recursos(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        """
        Import SIIF (Sistema Integrado de Información Financiera) data for 3% resources related to INVICO.

        Returns:
            pd.DataFrame: A DataFrame containing SIIF data specifically related to 3% resources
            associated with INVICO.

        This method imports SIIF data for the specified exercise (year) from the RCI02 table.
        It filters the data to include only records marked as related to INVICO and verified.
        The resulting DataFrame contains information regarding 3% resources related to INVICO.
        """
        filters = {
            "es_invico": True,
            "es_verificado": True,
        }
        df = await get_siif_rci02_unified_cta_cte(ejercicio=ejercicio, filters=filters)
        df = df.rename(
            columns={
                "importe": "recurso",
            }
        )
        # df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    async def siif_summarize_recursos(
        self,
        ejercicio: int = None,
        groupby_cols: List[str] = ["ejercicio", "mes", "cta_cte"],
    ) -> pd.DataFrame:
        """
        Summarize SIIF (Sistema Integrado de Información Financiera) data for 3% resources related to INVICO.

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by for summarization.
                Defaults to ['ejercicio', 'mes', 'cta_cte'].

        Returns:
            pd.DataFrame: A DataFrame containing summarized SIIF data specifically related to 3%
            resources associated with INVICO.

        This method summarizes SIIF data for the specified exercise (year), focusing on 3% resources
        related to INVICO. It imports and filters the data, grouping it based on the provided columns.
        The resulting DataFrame contains summarized SIIF data for 3% resources linked to INVICO.
        """
        df = await self.generate_siif_recursos(ejercicio=ejercicio)
        df = df.drop(["es_remanente", "es_verificado", "es_invico"], axis=1)
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        df = df.fillna(0)
        return df

    # --------------------------------------------------
    async def generate_siif_retenciones(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        """
        Import SIIF (Sistema Integrado de Información Financiera) data for 3% resources related to INVICO.

        Returns:
            pd.DataFrame: A DataFrame containing SIIF data specifically related to 3% resources
            associated with INVICO.

        This method imports SIIF data for the specified exercise (year) from the RCI02 table.
        It filters the data to include only records marked as related to INVICO and verified.
        The resulting DataFrame contains information regarding 3% resources related to INVICO.
        """
        filters = {
            "tipo_comprobante": {"$ne": "APE"},
            "cta_contable": "1112-2-6",
        }
        siif_banco = await get_siif_rcocc31(ejercicio=ejercicio, filters=filters)
        siif_banco = siif_banco.loc[
            :,
            ["ejercicio", "nro_entrada", "auxiliar_1"],
        ]
        # print(f"siif_banco.shape: {siif_banco.shape} - siif_banco.head: {siif_banco.head()}")
        siif_banco = siif_banco.rename(columns={"auxiliar_1": "cta_cte"})
        filters = {
            "tipo_comprobante": {"$ne": "APE"},
            "cta_contable": "2122-1-2",
            "auxiliar_1": "337",
        }
        siif_337 = await get_siif_rcocc31(ejercicio=ejercicio, filters=filters)
        siif_337 = siif_337.loc[
            :,
            [
                "ejercicio",
                "mes",
                "fecha",
                "nro_entrada",
                "tipo_comprobante",
                "debitos",
                "creditos",
            ],
        ]
        siif_337 = siif_337.rename(
            columns={
                "debitos": "retencion_pagada",
                "creditos": "retencion_practicada",
            }
        )
        df = siif_337.merge(siif_banco, how="left", on=["ejercicio", "nro_entrada"])
        # print(f"df.shape: {df.shape} - df.head: {df.head()}")
        ctas_ctes = pd.DataFrame(await CtasCtesRepository().get_all())
        map_to = ctas_ctes.loc[:, ["map_to", "siif_contabilidad_cta_cte"]]
        df = pd.merge(
            df,
            map_to,
            how="left",
            left_on="cta_cte",
            right_on="siif_contabilidad_cta_cte",
        )
        df["cta_cte"] = df["map_to"]
        df.drop(["map_to", "siif_contabilidad_cta_cte"], axis="columns", inplace=True)
        return df

    # --------------------------------------------------
    async def siif_summarize_retenciones(
        self,
        ejercicio: int = None,
        groupby_cols: List[str] = ["ejercicio", "mes"],
    ) -> pd.DataFrame:
        """
        Summarize SIIF (Sistema Integrado de Información Financiera) data related to code 337 retentions.

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by for summarization.
                Defaults to ['ejercicio', 'mes'].

        Returns:
            pd.DataFrame: A DataFrame containing summarized SIIF data related to code 337 retentions.

        This method fetches SIIF data pertinent to code 337 retentions using the 'import_siif_retencion_337'
        function. It then performs a summarization based on the provided groupby columns. The resulting DataFrame
        presents the summarized SIIF data specifically related to retentions under code 337, organized according
        to the specified grouping for further analysis or utilization.
        """
        df = await self.generate_siif_retenciones(ejercicio=ejercicio)
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        df = df.fillna(0)
        return df

    # --------------------------------------------------
    async def compute_control_aporte_empresario(
        self, params: ControlAporteEmpresarioParams, only_diff=False
    ) -> List[RouteReturnSchema]:
        """
        Compare SIIF resource data against code 337 retentions.

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by for comparison.
                Defaults to ['ejercicio', 'mes', 'cta_cte'].
            only_diff (bool, optional): If True, only returns rows with differences between SIIF resource
                and code 337 retentions data. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame containing a comparison between SIIF resource data and code 337
            retentions data.

        This method compares SIIF resource data, summarized using 'siif_summarize_recurso_3_percent',
        with code 337 retention data, summarized using 'siif_summarize_retencion_337'. It aligns both datasets
        based on the provided groupby columns and produces a DataFrame highlighting the differences between the
        two datasets when 'only_diff' is True. If 'only_diff' is False, it returns a DataFrame with the merged
        data, suitable for further analysis or review.
        """
        return_schema = []
        groupby_cols = ["ejercicio", "mes", "cta_cte"]
        try:
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                siif_recursos = await self.siif_summarize_recursos(
                    groupby_cols=groupby_cols, ejercicio=ejercicio
                )
                # print(f"siif_recursos.shape: {siif_recursos.shape} - siif_recursos.head: {siif_recursos.head()}")
                siif_recursos = siif_recursos.set_index(groupby_cols)
                siif_retenciones = await self.siif_summarize_retenciones(
                    groupby_cols=groupby_cols, ejercicio=ejercicio
                )
                siif_retenciones.drop(
                    ["retencion_practicada"], axis="columns", inplace=True
                )
                siif_retenciones = siif_retenciones.rename(
                    columns={"retencion_pagada": "retencion"}
                )
                siif_retenciones["retencion"] = siif_retenciones["retencion"] * (-1)
                # print(
                #     f"siif_retenciones.shape: {siif_retenciones.shape} - siif_retenciones.head: {siif_retenciones.head()}"
                # )
                siif_retenciones = siif_retenciones.set_index(groupby_cols)
                df = siif_recursos.merge(
                    siif_retenciones, how="outer", left_index=True, right_index=True
                )
                df = df.reset_index()
                df = df.fillna(0)
                # print(f"df.shape: {df.shape} - df.head: {df.head()}")
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
                    model=ControlAporteEmpresarioReport,
                    field_id="mes",
                )

                partial_schema = await sync_validated_to_repository(
                    repository=self.control_aporte_empresario_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title=f"Control Aporte Empresario del ejercicio {ejercicio}",
                    logger=logger,
                    label=f"Control Aporte Empresario del ejercicio {ejercicio}",
                )
                return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Aporte Empresario",
            )
        except Exception as e:
            logger.error(f"Error in Aporte Empresario: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in Aporte Empresario",
            )
        finally:
            return return_schema


ControlAporteEmpresarioServiceDependency = Annotated[
    ControlAporteEmpresarioService, Depends()
]
