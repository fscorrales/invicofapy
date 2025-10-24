#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: SIIF Haberes vs SSCC
Data required:
    - SLAVE
    - SIIF rcg01_uejp
    - SIIF rpa03g
    - SGF Resumen de Rendiciones
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1fQhp1CdESnvqzrp3QMV5bFSHmGdi7SNoaBRWtmw-JgA
"""

__all__ = ["ControlHonorariosService", "ControlHonorariosServiceDependency"]

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
from ...slave.handlers import SlaveMongoMigrator
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
    get_siif_comprobantes_honorarios,
    get_slave_honorarios,
)
from ..repositories.control_honorarios import (
    ControlHonorariosSGFvsSlaveRepositoryDependency,
    ControlHonorariosSIIFvsSlaveRepositoryDependency,
)
from ..schemas.control_honorarios import (
    ControlHonorariosParams,
    ControlHonorariosSGFvsSlaveReport,
    ControlHonorariosSIIFvsSlaveReport,
    ControlHonorariosSyncParams,
)


# --------------------------------------------------
@dataclass
class ControlHonorariosService:
    control_siif_vs_slave_repo: ControlHonorariosSIIFvsSlaveRepositoryDependency
    control_sgf_vs_slave_repo: ControlHonorariosSGFvsSlaveRepositoryDependency
    sscc_banco_invico_service: BancoINVICOServiceDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency
    siif_rcg01_uejp_repo: Rcg01UejpRepositoryDependency
    siif_rcg01_uejp_handler: Rcg01Uejp = field(init=False)  # No se pasa como argumento
    siif_rpa03g_repo: Rpa03gRepositoryDependency
    siif_rcocc31_handler: Rpa03g = field(init=False)  # No se pasa como argumento
    sgf_resumend_rend_prov_service: ResumenRendProvServiceDependency

    # -------------------------------------------------
    async def sync_control_honorarios_from_source(
        self,
        params: ControlHonorariosSyncParams = None,
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
                # 🔹 Rcg01Uejp
                self.siif_rcg01_uejp_handler = Rcg01Uejp(siif=connect_siif)
                await self.siif_rcg01_uejp_handler.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rcg01_uejp_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio)
                    )
                    return_schema.append(partial_schema)

                # 🔹 Rpa03g
                self.siif_rcocc31_handler = Rpa03g(siif=connect_siif)
                for ejercicio in ejercicios:
                    for grupo in [g.value for g in GrupoPartidaSIIF]:
                        partial_schema = await self.siif_rcocc31_handler.download_and_sync_validated_to_repository(
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

                # 🔹Resumen Rendicion Proveedores
                partial_schema = await self.sgf_resumend_rend_prov_service.sync_resumen_rend_prov_from_sgf(
                    username=params.sgf_username,
                    password=params.sgf_password,
                    params=params,
                )
                return_schema.extend(partial_schema)

                # 🔹 Slave
                migrator = SlaveMongoMigrator(access_path=params.slave_access_path)
                return_schema.extend(await migrator.migrate_all())

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
        self, params: ControlHonorariosParams
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control Honorarios
            partial_schema = await self.compute_control_siif_vs_slave(params=params)
            return_schema.extend(partial_schema)
            partial_schema = await self.compute_control_sgf_vs_slave(
                params=params, only_importe_bruto=False, only_diff=True
            )
            return_schema.extend(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control de Honorarios",
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
        control_siif_vs_slave_docs = (
            await self.control_siif_vs_slave_repo.find_by_filter(
                {"ejercicio": {"$in": ultimos_ejercicios}}
            )
        )
        control_sgf_vs_slave_docs = await self.control_sgf_vs_slave_repo.find_by_filter(
            {"ejercicio": {"$in": ultimos_ejercicios}}
        )

        if not control_siif_vs_slave_docs and not control_sgf_vs_slave_docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        siif = pd.DataFrame()
        slave = pd.DataFrame()
        sgf = pd.DataFrame()
        for ejercicio in ultimos_ejercicios:
            df = await get_siif_comprobantes_honorarios(ejercicio=ejercicio)
            siif = pd.concat([siif, df], ignore_index=True)
            df = await self.generate_slave_honorarios(
                ejercicio=ejercicio, add_cta_cte=True
            )
            slave = pd.concat([slave, df], ignore_index=True)
            df = await self.generate_sgf_honorarios(ejercicio=ejercicio, dep_emb=True)
            sgf = pd.concat([sgf, df], ignore_index=True)

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (pd.DataFrame(control_siif_vs_slave_docs), "siif_vs_slave_db"),
                (pd.DataFrame(control_sgf_vs_slave_docs), "sgf_vs_slave_db"),
                (siif, "siif_db"),
                (slave, "slave_db"),
                (sgf, "sgf_db"),
            ],
            filename="control_honorarios.xlsx",
            spreadsheet_key="1fQhp1CdESnvqzrp3QMV5bFSHmGdi7SNoaBRWtmw-JgA",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # --------------------------------------------------
    async def siif_summarize(
        self,
        ejercicio: int = None,
        groupby_cols: List[str] = ["ejercicio", "mes", "nro_comprobante", "cta_cte"],
    ) -> pd.DataFrame:
        """
        Summarize and aggregate SIIF comprobante data.

        This method imports SIIF comprobante data using the `import_siif_comprobantes` method and summarizes it by aggregating
        it based on the specified grouping columns. It returns a DataFrame containing the summarized data.

        Args:
            groupby_cols (List[str], optional): A list of column names used for grouping and summarizing the data.
                Defaults to ['ejercicio', 'mes', 'nro_comprobante', 'cta_cte'].

        Returns:
            pd.DataFrame: A DataFrame containing summarized SIIF comprobante data.

        Notes:
            - Data import: Imports comprobante data from the SIIF system using the `import_siif_comprobantes` method.
            - Data aggregation: Aggregates the imported data by grouping it based on the specified columns and calculating
            the sum of numeric columns.
            - Reset index: Resets the DataFrame index for consistency.
        """
        df = await get_siif_comprobantes_honorarios(ejercicio=ejercicio)
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        df = df.fillna(0)
        return df

    # --------------------------------------------------
    async def generate_slave_honorarios(
        self,
        ejercicio: int = None,
        add_cta_cte: bool = False,
    ) -> pd.DataFrame:
        df = await get_slave_honorarios(ejercicio=ejercicio)
        df = df.rename(columns={"otras_retenciones": "otras"})
        df["otras"] = (
            df["otras"]
            + df["anticipo"]
            + df["descuento"]
            + df["embargo"]
            + df["mutual"]
        )
        df["sellos"] = df["sellos"] + df["lp"]
        df = df.drop(columns=["anticipo", "descuento", "embargo", "lp", "mutual"])
        df["retenciones"] = df["iibb"] + df["sellos"] + df["seguro"] + df["otras"]
        df["importe_neto"] = df["importe_bruto"] - df["retenciones"]
        if add_cta_cte:
            cta_cte = await get_siif_comprobantes_honorarios(ejercicio=ejercicio)
            cta_cte = cta_cte.loc[:, ["nro_comprobante", "cta_cte"]]
            cta_cte = cta_cte.drop_duplicates()
            df = df.merge(cta_cte, on="nro_comprobante", how="left")
            df = df.fillna(0)
        return df

    # --------------------------------------------------
    async def slave_summarize(
        self,
        ejercicio: int = None,
        groupby_cols: List[str] = ["ejercicio", "mes", "nro_comprobante", "cta_cte"],
        only_importe_bruto=False,
    ) -> pd.DataFrame:
        """
        Summarize financial data from the "slave" system.

        This method summarizes financial data from the "slave" system by performing aggregation based on the specified
        grouping columns. It returns a DataFrame containing the summarized data.

        Args:
            groupby_cols (List[str], optional): List of column names to group the data by. Defaults to
                ['ejercicio', 'mes', 'nro_comprobante', 'cta_cte'].
            only_importe_bruto (bool, optional): If True, includes only the 'importe_bruto' column in the resulting DataFrame.
                Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame containing summarized financial data from the "slave" system.

        Notes:
            - Data aggregation: Aggregates financial data based on the specified grouping columns.
            - Optional column selection: Allows including only the 'importe_bruto' column if 'only_importe_bruto' is True.
            - Missing values: Fills missing values with 0.
        """
        df = await self.generate_slave_honorarios(
            ejercicio=ejercicio, add_cta_cte="cta_cte" in groupby_cols
        )
        if only_importe_bruto:
            df = df.loc[:, groupby_cols + ["importe_bruto"]]
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        df = df.fillna(0)
        return df

    # --------------------------------------------------
    async def generate_sgf_honorarios(
        self,
        ejercicio: int = None,
        dep_emb: bool = True,
    ) -> pd.DataFrame:
        df = await get_resumen_rend_honorarios(ejercicio=ejercicio)
        if dep_emb:
            filters = {"cod_imputacion": "049"}
            banco = await get_banco_invico_unified_cta_cte(
                ejercicio=ejercicio, filters=filters
            )
            banco = banco.loc[banco["cta_cte"] == "130832-05"]
            banco["importe_bruto"] = banco["importe"] * (-1)
            banco["importe_neto"] = 0
            banco["otras"] = banco["importe_bruto"]
            banco["retenciones"] = banco["importe_bruto"]
            banco["destino"] = "EMBARGO POR ALIMENTOS"
            banco["beneficiario"] = "EMBARGO POR ALIMENTOS"
            banco.rename(
                columns={
                    "libramiento": "libramiento_sgf",
                },
                inplace=True,
            )
            banco = banco.loc[
                :,
                [
                    "ejercicio",
                    "mes",
                    "fecha",
                    "beneficiario",
                    "destino",
                    "cta_cte",
                    "libramiento_sgf",
                    "movimiento",
                    "importe_bruto",
                    "otras",
                    "retenciones",
                    "importe_neto",
                ],
            ]
            df = pd.concat([df, banco])
            df = df.fillna(0)
        df["otras"] = (
            df["otras"]
            + df["gcias"]
            + df["suss"]
            + df["invico"]
            + df["salud"]
            + df["mutual"]
        )
        df = df.drop(["gcias", "suss", "invico", "salud", "mutual"], axis=1)
        return df

    # --------------------------------------------------
    async def sgf_summarize(
        self,
        ejercicio: int = None,
        groupby_cols: List[str] = ["ejercicio", "mes", "cuit", "beneficiario"],
        only_importe_bruto=False,
        dep_emb: bool = True,
    ) -> pd.DataFrame:
        """
        Summarize SGF (Sistema de Gestión Financiera) data.

        This method summarizes financial data from SGF for specific grouping columns. It returns the summarized data as
        a DataFrame.

        Args:
            groupby_cols (List[str], optional): A list of column names to group the data by. Defaults to
                ['ejercicio', 'mes', 'cuit', 'beneficiario'].
            only_importe_bruto (bool, optional): If True, only the 'importe_bruto' column is included in the resulting
                DataFrame. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame containing the summarized SGF financial data based on the specified grouping columns.

        Notes:
            - Data import: Imports financial data from SGF using the `import_resumen_rend_honorarios` method from the
            superclass.
            - DataFrame transformation: Groups the data by the specified columns and calculates the sum for numeric
            columns. If 'only_importe_bruto' is True, the DataFrame is filtered to include only 'importe_bruto'.
        """
        df = await self.generate_sgf_honorarios(ejercicio=ejercicio, dep_emb=dep_emb)
        if only_importe_bruto:
            df = df.loc[:, groupby_cols + ["importe_bruto"]]
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        return df

    # --------------------------------------------------
    async def compute_control_siif_vs_slave(
        self,
        params: ControlHonorariosParams,
    ) -> List[RouteReturnSchema]:
        return_schema = []
        groupby_cols = ["ejercicio", "mes", "nro_comprobante"]
        try:
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                siif = await self.siif_summarize(
                    ejercicio=ejercicio, groupby_cols=groupby_cols
                )
                siif = siif.rename(
                    columns={
                        "importe": "siif_importe",
                        "nro_comprobante": "siif_nro",
                        "mes": "siif_mes",
                    }
                )
                # print(f"siif.shape: {siif.shape} - siif.head: {siif.head()}")
                slave = await self.slave_summarize(
                    ejercicio=ejercicio,
                    groupby_cols=groupby_cols,
                    only_importe_bruto=True,
                )
                slave = slave.rename(
                    columns={
                        "importe_bruto": "slave_importe",
                        "nro_comprobante": "slave_nro",
                        "mes": "slave_mes",
                    }
                )
                # print(f"slave.shape: {slave.shape} - slave.head: {slave.head()}")
                df = pd.merge(
                    siif,
                    slave,
                    how="outer",
                    left_on=["ejercicio", "siif_nro"],
                    right_on=["ejercicio", "slave_nro"],
                    copy=False,
                )
                df = df.fillna(0)
                df["err_nro"] = df["siif_nro"] != df["slave_nro"]
                df["err_importe"] = np.where(
                    np.abs(df["siif_importe"] - df["slave_importe"]) > 0.01, True, False
                )
                df["err_mes"] = df["siif_mes"] != df["slave_mes"]
                df = df.loc[
                    :,
                    [
                        "ejercicio",
                        "siif_nro",
                        "slave_nro",
                        "err_nro",
                        "siif_importe",
                        "slave_importe",
                        "err_importe",
                        "siif_mes",
                        "slave_mes",
                        "err_mes",
                    ],
                ]
                # print(f"df.shape: {df.shape} - df.head: {df.head()}")
                df = df.query("err_nro | err_mes | err_importe")
                df = df.sort_values(
                    by=["err_nro", "err_importe", "err_mes"], ascending=False
                )
                df = df.reset_index(drop=True)
                # print(f"df.shape: {df.shape} - df.head: {df.head()}")

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df,
                    model=ControlHonorariosSIIFvsSlaveReport,
                    field_id="siif_nro",
                )
                # print(f"validate_and_errors: {validate_and_errors}")

                partial_schema = await sync_validated_to_repository(
                    repository=self.control_siif_vs_slave_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title=f"Control SIIF vs Slave del ejercicio {ejercicio}",
                    logger=logger,
                    label=f"Control SIIF vs Slave del ejercicio {ejercicio}",
                )
                return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from SIIF vs Slave Control Honorarios",
            )
        except Exception as e:
            logger.error(f"Error in SIIF vs Slave Control Honorarios: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in SIIF vs Slave Control Honorarios",
            )
        finally:
            return return_schema

    # --------------------------------------------------
    async def compute_control_sgf_vs_slave(
        self, params: ControlHonorariosParams, only_importe_bruto=False, only_diff=False
    ) -> List[RouteReturnSchema]:
        return_schema = []
        groupby_cols = ["ejercicio", "mes", "cta_cte", "beneficiario"]
        try:
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                sgf = await self.sgf_summarize(
                    ejercicio=ejercicio,
                    groupby_cols=groupby_cols,
                    only_importe_bruto=only_importe_bruto,
                )
                # print(f"sgf.shape: {sgf.shape} - sgf.head: {sgf.head()}")
                slave = await self.slave_summarize(
                    ejercicio=ejercicio,
                    groupby_cols=groupby_cols,
                    only_importe_bruto=only_importe_bruto,
                )
                slave = slave.set_index(groupby_cols)
                # print(f"slave.shape: {slave.shape} - slave.head: {slave.head()}")
                sgf = sgf.set_index(groupby_cols)
                # Obtener los índices faltantes en slave
                missing_indices = sgf.index.difference(slave.index)
                # Reindexar el DataFrame slave con los índices faltantes
                slave = slave.reindex(slave.index.union(missing_indices))
                sgf = sgf.reindex(slave.index)
                slave = slave.fillna(0)
                sgf = sgf.fillna(0)
                df = slave.subtract(sgf)
                df = df.reset_index()
                # Reindexamos el DataFrame
                slave = slave.reset_index()
                df = df.reindex(columns=slave.columns)
                if only_diff:
                    # Seleccionar solo las columnas numéricas
                    numeric_cols = df.select_dtypes(include=np.number).columns
                    # Filtrar el DataFrame utilizando las columnas numéricas válidas
                    # df = df[df[numeric_cols].sum(axis=1) != 0]
                    df = df[~(np.abs(df[numeric_cols].sum(axis=1)) > 0.01)]
                    df = df.reset_index(drop=True)

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df,
                    model=ControlHonorariosSGFvsSlaveReport,
                    field_id="beneficiario",
                )

                partial_schema = await sync_validated_to_repository(
                    repository=self.control_sgf_vs_slave_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title=f"Control SGF vs Slave del ejercicio {ejercicio}",
                    logger=logger,
                    label=f"Control SGF vs Slave del ejercicio {ejercicio}",
                )
                return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from SGF vs Slave Control Honorarios",
            )
        except Exception as e:
            logger.error(f"Error in SGF vs Slave Control Honorarios: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in SGF vs Slave Control Honorarios",
            )
        finally:
            return return_schema


ControlHonorariosServiceDependency = Annotated[ControlHonorariosService, Depends()]
