#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control Anticipo de Viaticos (PA3)
Data required:
    - SIIF rfondos04 (PA3 y REV)
    - SIIF rcocc31
        + 1112-2-6 Banco SIIF
        + 2113-1-13 Anticipo de Viaticos Pagados (PAV de PA3)
        + 4112-1-3 Reembolso por Gastos Mayores (Partida 373)
        + 1141-1-4 Devolución de Fondos no Utilizados (REV)
    - SSCC Resumen General de Movimientos
        + 029 Anticipo de Viaticos Pagados (PAV de PA3)
        + 040 Reembolso por Gastos Mayores (Partida 373)
        + 005 Devolución de Fondos no Utilizados (REV)
    - SSCC ctas_ctes (manual data)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1alo5UBd7YRIBhSXqfo_c5Lz8JBfU3CXrVC0XOmtiH8c
"""

__all__ = ["ControlViaticosService", "ControlViaticosServiceDependency"]


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
    Rcg01Uejp,
    Rcocc31,
    Rfondo07tp,
    Rfondos04,
    Rpa03g,
    login,
    logout,
)
from ...siif.repositories import (
    Rcocc31RepositoryDependency,
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
    get_siif_comprobantes_gtos_unified_cta_cte,
    get_siif_rcocc31,
    get_siif_rfondo07tp,
    get_siif_rfondos04,
)
from ..repositories.control_viaticos import (
    ControlViaticosRendicionRepositoryDependency,
)
from ..schemas.control_viaticos import (
    ControlViaticosParams,
    ControlViaticosRendicionReport,
    ControlViaticosSyncParams,
)


# --------------------------------------------------
@dataclass
class ControlViaticosService:
    control_rendicion_repo: ControlViaticosRendicionRepositoryDependency
    # control_siif_vs_sgf_repo: ControlEscribanosSIIFvsSGFRepositoryDependency
    sscc_banco_invico_service: BancoINVICOServiceDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency
    siif_rcocc31_handler: Rcocc31 = field(init=False)  # No se pasa como argumento
    siif_rcg01_uejp_handler: Rcg01Uejp = field(init=False)  # No se pasa como argumento
    siif_rpa03g_handler: Rpa03g = field(init=False)  # No se pasa como argumento
    siif_rfondo07tp_handler: Rfondo07tp = field(init=False)  # No se pasa como argumento
    siif_rfondos04_handler: Rfondos04 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_control_viaticos_from_source(
        self,
        params: ControlViaticosSyncParams = None,
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

                # # 🔹 Rfondo07tp
                # self.siif_rfondo07tp_handler = Rfondo07tp(siif=connect_siif)
                # for ejercicio in ejercicios:
                #     partial_schema = await self.siif_rfondo07tp_handler.download_and_sync_validated_to_repository(
                #         ejercicio=int(ejercicio), tipo_comprobante="PA3"
                #     )
                #     return_schema.append(partial_schema)

                # 🔹 Rfondos04
                self.siif_rfondos04_handler = Rfondos04(siif=connect_siif)
                for ejercicio in ejercicios:
                    for tipo_comprobante in ["PA3", "REV"]:
                        partial_schema = await self.siif_rfondos04_handler.download_and_sync_validated_to_repository(
                            ejercicio=int(ejercicio), tipo_comprobante=tipo_comprobante
                        )
                        return_schema.append(partial_schema)

                # 🔹 Rcocc31
                self.siif_rcocc31_handler = Rcocc31(siif=connect_siif)
                cuentas_contables = ["1112-2-6", "2113-1-13", "4112-1-3", "1141-1-4"]
                for ejercicio in ejercicios:
                    for cta_contable in cuentas_contables:
                        partial_schema = await self.siif_rcocc31_handler.download_and_sync_validated_to_repository(
                            ejercicio=int(ejercicio), cta_contable=cta_contable
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
        self, params: ControlViaticosParams
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control Viaticos
            partial_schema = await self.compute_control_rendicion(params=params)
            return_schema.extend(partial_schema)
            # partial_schema = await self.compute_control_siif_vs_sgf(
            #     params=params, only_diff=False
            # )
            # return_schema.extend(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control de Viaticos",
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
        self,
        upload_to_google_sheets: bool = True,
        params: ControlViaticosParams = None,
    ) -> StreamingResponse:
        ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
        control_rendicion_docs = await self.control_rendicion_repo.find_by_filter(
            {"ejercicio": {"$in": ejercicios}}
        )
        # control_siif_vs_sgf_docs = await self.control_siif_vs_sgf_repo.find_by_filter(
        #     {"ejercicio": {"$in": ejercicios}}
        # )

        if not control_rendicion_docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        siif_fondos = pd.DataFrame()
        siif_gastos = pd.DataFrame()
        sscc = pd.DataFrame()
        for ejercicio in ejercicios:
            df = await self.generate_siif_fondo_viaticos(ejercicio=ejercicio)
            siif_fondos = pd.concat([siif_fondos, df], ignore_index=True)
            df = await self.generate_siif_rendicion_viaticos(ejercicio=ejercicio)
            siif_gastos = pd.concat([siif_gastos, df], ignore_index=True)
            df = await self.generate_banco_viaticos(ejercicio=ejercicio)
            sscc = pd.concat([sscc, df], ignore_index=True)

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (pd.DataFrame(control_rendicion_docs), "control_rendicion_db"),
                # (pd.DataFrame(control_sgf_vs_sscc_docs), "sgf_vs_sscc_db"),
                (siif_fondos, "siif_fondos_db"),
                (siif_gastos, "siif_gastos_db"),
                (sscc, "sscc_db"),
            ],
            filename="control_viaticos.xlsx",
            spreadsheet_key="1alo5UBd7YRIBhSXqfo_c5Lz8JBfU3CXrVC0XOmtiH8c",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # --------------------------------------------------
    async def generate_siif_rendicion_viaticos(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        df = await get_siif_comprobantes_gtos_unified_cta_cte(
            ejercicio=ejercicio, partidas=["372", "373"]
        )
        return df

    # --------------------------------------------------
    async def generate_siif_fondo_viaticos(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        filters = {"tipo_comprobante": {"$in": ["PA3", "REV"]}}
        df = await get_siif_rfondos04(ejercicio=ejercicio, filters=filters)
        return df

    # --------------------------------------------------
    async def generate_banco_viaticos(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        codigos_imputacion = ["029", "040", "005"]
        filters = {
            "cta_cte": "130832-02",
            "cod_imputacion": {"$in": codigos_imputacion},
        }
        df = await get_banco_invico_unified_cta_cte(
            ejercicio=ejercicio, filters=filters
        )
        df["new_concepto"] = df["concepto"].str.replace(".", "")
        df["nro_expte"] = df["new_concepto"].str.extract(r"EXP\s*(\d+\s*\d+\s*\d+)\s*")[
            0
        ]
        df = df.drop(["new_concepto"], axis=1)

        # Función para transformar el campo "nro_expte"
        def transform_nro_expte(x):
            # Extraer el número de identificación, el número de expediente y el año
            if not isinstance(x, (str, bytes)):
                return None

            if x is None or x == "":
                return None

            expediente = x.split(" ")
            id_institucion = "900"
            nro_expediente = "00000"
            año = "0000"
            if len(expediente) >= 2:
                año = expediente[-1]
                año = año if len(año) == 4 else "20" + año
                nro_expediente = expediente[-2].zfill(5)
            # Construir el nuevo formato
            new_format = f"{id_institucion}{nro_expediente}{año}"
            return new_format

        # Aplicar la función a la columna "nro_expte"
        df["nro_expte"] = df["nro_expte"].apply(transform_nro_expte)

        return df

    # --------------------------------------------------
    async def compute_control_rendicion(
        self, params: ControlViaticosParams
    ) -> List[RouteReturnSchema]:
        return_schema = []
        groupby_cols = ["ejercicio", "mes", "nro_expte"]
        try:
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                siif_fondos = await self.generate_siif_fondo_viaticos(
                    ejercicio=ejercicio
                )
                siif_fondos["siif_anticipo"] = np.where(
                    siif_fondos["tipo_comprobante"] == "PA3",
                    siif_fondos["importe"],
                    0,
                )
                siif_fondos["siif_reversion"] = np.where(
                    siif_fondos["tipo_comprobante"] == "REV",
                    siif_fondos["importe"],
                    0,
                )
                siif_fondos = siif_fondos.groupby(["nro_fondo"])[
                    [
                        "siif_anticipo",
                        "siif_reversion",
                    ]
                ].sum()
                siif_fondos = siif_fondos.reset_index()
                siif_rendicion = await self.generate_siif_rendicion_viaticos(
                    ejercicio=ejercicio
                )
                siif_rendicion = siif_rendicion.merge(
                    siif_fondos, how="left", left_on="nro_fondo", right_on="nro_fondo"
                )
                siif_rendicion["siif_rendido"] = np.where(
                    siif_rendicion["partida"] == "372",
                    siif_rendicion["importe"],
                    0,
                )
                siif_rendicion["siif_reembolso"] = np.where(
                    siif_rendicion["partida"] == "373", siif_rendicion["importe"], 0
                )
                siif_rendicion["siif_saldo"] = (
                    siif_rendicion.siif_anticipo
                    - siif_rendicion.siif_reversion
                    - siif_rendicion.siif_rendido
                )
                siif_rendicion = siif_rendicion.groupby(groupby_cols)[
                    [
                        "siif_anticipo",
                        "siif_rendido",
                        "siif_reversion",
                        "siif_saldo",
                        "siif_reembolso",
                    ]
                ].sum()
                df = siif_rendicion.reset_index()
                # sscc = await self.sscc_summarize(
                #     groupby_cols=groupby_cols, ejercicio=ejercicio
                # )
                # sscc = sscc.set_index(groupby_cols)
                # # Obtener los índices faltantes en sgf
                # missing_indices = sscc.index.difference(sgf.index)
                # # Reindexar el DataFrame sgf con los índices faltantes
                # sgf = sgf.reindex(sgf.index.union(missing_indices))
                # sscc = sscc.reindex(sgf.index)
                # sgf = sgf.fillna(0)
                # sscc = sscc.fillna(0)
                # df = sgf.subtract(sscc)
                # df = df.reset_index()
                # df = df.fillna(0)
                # # Reindexamos el DataFrame
                # sgf = sgf.reset_index()
                # df = df.reindex(columns=sgf.columns)

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df,
                    model=ControlViaticosRendicionReport,
                    field_id="nro_expte",
                )

                partial_schema = await sync_validated_to_repository(
                    repository=self.control_rendicion_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title=f"Control Rendicion Viaticos del ejercicio {ejercicio}",
                    logger=logger,
                    label=f"Control Rendicion Viaticos del ejercicio {ejercicio}",
                )
                return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control Rendicion Viaticos",
            )
        except Exception as e:
            logger.error(f"Error in Control Rendicion Viaticos: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in Control Rendicion Viaticos",
            )
        finally:
            return return_schema


ControlViaticosServiceDependency = Annotated[ControlViaticosService, Depends()]
