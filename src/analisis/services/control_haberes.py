#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: SIIF Haberes vs SSCC
Data required:
    - SIIF rcg01_uejp
    - SIIF rpa03g
    - SIIF rdeu012
    - SIIF rcocc31 (2122-1-2)
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1A9ypUkwm4kfLqUAwr6-55crcFElisOO9fOdI6iflMAc
"""

__all__ = ["ControlHaberesService", "ControlHaberesServiceDependency"]


from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, List

import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import logger
from ...siif.handlers import (
    Rcg01Uejp,
    Rcocc31,
    Rdeu012,
    Rpa03g,
    login,
    logout,
)
from ...siif.repositories import (
    Rcg01UejpRepositoryDependency,
    Rcocc31RepositoryDependency,
    Rdeu012RepositoryDependency,
    Rpa03gRepositoryDependency,
)
from ...siif.schemas import GrupoPartidaSIIF
from ...sscc.services import BancoINVICOServiceDependency, CtasCtesServiceDependency
from ...utils import (
    BaseFilterParams,
    RouteReturnSchema,
    export_dataframe_as_excel_response,
    export_multiple_dataframes_to_excel,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..handlers import (
    get_banco_invico_unified_cta_cte,
    get_siif_comprobantes_haberes,
    get_siif_rdeu012_unified_cta_cte,
)
from ..repositories.control_haberes import ControlHaberesRepositoryDependency
from ..schemas.control_haberes import (
    ControlHaberesDocument,
    ControlHaberesParams,
    ControlHaberesReport,
    ControlHaberesSyncParams,
)


# --------------------------------------------------
@dataclass
class ControlHaberesService:
    control_haberes_repo: ControlHaberesRepositoryDependency
    sscc_banco_invico_service: BancoINVICOServiceDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency
    siif_rcg01_uejp_repo: Rcg01UejpRepositoryDependency
    siif_rcg01_uejp_handler: Rcg01Uejp = field(init=False)  # No se pasa como argumento
    siif_rpa03g_repo: Rpa03gRepositoryDependency
    siif_rcocc31_handler: Rpa03g = field(init=False)  # No se pasa como argumento
    siif_rcocc31_repo: Rcocc31RepositoryDependency
    siif_rcocc31_handler: Rcocc31 = field(init=False)  # No se pasa como argumento
    siif_rdeu012_repo: Rdeu012RepositoryDependency
    siif_rdeu012_handler: Rdeu012 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_control_haberes_from_source(
        self,
        params: ControlHaberesSyncParams = None,
    ) -> List[RouteReturnSchema]:
        """Downloads a report from SIIF, processes it, validates the data,
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
                self.siif_rcocc31_handler = Rpa03g(siif=connect_siif)
                for ejercicio in ejercicios:
                    for grupo in [g.value for g in GrupoPartidaSIIF]:
                        partial_schema = await self.siif_rcocc31_handler.download_and_sync_validated_to_repository(
                            ejercicio=int(ejercicio), grupo_partida=grupo
                        )
                        return_schema.append(partial_schema)

                # 🔹 Rcocc31
                self.siif_rcocc31_handler = Rcocc31(siif=connect_siif)
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rcocc31_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio), cta_contable="2122-1-2"
                    )
                    return_schema.append(partial_schema)

                # 🔹Rdeu012
                # Obtenemos los meses a descargar
                start = datetime.strptime(str(params.ejercicio_desde), "%Y")
                end = (
                    datetime.strptime("12/" + str(params.ejercicio_hasta), "%m/%Y")
                    if params.ejercicio_hasta < datetime.now().year
                    else datetime.now().replace(day=1)
                )

                meses = []
                current = start
                while current <= end:
                    meses.append(current.strftime("%m/%Y"))
                    current += relativedelta(months=1)

                self.siif_rdeu012_handler = Rdeu012(siif=connect_siif)
                for mes in meses:
                    partial_schema = await self.siif_rdeu012_handler.download_and_sync_validated_to_repository(
                        mes=str(mes)
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
        self, params: ControlHaberesParams
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control Obras
            partial_schema = await self.compute_control_obras(params=params)
            return_schema.extend(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control de Obras",
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
        control_haberes_docs = await self.control_haberes_repo.find_by_filter(
            {"ejercicio": {"$in": ultimos_ejercicios}}
        )

        if not control_haberes_docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        comprobantes_haberes = pd.DataFrame()
        banco_invico = pd.DataFrame()
        for ejercicio in ultimos_ejercicios:
            df = await self.generate_comprobantes_haberes_neto_rdeu(ejercicio=ejercicio)
            comprobantes_haberes = pd.concat(
                [comprobantes_haberes, df], ignore_index=True
            )
            df = await self.get_banco_invico_unified_cta_cte(ejercicio=ejercicio)
            banco_invico = pd.concat([banco_invico, df], ignore_index=True)

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (pd.DataFrame(control_haberes_docs), "control_mensual"),
                (comprobantes_haberes, "siif_comprobantes_haberes_neto_rdeu"),
                (banco_invico, "banco_invico"),
            ],
            filename="control_haberes.xlsx",
            spreadsheet_key="1A9ypUkwm4kfLqUAwr6-55crcFElisOO9fOdI6iflMAc",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # --------------------------------------------------
    async def generate_comprobantes_haberes_neto_rdeu(
        self, ejercicio: int = None
    ) -> pd.DataFrame:
        try:
            comprobantes_haberes = await get_siif_comprobantes_haberes(
                ejercicio=ejercicio, neto_art=True, neto_gcias_310=True
            )
            rdeu_docs = await get_siif_rdeu012_unified_cta_cte()

            rdeu = pd.DataFrame(rdeu_docs)
            rdeu = rdeu.drop(
                columns=[
                    "mes_hasta",
                    "fecha_aprobado",
                    "fecha_desde",
                    "fecha_hasta",
                    "org_fin",
                ]
            )
            rdeu = rdeu.drop_duplicates(subset=["nro_comprobante", "mes"])
            semi_table = pd.merge(
                rdeu,
                comprobantes_haberes,
                how="inner",
                copy=False,
                on="nro_comprobante",
            )
            in_both = rdeu["nro_comprobante"].isin(semi_table["nro_comprobante"])
            rdeu = rdeu[in_both]
            rdeu = rdeu.drop_duplicates(subset=["nro_comprobante", "mes", "saldo"])
            rdeu["importe"] = rdeu["saldo"] * (-1)
            rdeu["clase_reg"] = "CYO"
            rdeu["clase_nor"] = "NOR"
            rdeu["clase_gto"] = "RDEU"
            rdeu["es_comprometido"] = True
            rdeu["es_verificado"] = True
            rdeu["es_aprobado"] = True
            rdeu["es_pagado"] = True
            rdeu = rdeu.drop(columns=["saldo"])
            comprobantes_haberes_neto_rdeu = pd.concat([comprobantes_haberes, rdeu])

            # Ajustamos la Deuda Flotante Pagada
            rdeu = pd.DataFrame(rdeu_docs)
            rdeu = rdeu.drop_duplicates(subset=["nro_comprobante"], keep="last")
            rdeu["fecha_hasta"] = rdeu["fecha_hasta"] + pd.tseries.offsets.DateOffset(
                months=1
            )
            rdeu["mes_hasta"] = rdeu["fecha_hasta"].dt.strftime("%m/%Y")
            rdeu["ejercicio"] = pd.to_numeric(rdeu["mes_hasta"].str[-4:])

            # Incorporamos los comprobantes de gastos pagados
            # en periodos posteriores (Deuda Flotante)
            if ejercicio is not None:
                if isinstance(ejercicio, list):
                    rdeu = rdeu.loc[rdeu["ejercicio"].isin(ejercicio)]
                else:
                    rdeu = rdeu.loc[rdeu["ejercicio"].isin([ejercicio])]
                    rdeu["fecha"] = rdeu["fecha_hasta"]
            rdeu["mes"] = rdeu["mes_hasta"]
            rdeu = rdeu.drop(
                columns=[
                    "mes_hasta",
                    "fecha_aprobado",
                    "fecha_desde",
                    "fecha_hasta",
                    "org_fin",
                ]
            )
            semi_table = pd.merge(
                rdeu,
                comprobantes_haberes,
                how="inner",
                copy=False,
                on="nro_comprobante",
            )
            in_both = rdeu["nro_comprobante"].isin(semi_table["nro_comprobante"])
            rdeu = rdeu[in_both]
            rdeu = rdeu.drop_duplicates(subset=["nro_comprobante", "mes", "saldo"])
            rdeu["importe"] = rdeu["saldo"]
            rdeu["clase_reg"] = "CYO"
            rdeu["clase_nor"] = "NOR"
            rdeu["clase_gto"] = "RDEU"
            rdeu["es_comprometido"] = True
            rdeu["es_verificado"] = True
            rdeu["es_aprobado"] = True
            rdeu["es_pagado"] = True
            rdeu = rdeu.drop(columns=["saldo"])

            if isinstance(ejercicio, list):
                rdeu = rdeu.loc[rdeu["ejercicio"].isin(ejercicio)]
            else:
                rdeu = rdeu.loc[rdeu["ejercicio"].isin([ejercicio])]
            df = pd.concat([comprobantes_haberes_neto_rdeu, rdeu])
            return df
        except Exception as e:
            logger.error(
                f"Error retrieving SIIF's Comprobantes Haberes Neto Deuda Flotante Data from database: {e}"
            )
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SIIF's Comprobantes Haberes Neto Deuda Flotante Data from the database",
            )

    # --------------------------------------------------
    async def compute_control_haberes(
        self,
        params: ControlHaberesParams,
    ) -> List[RouteReturnSchema]:
        return_schema = []
        groupby_cols: list = ["ejercicio", "mes"]
        try:
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                siif = await self.generate_comprobantes_haberes_neto_rdeu(ejercicio)
                siif = siif.loc[:, groupby_cols + ["importe"]]
                siif = siif.groupby(groupby_cols)["importe"].sum()
                siif = siif.reset_index()
                siif = siif.rename(columns={"importe": "ejecutado_siif"})
                # print(f"siif.shape: {siif.shape} - siif.head: {siif.head()}")
                sscc = await get_banco_invico_unified_cta_cte(ejercicio=ejercicio)
                sscc = sscc.loc[:, groupby_cols + ["importe"]]
                sscc = sscc.groupby(groupby_cols)["importe"].sum()
                sscc = sscc.reset_index()
                sscc = sscc.rename(columns={"importe": "pagado_sscc"})
                # print(f"sscc.shape: {sscc.shape} - sscc.head: {sscc.head()}")
                df = pd.merge(siif, sscc, how="outer", on=groupby_cols, copy=False)
                df[["ejecutado_siif", "pagado_sscc"]] = df[
                    ["ejecutado_siif", "pagado_sscc"]
                ].fillna(0)
                df["diferencia"] = df.ejecutado_siif - df.pagado_sscc
                df = df.sort_values(by=["ejercicio", "mes"])
                df = pd.DataFrame(df)
                df["dif_acum"] = df["diferencia"].cumsum()
                df.reset_index(drop=True, inplace=True)
                # print(f"df.shape: {df.shape} - df.head: {df.head()}")

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df, model=ControlHaberesReport, field_id="cuit"
                )

                partial_schema = await sync_validated_to_repository(
                    repository=self.control_haberes_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title=f"Control Haberes del ejercicio {ejercicio}",
                    logger=logger,
                    label=f"Control Haberes del ejercicio {ejercicio}",
                )
                return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control Obras",
            )
        except Exception as e:
            logger.error(f"Error in compute_obras: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in compute_obras",
            )
        finally:
            return return_schema

    # -------------------------------------------------
    async def get_control_haberes_from_db(
        self, params: BaseFilterParams
    ) -> List[ControlHaberesDocument]:
        return await self.control_haberes_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Control Haberes from the database",
        )

    # -------------------------------------------------
    async def export_control_haberes_from_db(
        self, upload_to_google_sheets: bool = True
    ) -> StreamingResponse:
        df = pd.DataFrame(await self.control_haberes_repo.get_all())

        return export_dataframe_as_excel_response(
            df,
            filename="control_haberes.xlsx",
            sheet_name="control_mensual",
            upload_to_google_sheets=upload_to_google_sheets,
            google_sheet_key="1A9ypUkwm4kfLqUAwr6-55crcFElisOO9fOdI6iflMAc",
        )


ControlHaberesServiceDependency = Annotated[ControlHaberesService, Depends()]
