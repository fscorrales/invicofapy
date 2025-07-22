#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: SIIF Recursos vs SSCC depósitos
Data required:
    - Icaro
    - SIIF rdeu012
    - SGF Resumen de Rendiciones por Proveedor
    - SGF Listado Proveedores (POR LE MONENTO USO PROVEEDORES DE ICARO)
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/16v2ovmQnS1v73-WxTOK6b9Tx9DRugGc70ufpjVi-rPA
"""

__all__ = ["ControlObrasService", "ControlObrasServiceDependency"]

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
from ...icaro.repositories import ObrasRepositoryDependency
from ...sgf.services import ResumenRendProvServiceDependency
from ...siif.handlers import (
    Rci02,
    login,
    logout,
)
from ...sscc.services import BancoINVICOServiceDependency, CtasCtesServiceDependency
from ...utils import (
    BaseFilterParams,
    RouteReturnSchema,
    export_dataframe_as_excel_response,
    export_multiple_dataframes_to_excel,
    get_r_icaro_path,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..handlers import (
    get_banco_invico_cert_neg,
    get_resumen_rend_prov_unified_cta_cte,
)
from ..repositories.control_obras import ControlObrasRepositoryDependency
from ..schemas.control_obras import (
    ControlObrasDocument,
    ControlObrasParams,
    ControlObrasReport,
    ControlObrasSyncParams,
)


# --------------------------------------------------
@dataclass
class ControlObrasService:
    control_obras_repo: ControlObrasRepositoryDependency
    icaro_obras_repo: ObrasRepositoryDependency
    sscc_banco_invico_service: BancoINVICOServiceDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency
    sgf_resumend_rend_prov_service: ResumenRendProvServiceDependency
    siif_rf602_handler: Rci02 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_obras_from_source(
        self,
        params: ControlObrasSyncParams = None,
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
                # 🔹Rci02
                self.siif_rci02_handler = Rci02(siif=connect_siif)
                partial_schema = await self.siif_rci02_handler.download_and_sync_validated_to_repository(
                    ejercicio=int(params.ejercicio)
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
                return_schema.append(partial_schema)

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
                return_schema.append(partial_schema)

                # 🔹 Icaro
                path = os.path.join(get_r_icaro_path(), "ICARO.sqlite")
                migrator = IcaroMongoMigrator(sqlite_path=path)
                return_schema.append(await migrator.migrate_obras())
                return_schema.append(
                    await migrator.migrate_proveedores()
                )  # NO ES LO CORRECTO, debería usar Listado Proveedores SGF

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
    async def compute_all(self, params: ControlObrasParams) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control Recursos
            partial_schema = await self.compute_control_recursos(params=params)
            return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control de Recursos",
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
        # ejecucion_obras.reporte_planillometro_contabilidad (planillometro_contabilidad)

        control_obras_docs = await self.control_obras_repo.get_all()

        if not control_obras_docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (pd.DataFrame(control_obras_docs), "control_mes_cta_cte_cuit_db"),
                (
                    await self.generate_siif_comprobantes_recursos(),
                    "siif_recursos",
                ),
                (await self.generate_banco_invico(), "banco_ingresos"),
            ],
            filename="control_obras.xlsx",
            spreadsheet_key="16v2ovmQnS1v73-WxTOK6b9Tx9DRugGc70ufpjVi-rPA",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # --------------------------------------------------
    async def generate_icaro_obras_without_codigo(self) -> pd.DataFrame:
        docs = await self.icaro_obras_repo.safe_find_by_filter()
        if not docs:
            raise HTTPException(
                status_code=404,
                detail="No se encontraron registros en la colección siif_comprobantes_recursos",
            )
        df = pd.DataFrame(docs)
        # Supongamos que tienes un DataFrame df con una columna 'columna_con_numeros' que contiene los registros con la parte numérica al principio
        df["obra_sin_cod"] = df["desc_obra"].str.replace(r"^\d+-\d+", "", regex=True)
        df["obra_sin_cod"] = df["obra_sin_cod"].str.lstrip()
        df["imputacion"] = df["actividad"] + "-" + df["partida"]
        df = pd.concat(
            [
                df[["obra_sin_cod", "imputacion"]],
                df.drop(columns=["obra_sin_cod", "imputacion"]),
            ],
            axis=1,
        )
        return df

    # --------------------------------------------------
    async def generate_resumen_rend_cuit(self, ejercicio: int = None) -> pd.DataFrame:
        filters = {"origen": {"$neq": "FUNCIONAMIENTO"}}
        sgf = await get_resumen_rend_prov_unified_cta_cte(
            ejercicio=ejercicio, filters=filters
        )
        cert_neg = await get_banco_invico_cert_neg(ejercicio=ejercicio)
        df = pd.concat([sgf, cert_neg], ignore_index=True)
        return df

    # --------------------------------------------------
    async def compute_control_recursos(
        self, params: ControlObrasParams
    ) -> RouteReturnSchema:
        return_schema = RouteReturnSchema()
        try:
            group_by = ["ejercicio", "mes", "cta_cte", "grupo"]
            siif = self.generate_siif_comprobantes_recursos(
                ejercicio=int(params.ejercicio)
            )
            siif = siif.loc[not siif["es_invico"]]
            siif = siif.loc[not siif["es_remanente"]]
            siif = siif.groupby(group_by)["importe"].sum()
            siif = siif.reset_index(drop=False)
            siif = siif.rename(columns={"importe": "recursos_siif"})
            sscc = self.generate_banco_invico(ejercicio=int(params.ejercicio))
            sscc = sscc.groupby(group_by)["importe"].sum()
            sscc = sscc.reset_index(drop=False)
            sscc = sscc.rename(columns={"importe": "depositos_banco"})
            df = pd.merge(siif, sscc, how="outer")
            # 🔹 Validar datos usando Pydantic
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df, model=ControlObrasReport, field_id="cta_cte"
            )

            return_schema = await sync_validated_to_repository(
                repository=self.control_obras_repo,
                validation=validate_and_errors,
                delete_filter=None,
                title="Reporte de Ejecución Presupuestaria SIIF con Descripción",
                logger=logger,
                label=f"Reporte de Ejecución Presupuestaria SIIF con Descripción hasta el ejercicio {params.ejercicio}",
            )
            # return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control de Recursos",
            )
        except Exception as e:
            logger.error(f"Error in compute_control_recursos: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in compute_control_recursos",
            )
        finally:
            return return_schema

    # -------------------------------------------------
    async def get_control_recursos_from_db(
        self, params: BaseFilterParams
    ) -> List[ControlObrasDocument]:
        return await self.control_obras_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Reporte de Ejecución Presupuestaria SIIF con Descripción from the database",
        )

    # -------------------------------------------------
    async def export_control_recursos_from_db(
        self, upload_to_google_sheets: bool = True
    ) -> StreamingResponse:
        df = pd.DataFrame(await self.control_obras_repo.get_all())

        return export_dataframe_as_excel_response(
            df,
            filename="control_obras.xlsx",
            sheet_name="control_mes_cta_cte_cuit_db",
            upload_to_google_sheets=upload_to_google_sheets,
            google_sheet_key="16v2ovmQnS1v73-WxTOK6b9Tx9DRugGc70ufpjVi-rPA",
        )

        # Control Obras por Ejercicio, Mes, Cta. Cte. y CUIT
        # self.df = control_obras.control_cruzado(
        #     groupby_cols=["ejercicio", "mes", "cta_cte", "cuit"]
        # )
        # self.df = self.df.fillna("")
        # spreadsheet_key = "16v2ovmQnS1v73-WxTOK6b9Tx9DRugGc70ufpjVi-rPA"
        # wks_name = "control_mes_cta_cte_cuit_db"
        # self.gs.to_google_sheets(
        #     self.df, spreadsheet_key=spreadsheet_key, wks_name=wks_name
        # )
        # print("-- Control Obras por Ejercicio, Mes, Cta. Cte. y CUIT --")
        # print(self.df.head())

    # --------------------------------------------------
    def import_resumen_rend_cuit(self):
        df = super().import_resumen_rend_cuit(self.ejercicio, neto_cert_neg=True)
        df = df.loc[df["origen"] != "FUNCIONAMIENTO"]
        # Filtramos los registros de honorarios en EPAM
        df_epam = df.copy()
        keep = ["HONORARIOS"]
        df_epam = df_epam.loc[df_epam["origen"] == "EPAM"]
        df_epam = df_epam.loc[~df_epam.destino.str.contains("|".join(keep))]
        df = df.loc[df["origen"] != "EPAM"]
        df = pd.concat([df, df_epam], ignore_index=True)
        self.sgf_resumen_rend_cuit = pd.DataFrame(df)
        return self.sgf_resumen_rend_cuit

    # --------------------------------------------------
    def control_cruzado(
        self, groupby_cols: list = ["ejercicio", "mes", "cta_cte"]
    ) -> pd.DataFrame:
        icaro = self.import_icaro_carga_neto_rdeu(self.ejercicio).copy()
        icaro = icaro.loc[:, groupby_cols + ["importe"]]
        icaro = icaro.groupby(groupby_cols)["importe"].sum()
        icaro = icaro.reset_index()
        icaro = icaro.rename(columns={"importe": "ejecutado_icaro"})
        # icaro = icaro >> \
        #     dplyr.select(f.mes, f.cta_cte, f.importe) >> \
        #     dplyr.group_by(f.mes, f.cta_cte) >> \
        #     dplyr.summarise(ejecutado_icaro = base.sum_(f.importe),
        #                     _groups = 'drop')
        sgf = self.sgf_resumen_rend_cuit.copy()
        sgf = sgf.loc[:, groupby_cols + ["importe_bruto"]]
        sgf = sgf.groupby(groupby_cols)["importe_bruto"].sum()
        sgf = sgf.reset_index()
        sgf = sgf.rename(columns={"importe_bruto": "bruto_sgf"})
        # sgf = sgf >> \
        #     dplyr.select(
        #         f.mes, f.cta_cte,
        #         f.importe_bruto
        #     ) >> \
        #     dplyr.group_by(f.mes, f.cta_cte) >> \
        #     dplyr.summarise(
        #         bruto_sgf = base.sum_(f.importe_bruto),
        #         _groups = 'drop')
        df = pd.merge(icaro, sgf, how="outer", on=groupby_cols, copy=False)
        df[["ejecutado_icaro", "bruto_sgf"]] = df[
            ["ejecutado_icaro", "bruto_sgf"]
        ].fillna(0)
        df["diferencia"] = df.ejecutado_icaro - df.bruto_sgf
        # df = icaro >> \
        #     dplyr.full_join(sgf) >> \
        #     dplyr.mutate(
        #         dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
        #     ) >> \
        #     dplyr.mutate(
        #         diferencia = f.ejecutado_icaro - f.bruto_sgf
        #     )
        #     dplyr.filter_(~dplyr.near(f.diferencia, 0))
        # df.sort_values(by=['mes', 'cta_cte'], inplace= True)
        # df['dif_acum'] = df['diferencia'].cumsum()
        df = pd.DataFrame(df)
        df.reset_index(drop=True, inplace=True)
        return df


ControlObrasServiceDependency = Annotated[ControlObrasService, Depends()]
