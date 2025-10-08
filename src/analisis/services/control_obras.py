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
from datetime import datetime
from typing import Annotated, List

import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import logger
from ...icaro.handlers import IcaroMongoMigrator
from ...icaro.repositories import CargaRepositoryDependency
from ...sgf.schemas import Origen
from ...sgf.services import ResumenRendProvServiceDependency
from ...siif.handlers import (
    Rdeu012,
    login,
    logout,
)
from ...siif.repositories import Rdeu012RepositoryDependency
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
    get_resumen_rend_prov_with_desc,
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
    icaro_carga_repo: CargaRepositoryDependency
    sscc_banco_invico_service: BancoINVICOServiceDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency
    sgf_resumend_rend_prov_service: ResumenRendProvServiceDependency
    siif_rdeu012_repo: Rdeu012RepositoryDependency
    siif_rdeu012_handler: Rdeu012 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_control_obras_from_source(
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
                await self.siif_rdeu012_handler.go_to_reports()
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

                # 🔹Resumen Rendicion Proveedores
                partial_schema = await self.sgf_resumend_rend_prov_service.sync_resumen_rend_prov_from_sgf(
                    username=params.sgf_username,
                    password=params.sgf_password,
                    params=params,
                )
                return_schema.extend(partial_schema)

                # 🔹 Icaro
                path = os.path.join(get_r_icaro_path(), "ICARO.sqlite")
                migrator = IcaroMongoMigrator(sqlite_path=path)
                return_schema.append(await migrator.migrate_carga())
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
            partial_schema = await self.compute_control_obras(params=params)
            return_schema.append(partial_schema)

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
        # ejecucion_obras.reporte_planillometro_contabilidad (planillometro_contabilidad)

        control_obras_docs = await self.control_obras_repo.get_all()

        if not control_obras_docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (pd.DataFrame(control_obras_docs), "control_mes_cta_cte_cuit_db"),
                (
                    await self.generate_icaro_carga_neto_rdeu(),
                    "icaro_carga_neto_rdeu",
                ),
                (await self.generate_resumen_rend_cuit(), "resumen_rend_cuit"),
            ],
            filename="control_obras.xlsx",
            spreadsheet_key="16v2ovmQnS1v73-WxTOK6b9Tx9DRugGc70ufpjVi-rPA",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # # --------------------------------------------------
    # async def generate_icaro_obras_without_codigo(self) -> pd.DataFrame:
    #     docs = await self.icaro_obras_repo.safe_find_by_filter()
    #     if not docs:
    #         raise HTTPException(
    #             status_code=404,
    #             detail="No se encontraron registros en la colección siif_comprobantes_recursos",
    #         )
    #     df = pd.DataFrame(docs)
    #     # Supongamos que tienes un DataFrame df con una columna 'columna_con_numeros' que contiene los registros con la parte numérica al principio
    #     df["obra_sin_cod"] = df["desc_obra"].str.replace(r"^\d+-\d+", "", regex=True)
    #     df["obra_sin_cod"] = df["obra_sin_cod"].str.lstrip()
    #     df["imputacion"] = df["actividad"] + "-" + df["partida"]
    #     df = pd.concat(
    #         [
    #             df[["obra_sin_cod", "imputacion"]],
    #             df.drop(columns=["obra_sin_cod", "imputacion"]),
    #         ],
    #         axis=1,
    #     )
    #     return df

    # --------------------------------------------------
    async def generate_resumen_rend_cuit(self, ejercicio: int = None) -> pd.DataFrame:
        filters = {"origen": {"$ne": "FUNCIONAMIENTO"}}
        sgf = await get_resumen_rend_prov_with_desc(
            ejercicio=ejercicio, filters=filters
        )
        cert_neg = await get_banco_invico_cert_neg(ejercicio=ejercicio)
        df = pd.concat([sgf, cert_neg], ignore_index=True)
        # Filtramos los registros de honorarios en EPAM
        df_epam = df.copy()
        keep = ["HONORARIOS"]
        df_epam = df_epam.loc[df_epam["origen"] == Origen.epam.value]
        df_epam = df_epam.loc[~df_epam.destino.str.contains("|".join(keep))]
        df = df.loc[df["origen"] != Origen.epam.value]
        df = pd.DataFrame(pd.concat([df, df_epam], ignore_index=True))
        return df

    # --------------------------------------------------
    async def generate_icaro_carga_neto_rdeu(
        self, ejercicio: int = None
    ) -> pd.DataFrame:
        try:
            icaro_docs = await self.icaro_carga_repo.get_all()
            rdeu_docs = await self.siif_rdeu012_repo.get_all()

            icaro = pd.DataFrame(icaro_docs)
            icaro = icaro.loc[~icaro["tipo"].isin(["PA6", "REG"])]
            rdeu = pd.DataFrame(rdeu_docs).loc[:, ["nro_comprobante", "saldo", "mes"]]
            rdeu = rdeu.drop_duplicates(subset=["nro_comprobante", "mes"])
            rdeu = pd.merge(rdeu, icaro, how="inner", copy=False)
            rdeu["importe"] = rdeu.saldo * (-1)
            rdeu["tipo"] = "RDEU"
            rdeu = rdeu.drop(columns=["saldo"])
            rdeu = pd.concat([rdeu, icaro], copy=False)
            icaro = pd.DataFrame(icaro_docs)
            icaro = icaro.loc[icaro["tipo"].isin(["PA6"])]
            rdeu = pd.concat([rdeu, icaro], copy=False)
            icaro_carga_neto_rdeu = rdeu

            # Ajustamos la Deuda Flotante Pagada
            rdeu = pd.DataFrame(rdeu_docs)
            rdeu = rdeu.drop_duplicates(subset=["nro_comprobante"], keep="last")
            rdeu["fecha_hasta"] = rdeu["fecha_hasta"] + pd.tseries.offsets.DateOffset(
                months=1
            )
            rdeu["mes_hasta"] = rdeu["fecha_hasta"].dt.strftime("%m/%Y")
            rdeu["ejercicio"] = rdeu["mes_hasta"].str[-4:]

            # Incorporamos los comprobantes de gastos pagados
            # en periodos posteriores (Deuda Flotante)
            if ejercicio is not None:
                if isinstance(ejercicio, list):
                    rdeu = rdeu.loc[rdeu["ejercicio"].isin(ejercicio)]
                else:
                    rdeu = rdeu.loc[rdeu["ejercicio"].isin([ejercicio])]
            icaro = pd.DataFrame(icaro_docs)
            icaro = icaro.loc[~icaro["tipo"].isin(["PA6", "REG"])]
            icaro = icaro.loc[
                :,
                [
                    "nro_comprobante",
                    "actividad",
                    "partida",
                    "fondo_reparo",
                    "certificado",
                    "avance",
                    "origen",
                    "obra",
                ],
            ]
            rdeu = pd.merge(rdeu, icaro, on="nro_comprobante", copy=False)
            rdeu["importe"] = rdeu.saldo
            rdeu["tipo"] = "RDEU"
            rdeu["id"] = rdeu["nro_comprobante"] + "C"
            rdeu = rdeu.loc[~rdeu["actividad"].isna()]
            rdeu = rdeu.drop(columns=["fecha", "mes"])
            rdeu = rdeu.rename(columns={"fecha_hasta": "fecha", "mes_hasta": "mes"})
            rdeu = rdeu.loc[
                :,
                [
                    "ejercicio",
                    "nro_comprobante",
                    "fuente",
                    "cuit",
                    "cta_cte",
                    "tipo",
                    "importe",
                    "id",
                    "actividad",
                    "partida",
                    "fondo_reparo",
                    "certificado",
                    "avance",
                    "origen",
                    "obra",
                    "fecha",
                    "mes",
                ],
            ]
            df = pd.concat([rdeu, icaro_carga_neto_rdeu], copy=False)
            if ejercicio is not None:
                if isinstance(ejercicio, list):
                    df = df.loc[df["ejercicio"].isin(ejercicio)]
                else:
                    df = df.loc[df["ejercicio"].isin([ejercicio])]
            return df
        except Exception as e:
            logger.error(
                f"Error retrieving Icaro's Carga Neto Deuda Flotante Data from database: {e}"
            )
            raise HTTPException(
                status_code=500,
                detail="Error retrieving Icaro's Carga Neto Deuda Flotante Data from the database",
            )

    # --------------------------------------------------
    async def compute_control_obras(
        self,
        params: ControlObrasParams,
        groupby_cols: list = ["ejercicio", "mes", "cta_cte", "cuit"],
    ) -> RouteReturnSchema:
        return_schema = RouteReturnSchema()
        try:
            icaro = self.generate_icaro_carga_neto_rdeu(params.ejercicio)
            icaro = icaro.loc[:, groupby_cols + ["importe"]]
            icaro = icaro.groupby(groupby_cols)["importe"].sum()
            icaro = icaro.reset_index()
            icaro = icaro.rename(columns={"importe": "ejecutado_icaro"})
            sgf = self.generate_resumen_rend_cuit(ejercicio=params.ejercicio)
            sgf = sgf.loc[:, groupby_cols + ["importe_bruto"]]
            sgf = sgf.groupby(groupby_cols)["importe_bruto"].sum()
            sgf = sgf.reset_index()
            sgf = sgf.rename(columns={"importe_bruto": "bruto_sgf"})
            df = pd.merge(icaro, sgf, how="outer", on=groupby_cols, copy=False)
            df[["ejecutado_icaro", "bruto_sgf"]] = df[
                ["ejecutado_icaro", "bruto_sgf"]
            ].fillna(0)
            df["diferencia"] = df.ejecutado_icaro - df.bruto_sgf
            df = pd.DataFrame(df)
            df.reset_index(drop=True, inplace=True)

            # 🔹 Validar datos usando Pydantic
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df, model=ControlObrasReport, field_id="cuit"
            )

            return_schema = await sync_validated_to_repository(
                repository=self.control_obras_repo,
                validation=validate_and_errors,
                delete_filter=None,
                title="Control Obras",
                logger=logger,
                label=f"Control Obras del ejercicio {params.ejercicio}",
            )
            # return_schema.append(partial_schema)

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
    async def get_control_obras_from_db(
        self, params: BaseFilterParams
    ) -> List[ControlObrasDocument]:
        return await self.control_obras_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Control Obras from the database",
        )

    # -------------------------------------------------
    async def export_control_obras_from_db(
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


ControlObrasServiceDependency = Annotated[ControlObrasService, Depends()]
