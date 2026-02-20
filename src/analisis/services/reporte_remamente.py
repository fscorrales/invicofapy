#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Reporte Remamente
Data required:
    - SIIF rf602
    - SIIF rf610
    - SIIF rci02
    - SIIF rdeu012
    - SIIF rdeu012bc_c (Pedir a Tesorería General de la Provincia)
    - SSCC ctas_ctes (manual data)
    - Saldos por cuenta Banco INVICO (SSCC) al 31/12 de cada año (SSCC-Cuentas-Resumen Gral de Saldos)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1hLbpzEXFp3hcGEbRolQTIj8_HSQ0vwWPB3XuQVR7NXs (Ejecución Obras)

"""

__all__ = [
    "ReporteRemanenteService",
    "ReporteRemanenteServiceDependency",
]

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
from ...siif.handlers import (
    Rci02,
    Rdeu012,
    Rf602,
    Rf610,
    login,
    logout,
)
from ...sscc.services import CtasCtesServiceDependency
from ...utils import (
    GoogleExportResponse,
    RouteReturnSchema,
    export_multiple_dataframes_to_excel,
    upload_multiple_dataframes_to_google_sheets,
)
from ..handlers import get_siif_desc_pres, get_siif_rf602
from ..repositories.reporte_modulos_basicos import (
    ReporteModulosBasicosIcaroRepositoryDependency,
)
from ..schemas.reporte_remanente import (
    ReporteRemanenteParams,
    ReporteRemanenteSyncParams,
)


# --------------------------------------------------
@dataclass
class ReporteRemanenteService:
    reporte_mod_bas_icaro_repo: ReporteModulosBasicosIcaroRepositoryDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency
    siif_rf610_handler: Rf610 = field(init=False)  # No se pasa como argumento
    siif_rf602_handler: Rf602 = field(init=False)  # No se pasa como argumento
    siif_rdeu012_handler: Rdeu012 = field(init=False)  # No se pasa como argumento
    siif_rci02_handler: Rci02 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_remanente_from_source(
        self,
        params: ReporteRemanenteSyncParams = None,
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

                # 🔹 RF610
                self.siif_rf610_handler = Rf610(siif=connect_siif)
                await self.siif_rf610_handler.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rf610_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio)
                    )
                    return_schema.append(partial_schema)

                # 🔹 RF602
                self.siif_rf602_handler = Rf602(siif=connect_siif)
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rf602_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio)
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

                # 🔹Rci02
                self.siif_rci02_handler = Rci02(siif=connect_siif)
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rci02_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio),
                    )
                    return_schema.append(partial_schema)

                # 🔹Ctas Ctes
                partial_schema = (
                    await self.sscc_ctas_ctes_service.sync_ctas_ctes_from_excel(
                        excel_path=params.ctas_ctes_excel_path,
                    )
                )
                return_schema.append(partial_schema)

                # 🔹 Icaro
                # path = os.path.join(get_r_icaro_path(), "ICARO.sqlite")
                # migrator = IcaroMongoMigrator(sqlite_path=path)
                # return_schema.append(await migrator.migrate_carga())
                # return_schema.append(await migrator.migrate_estructuras())
                # return_schema.append(await migrator.migrate_proveedores())
                # return_schema.append(await migrator.migrate_obras())

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
    async def generate_all(
        self, params: ReporteRemanenteParams = None
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Reporte Planillometro
            partial_schema = await self.generate_reporte_ejecucion_obras_icaro(
                params=params
            )
            return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Reporte Remanente generation",
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
        params: ReporteRemanenteParams = None,
    ) -> list[tuple[pd.DataFrame, str]]:
        ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))

        # control_banco_docs = await self.control_banco_repo.find_by_filter(
        #     filters={"ejercicio": {"$in": ejercicios}},
        # )

        # if not control_banco_docs:
        #     raise HTTPException(status_code=404, detail="No se encontraron registros")

        hoja_trabajo = pd.DataFrame()

        for ejercicio in ejercicios:
            hoja_trabajo = pd.concat(
                [
                    hoja_trabajo,
                    await self.generate_hoja_trabajo(ejercicio=ejercicio),
                ],
                ignore_index=True,
            )

        return [
            # (pd.DataFrame(control_banco_docs), "siif_vs_sscc_db"),
            # (sscc, "sscc_db"),
            # (planillometro, "bd_planillometro"),
            (hoja_trabajo, "hoja_trabajo"),
        ]

    # -------------------------------------------------
    async def export_all_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ReporteRemanenteParams = None,
    ) -> StreamingResponse:
        df_sheet_pairs = await self._build_dataframes_to_export(params)

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=df_sheet_pairs,
            filename="remanente.xlsx",
            spreadsheet_key="1hLbpzEXFp3hcGEbRolQTIj8_HSQ0vwWPB3XuQVR7NXs",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # -------------------------------------------------
    async def export_all_from_db_to_google(
        self,
        params: ReporteRemanenteParams = None,
    ) -> GoogleExportResponse:
        df_sheet_pairs = await self._build_dataframes_to_export(params)

        return upload_multiple_dataframes_to_google_sheets(
            df_sheet_pairs=df_sheet_pairs,
            spreadsheet_key="1hLbpzEXFp3hcGEbRolQTIj8_HSQ0vwWPB3XuQVR7NXs",
            title="Remanente",
        )

    # --------------------------------------------------
    # def import_sdo_final_banco_invico(self) -> pd.DataFrame:
    #     df = self.import_df.import_sdo_final_banco_invico(ejercicio=self.ejercicio)
    #     return df

    # --------------------------------------------------
    async def generate_hoja_trabajo(self, ejercicio: int) -> pd.DataFrame:
        df = await get_siif_rf602(
            ejercicio=ejercicio,
            filters={
                "$and": [
                    {"partida": {"$in": ["421", "422"]}},
                    {"fuente": {"$ne": "11"}},
                ]
            },
        )

        if df.empty:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        # df = df.loc[df["partida"].isin(["421", "422"])]
        # df = df.loc[df["fuente"] != "11"]
        df["estructura"] = df["estructura"].str[:-4]
        df_desc = await get_siif_desc_pres(ejercicio_to=ejercicio)
        df_desc = df_desc.drop(["estructura"], axis=1)
        df_desc = df_desc.rename(
            columns={
                "actividad": "estructura",
                "desc_programa": "prog_con_desc",
                "desc_subprograma": "subprog_con_desc",
                "desc_proyecto": "proy_con_desc",
                "desc_actividad": "act_con_desc",
            },
        )
        df = df.merge(df_desc, how="left", on="estructura", copy=False)
        df = df.drop(
            [
                "grupo",
                "credito_original",
                "comprometido",
                "pendiente",
                "programa",
                "subprograma",
                "proyecto",
                "actividad",
            ],
            axis=1,
        )
        df["remte"] = 0
        df["mal_remte"] = 0
        df["saldo_remte"] = df["saldo"]
        df["cc_remte"] = None
        df["cc_mal_remte"] = None
        df["f_y_f"] = None
        return df

    def hoja_trabajo_proyectos(self, hoja_trabajo_df: pd.DataFrame) -> pd.DataFrame:
        df = hoja_trabajo_df.copy()
        df["estructura"] = df["estructura"].str[0:8]
        df = df.drop(
            columns=[
                "partida",
                "act_con_desc",
                "cc_remte",
                "cc_mal_remte",
                "f_y_f",
            ]
        )
        groupby_fields = [
            "estructura",
            "fuente",
            "org",
            "prog_con_desc",
            "subprog_con_desc",
            "proy_con_desc",
        ]
        df = df.groupby(groupby_fields).sum(numeric_only=True).reset_index()
        return df

    # --------------------------------------------------
    def deuda_flotante(self) -> pd.DataFrame:
        rdeu = self.import_df.import_siif_rdeu012(ejercicio=self.ejercicio)
        months = rdeu["mes_hasta"].tolist()
        # Convertir cada elemento de la lista a un objeto datetime
        dates = [dt.datetime.strptime(month, "%m/%Y") for month in months]
        # Obtener la fecha más reciente y Convertir la fecha mayor a un string en el formato 'MM/YYYY'
        gt_month = max(dates).strftime("%m/%Y")

        rdeu = rdeu.loc[rdeu["mes_hasta"] == gt_month, :]
        return rdeu

    # --------------------------------------------------
    def deuda_flotante_tg(self) -> pd.DataFrame:
        return self.import_df.import_siif_rdeu012b2_c(mes_hasta="12/" + self.ejercicio)

    # --------------------------------------------------
    def remanente_met_1(self):
        """
        El cálculo de Remanente por método I consiste en restarle al saldo real de Banco SSCC
        (Resumen Gral de Saldos), al cierre del ejercicio, la deuda flotante (reporte SIIF
        rdeu012) del SIIF al 31/12 del mismo ejercicio. Tener en cuenta las siguiente
        consideraciones:
        - Las fuentes 10 y 12 del SIIF son ejecutadas con las cuentas corrientes 130832-03
        y 130832-06
        - La fuente 13 del SIIF es ejecutada con la cuenta corriente 130832-16 a lo que hay
        que adicionarle 4.262.062,77 que quedó de saldo transferido por la UCAPFI en 09/2019
        en la cuenta 22101105-48 del programa EPAM Habitat.
        - El resto del Saldo Banco SSCC corresponde a la fuente 11
        - Tener en cuenta los ajustes contables que buscan regularizar comprobantes del SIIF
        que quedaron en la Deuda Flotante y no deben formar parte de la misma.
        """
        # SALDO_UCAPFI = 4262062.77 #Saldo Real
        SALDO_UCAPFI = 4262059.73  # Saldo ajustado
        banco_sscc = self.import_sdo_final_banco_invico()
        rdeu = self.deuda_flotante()
        rem_met_1 = {
            "Fuente 10": {
                "saldo_bco": banco_sscc.loc[banco_sscc["cta_cte"].isin(["130832-03"])][
                    "saldo"
                ].sum(),
                "rdeu": rdeu.loc[rdeu.fuente.isin(["10", "12"])]["saldo"].sum(),
            },
            "Fuente 13": {
                "saldo_bco": banco_sscc.loc[banco_sscc["cta_cte"] == "130832-16"][
                    "saldo"
                ].sum()
                + SALDO_UCAPFI,
                "rdeu": rdeu.loc[rdeu.fuente.isin(["13"])]["saldo"].sum(),
            },
            "Fuente 11": {
                "saldo_bco": banco_sscc.loc[
                    ~banco_sscc["cta_cte"].isin(["130832-03", "130832-16"])
                ]["saldo"].sum()
                - SALDO_UCAPFI,
                "rdeu": rdeu.loc[rdeu.fuente.isin(["11"])]["saldo"].sum(),
            },
        }
        rem_met_1 = pd.DataFrame.from_dict(rem_met_1, orient="index")
        rem_met_1.reset_index(inplace=True)
        rem_met_1.columns = ["fuente", "saldo_bco", "rdeu"]
        rem_met_1["rte_met_1"] = rem_met_1.saldo_bco - rem_met_1.rdeu
        return rem_met_1

    # --------------------------------------------------
    def remanente_met_2(self):
        """
        El cálculo de Remanente por método II consiste en restarle a los Recursos SIIF
        (reporte SIIF rci02), ingresados en el ejercicio bajo análisis, los Gastos SIIF
        (reporte SIIF rf602) de dicho ejercicio. Tener en cuenta los ajustes contables
        que buscan regularizar comprobantes del SIIF que quedaron en la Deuda Flotante
        y no deben formar parte de la misma.
        """
        recursos = self.import_df.import_siif_rci02(ejercicio=self.ejercicio)
        gastos = self.import_df.import_siif_rf602(ejercicio=self.ejercicio)
        rem_met_2 = recursos.importe.groupby([recursos.fuente]).sum()
        rem_met_2 = pd.concat(
            [
                rem_met_2,
                gastos.ordenado.groupby([gastos.fuente]).sum(),
                gastos.saldo.groupby([gastos.fuente]).sum(),
            ],
            axis=1,
        )
        rem_met_2.columns = ["recursos", "gastos", "saldo_pres"]
        rem_met_2["rte_met_2"] = rem_met_2.recursos - rem_met_2.gastos
        rem_met_2.reset_index(inplace=True)
        # rem_met_2 = rem_met_2[~rem_met_2.index.isin(['11'], level=1)]
        # rem_met_2.dropna(inplace=True)
        return rem_met_2

    # --------------------------------------------------
    def remanente_met_2_hist(self):
        recursos = self.import_df.import_siif_rci02()
        gastos = self.import_df.import_siif_rf602()
        rem_solicitado = (
            recursos.loc[recursos.es_remanente == True]
            .importe.groupby([recursos.ejercicio, recursos.fuente])
            .sum()
            .to_frame()
        )
        rem_solicitado.reset_index(inplace=True)
        rem_solicitado["ejercicio"] = (
            rem_solicitado["ejercicio"].astype(int) - 1
        ).astype(str)
        rem_met_2_hist = recursos.importe.groupby(
            [recursos.ejercicio, recursos.fuente]
        ).sum()
        rem_met_2_hist = pd.concat(
            [
                rem_met_2_hist,
                gastos.ordenado.groupby([gastos.ejercicio, gastos.fuente]).sum(),
                gastos.saldo.groupby([gastos.ejercicio, gastos.fuente]).sum(),
            ],
            axis=1,
        )
        rem_met_2_hist.reset_index(inplace=True)
        rem_met_2_hist = rem_met_2_hist.merge(
            rem_solicitado, how="left", on=["ejercicio", "fuente"], copy=False
        )
        rem_met_2_hist.columns = [
            "ejercicio",
            "fuente",
            "recursos",
            "gastos",
            "saldo_pres",
            "rte_solicitado",
        ]
        rem_met_2_hist["rte_met_2"] = rem_met_2_hist.recursos - rem_met_2_hist.gastos
        # rem_met_2_hist["mal_rte_met_2"] = rem_met_2_hist.saldo_pres- rem_met_2_hist.rte_met_2
        rem_met_2_hist["dif_rte_solicitado"] = (
            rem_met_2_hist.rte_solicitado - rem_met_2_hist.rte_met_2
        )
        rem_met_2_hist = rem_met_2_hist[~rem_met_2_hist.fuente.isin(["11"])]
        # No sé qué pasó en Fuente 13 en el 2013, por eso lo filtro
        # rem_met_2_hist = rem_met_2_hist[~rem_met_2_hist.index.isin(['2011', '2012', '2013'], level=0)]
        # rem_met_2_hist.reset_index(inplace=True)
        rem_met_2_hist.dropna(inplace=True)
        return rem_met_2_hist

    # --------------------------------------------------
    def remanente_dif_met(self):
        rem_met_1 = self.remanente_met_1()
        rem_met_1 = rem_met_1.loc[:, ["fuente", "rte_met_1"]]
        rem_met_2 = self.remanente_met_2()
        rem_met_2 = rem_met_2.loc[:, ["fuente", "rte_met_2"]]
        # rem_met_2 = rem_met_2.loc[~rem_met_2['fuente'].isin(['10', '12'])]
        rem_met_2 = pd.DataFrame(
            [
                [
                    "Fuente 10 y 12",
                    rem_met_2.loc[rem_met_2["fuente"].isin(["10", "12"])][
                        "rte_met_2"
                    ].sum(),
                ],
                [
                    "Fuente 11",
                    rem_met_2.loc[rem_met_2["fuente"] == "11"]["rte_met_2"].sum(),
                ],
                [
                    "Fuente 13",
                    rem_met_2.loc[rem_met_2["fuente"] == "13"]["rte_met_2"].sum(),
                ],
            ],
            columns=["fuente", "rte_met_2"],
        )
        rem_met = rem_met_1.merge(rem_met_2, how="left", on="fuente")
        rem_met["dif_metodos"] = rem_met.rte_met_1 - rem_met.rte_met_2
        return rem_met


ReporteRemanenteServiceDependency = Annotated[ReporteRemanenteService, Depends()]
