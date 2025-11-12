#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control Banco SIIF vs SSCC (Banco Real)
Data required:
    - SIIF rvicon03
    - SIIF rcocc31
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1CRQjzIVzHKqsZE8_E1t8aRQDfWfZALhbe64WcxHiSM4
"""

__all__ = ["ControlBancoService", "ControlBancoServiceDependency"]

from dataclasses import dataclass, field
from enum import Enum
from typing import Annotated, List

import numpy as np
import pandas as pd
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import logger
from ...siif.handlers import (
    Rcocc31,
    Rvicon03,
    login,
    logout,
)
from ...sscc.repositories import CtasCtesRepository
from ...sscc.services import BancoINVICOServiceDependency, CtasCtesServiceDependency
from ...utils import (
    RouteReturnSchema,
    export_multiple_dataframes_to_excel,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..handlers import (
    get_banco_invico_unified_cta_cte,
    get_siif_comprobantes_honorarios,
    get_siif_rcg01_uejp,
    get_siif_rcocc31,
    get_siif_rvicon03,
)
from ..repositories.control_banco import ControlBancoRepositoryDependency
from ..schemas.control_banco import (
    ControlBancoParams,
    ControlBancoReport,
    ControlBancoSyncParams,
)


# -------------------------------------------------
class Categoria(str, Enum):
    sin_categoria = "NO Categorizado"
    fonavi = "Ingreso FO.NA.VI."
    recuperos = "Cobranza de Cuotas de Viviendas"
    aporte_empresario = "Ingreso 3% Aporte Empresario"
    fondos_provinciales = "Ingreso Fondos Provinciales"
    factureros_funcionamiento = "Pago Honorarios y Comisiones (Funcionamiento)"
    factureros_epam = "Pago Honorarios y Comisiones (EPAM)"
    factureros_mutual_funcionamiento = (
        "Pago Mutual de Honorarios y Comisiones (Funcionamiento)"
    )
    factureros_embargo_funcionamiento = "Pago Embargo sobre Honorarios (Funcionamiento)"
    factureros_seguro_funcionamiento = (
        "Pago Seguro de Honorarios (Funcionamiento y EPAM)"
    )
    contratistas = "Pago a Contratistas"
    proveedores = "Pago a Proveedores"
    retenciones = "Pago de Retenciones Contratistas y Proveedores"
    haberes = "Pago al Personal"
    escribanos = "Pagos Escribanos (FEI / PFE)"
    viaticos = "Pago Anticipo de Viáticos (PA3 / PAV)"
    viaticos_reversion = "Reversion de Viático (Rev)"
    viaticos_reembolso = "Reembolso de Viático en exceso (373)"


# --------------------------------------------------
@dataclass
class ControlBancoService:
    control_banco_repo: ControlBancoRepositoryDependency
    siif_rcocc31_handler: Rcocc31 = field(init=False)  # No se pasa como argumento
    siif_rvicon03_handler: Rvicon03 = field(init=False)  # No se pasa como argumento
    sscc_banco_invico_service: BancoINVICOServiceDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency

    # -------------------------------------------------
    async def sync_control_banco_from_source(
        self,
        params: ControlBancoSyncParams = None,
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
                # 🔹Rvicon03
                self.siif_rvicon03_handler = Rvicon03(siif=connect_siif)
                await self.siif_rvicon03_handler.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rvicon03_handler.download_and_sync_validated_to_repository(
                        ejercicio=ejercicio,
                    )
                    return_schema.append(partial_schema)

                # 🔹 Rcocc31
                self.siif_rcocc31_handler = Rcocc31(siif=connect_siif)
                cuentas_contables = await get_siif_rvicon03(ejercicio=params.ejercicio)
                cuentas_contables = cuentas_contables["cta_contable"].unique()
                logger.info(
                    f"Se Bajaran las siguientes cuentas contables: {cuentas_contables}"
                )
                for ejercicio in ejercicios:
                    for cta_contable in cuentas_contables:
                        partial_schema = await self.siif_rcocc31_handler.download_and_sync_validated_to_repository(
                            ejercicio=ejercicio,
                            cta_contable=cta_contable,
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
    async def compute_all(self, params: ControlBancoParams) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control banco
            partial_schema = await self.compute_control_banco(params=params)
            return_schema.extend(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control de Banco",
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
    async def generate_banco_siif(
        self,
        ejercicio: int,
        netear_pa6: bool = True,
        netear_aporte_empreario: bool = True,
    ) -> pd.DataFrame:
        df = await get_siif_rcocc31(ejercicio=ejercicio)

        if df.empty:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        # Solo incluimos los registros que tienen movimientos en la cuenta 1112-2-6
        df = df.loc[
            df["nro_entrada"].isin(
                df.loc[df["cta_contable"] == "1112-2-6"]["nro_entrada"].unique()
            )
        ]

        columns_to_flip_sign = ["debitos", "creditos", "saldo"]

        # Neteamos los PA6 pagados y ya regularizados
        if netear_pa6:
            gastos_df = await get_siif_rcg01_uejp(ejercicio=ejercicio)
            pa6_pagados = gastos_df["nro_fondo"].unique().tolist()
            pa6_pagados_df = df.loc[
                (df["tipo_comprobante"] == "PAP")
                & (df["nro_original"].isin(pa6_pagados))
            ].copy()
            pa6_pagados_df["tipo_comprobante"] = "FSC"
            pa6_pagados_df[columns_to_flip_sign] = pa6_pagados_df[
                columns_to_flip_sign
            ] * (-1)
            df = pd.concat([df, pa6_pagados_df])

        # Neteamos el Aporte Empresario tanto en ingresos como en gastos
        if netear_aporte_empreario:
            aporte_empresario_df = df.loc[df["cta_contable"] == "5123-1-1"].copy()
            aporte_empresario_df["tipo_comprobante"] = "FSC"
            aporte_empresario_df[columns_to_flip_sign] = aporte_empresario_df[
                columns_to_flip_sign
            ] * (-1)
            df = pd.concat([df, aporte_empresario_df])
            aporte_empresario_df = df.loc[
                (df["cta_contable"] == "2122-1-2") & (df["auxiliar_1"] == "337")
            ].copy()
            aporte_empresario_df["tipo_comprobante"] = "FSC"
            aporte_empresario_df[columns_to_flip_sign] = aporte_empresario_df[
                columns_to_flip_sign
            ] * (-1)
            df = pd.concat([df, aporte_empresario_df])

        # Agregamos la columna cta_cte desde auxiliar_1 de la cuenta 1112-2-6
        ctas_ctes_df = df.loc[
            df["cta_contable"] == "1112-2-6", ["nro_entrada", "auxiliar_1"]
        ].copy()
        ctas_ctes_df = ctas_ctes_df.drop_duplicates()
        ctas_ctes_df = ctas_ctes_df.rename(columns={"auxiliar_1": "cta_cte"})
        df = df.merge(ctas_ctes_df, on="nro_entrada", how="left")
        df = df.loc[df["cta_contable"] != "1112-2-6"]

        # Mapeamos las cuentas corrientes
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

        # Agregamos descripción a las cuentas contables
        ctas_contables_df = await get_siif_rvicon03(ejercicio=ejercicio)
        ctas_contables_df = ctas_contables_df.loc[
            :, ["cta_contable", "desc_cta_contable"]
        ]
        df = pd.merge(df, ctas_contables_df, how="left", on="cta_contable")

        # Agregamos columna para clasificar registros
        df["clase"] = Categoria.sin_categoria.value
        conditions = {
            "5172-4-4": Categoria.fonavi.value,
            "5172-2-1": Categoria.fondos_provinciales.value,
            "1122-1-1": Categoria.recuperos.value,
            "2111-1-1": Categoria.proveedores.value,
            "2111-1-2": Categoria.contratistas.value,
            "2113-2-9": Categoria.escribanos.value,
            "2122-1-2": Categoria.retenciones.value,
            "2113-1-13": Categoria.viaticos.value,
            "4112-1-3": Categoria.viaticos_reembolso.value,
            "1141-1-4": Categoria.viaticos_reversion.value,
        }
        df["clase"] = df["cta_contable"].map(conditions).fillna(df["clase"])

        ## Pago al personal (Haberes)
        df["clase"] = np.where(
            (df["cta_contable"] == "2121-1-1")  # Pago personal haberes
            | (
                (df["cta_contable"] == "2122-1-2")  # Pago retenciones haberes
                & (~df["auxiliar_1"].str.startswith("1"))
            )
            | (
                (df["cta_contable"] == "2111-1-3")  # Pago Movilidad y Comisión FONAVI
                & (df["cta_cte"] == "130832-04")
            ),
            Categoria.haberes.value,
            df["clase"],
        )

        ## Pago Embargos sobre Honorarios Funcionamiento
        df["clase"] = np.where(
            (df["cta_contable"] == "2122-1-2")
            & (df["auxiliar_1"] == "255")
            & (df["cta_cte"] == "130832-05"),
            Categoria.factureros_embargo_funcionamiento.value,
            df["clase"],
        )

        ## Para clasificar los pagos de Mutual de factureros funcionamiento
        df["clase"] = np.where(
            (df["cta_contable"] == "2122-1-2")
            & (df["auxiliar_1"] == "341")
            & (df["cta_cte"] == "130832-05"),
            Categoria.factureros_mutual_funcionamiento.value,
            df["clase"],
        )

        ## Para clasificar los pagos de Seguro de factureros funcionamiento y EPAM
        df["clase"] = np.where(
            (df["cta_contable"] == "2122-1-2")
            & (df["auxiliar_1"] == "413")
            & (df["cta_cte"] != "130832-04"),
            Categoria.factureros_seguro_funcionamiento.value,
            df["clase"],
        )

        ## Para clasificar los factureros
        siif_factureros = await get_siif_comprobantes_honorarios(ejercicio=ejercicio)
        siif_factureros["nro_comprobante"] = (
            siif_factureros["nro_comprobante"].str.lstrip("0").str[:-3]
        )
        siif_factureros_nro = (
            siif_factureros.loc[
                siif_factureros["cta_cte"] == "130832-05", "nro_comprobante"
            ]
            .unique()
            .tolist()
        )
        df["clase"] = np.where(
            (df["cta_contable"].isin(["2111-1-3", "2111-1-1"]))
            & (df["cta_cte"] == "130832-05")
            & (df["nro_original"].isin(siif_factureros_nro)),
            Categoria.factureros_funcionamiento.value,
            df["clase"],
        )
        siif_factureros_nro = (
            siif_factureros.loc[
                siif_factureros["cta_cte"] == "130832-07", "nro_comprobante"
            ]
            .unique()
            .tolist()
        )
        df["clase"] = np.where(
            (df["cta_contable"].isin(["2111-1-3", "2111-1-1"]))
            & (df["cta_cte"] == "130832-07")
            & (df["nro_original"].isin(siif_factureros_nro)),
            Categoria.factureros_epam.value,
            df["clase"],
        )

        # Ordenamos y seleccionamos columnas finales
        df["nro_entrada"] = pd.to_numeric(df["nro_entrada"], errors="coerce")
        df = df.sort_values(
            ["nro_entrada", "debitos", "creditos", "cta_contable"],
            ascending=[True, False, False, True],
        )
        df["nro_entrada"] = df["nro_entrada"].astype(str)
        df = df.loc[
            :,
            [
                "ejercicio",
                "mes",
                "fecha",
                "fecha_aprobado",
                "nro_entrada",
                "nro_original",
                "cta_contable",
                "tipo_comprobante",
                "debitos",
                "creditos",
                "saldo",
                "auxiliar_1",
                "auxiliar_2",
                "cta_cte",
                "desc_cta_contable",
                "clase",
            ],
        ]

        return df

    # --------------------------------------------------
    async def generate_banco_sscc(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        df = await get_banco_invico_unified_cta_cte(ejercicio=ejercicio)

        # Neteamos las transferencias internas
        df["cod_imputacion"] = np.where(
            df["cod_imputacion"].isin(["004", "034"]),
            "000",
            df["cod_imputacion"],
        )
        df["imputacion"] = np.where(
            df["cod_imputacion"] == "000",
            "TRANSFERENCIAS INTERNAS (NETAS)",
            df["imputacion"],
        )

        # Agregamos columna para clasificar registros
        df["clase"] = Categoria.sin_categoria.value
        conditions = {
            "001": Categoria.fonavi.value,
            "012": Categoria.fondos_provinciales.value,
            "002": Categoria.recuperos.value,
            "043": Categoria.factureros_funcionamiento.value,
            "021": Categoria.factureros_epam.value,
            "024": Categoria.haberes.value,
            "059": Categoria.haberes.value,  # Pago Mutual de la Movilidad
            "049": Categoria.factureros_embargo_funcionamiento.value,
            "036": Categoria.escribanos.value,
            "035": Categoria.retenciones.value,
            "029": Categoria.viaticos.value,
            "040": Categoria.viaticos_reembolso.value,
            "005": Categoria.viaticos_reversion.value,
        }
        df["clase"] = df["cod_imputacion"].map(conditions).fillna(df["clase"])

        ## Pago contratistas
        df["clase"] = np.where(
            (
                df["cod_imputacion"].isin(
                    ["065", "020", "041", "053", "217", "019", "066", "027", "162"]
                )
            )
            | (
                (df["cod_imputacion"] == "021")  # Pago Serv. y Mat. EPAM
                & (~df["concepto"].str.startswith("0175"))
            ),
            Categoria.contratistas.value,
            df["clase"],
        )

        ## Pago a Proveedores
        df["clase"] = np.where(
            (df["cod_imputacion"].isin(["023", "052", "031", "033", "037"]))
            | (
                (df["cod_imputacion"] == "032")  # Pago Renovación de Seguro
                & (~df["concepto"].str.startswith("SEGURO"))
            ),
            Categoria.proveedores.value,
            df["clase"],
        )

        ## Pago Mutual Factureros (Funcionamiento)
        df["clase"] = np.where(
            (df["clase"] == Categoria.factureros_funcionamiento.value)
            & (df["concepto"].str.startswith("MUTUAL")),
            Categoria.factureros_mutual_funcionamiento.value,
            df["clase"],
        )

        ## Pago Seguro Factureros (Funcionamiento y EPAM)
        df["clase"] = np.where(
            (df["cod_imputacion"] == "032") & (df["concepto"].str.startswith("SEGURO")),
            Categoria.factureros_seguro_funcionamiento.value,
            df["clase"],
        )

        # Ordenamos y seleccionamos columnas finales
        df = df.sort_values(
            ["fecha", "movimiento"],
            ascending=[True, True],
        )
        return df

    # --------------------------------------------------
    async def compute_control_banco(
        self,
        params: ControlBancoParams,
    ) -> List[RouteReturnSchema]:
        return_schema = []
        groupby_cols = ["ejercicio", "mes", "clase", "cta_cte"]
        try:
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                siif = await self.generate_banco_siif(ejercicio=ejercicio)
                siif["saldo"] = siif["saldo"] * (-1)
                siif = siif.groupby(groupby_cols)["saldo"].sum().reset_index()
                siif = siif.rename(columns={"saldo": "siif_importe"})
                sscc = await self.generate_banco_sscc(ejercicio=ejercicio)
                sscc = sscc.groupby(groupby_cols)["importe"].sum().reset_index()
                sscc = sscc.rename(columns={"importe": "sscc_importe"})
                df = pd.merge(siif, sscc, how="outer", on=groupby_cols, copy=False)
                df[["siif_importe", "sscc_importe"]] = df[
                    ["siif_importe", "sscc_importe"]
                ].fillna(0)
                df["diferencia"] = df.siif_importe - df.sscc_importe

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=df,
                    model=ControlBancoReport,
                    field_id="mes",
                )

                partial_schema = await sync_validated_to_repository(
                    repository=self.control_banco_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title=f"Control Banco del ejercicio {ejercicio}",
                    logger=logger,
                    label=f"Control Banco del ejercicio {ejercicio}",
                )
                return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control Banco",
            )
        except Exception as e:
            logger.error(f"Error in Control Banco: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in Control Banco",
            )
        finally:
            return return_schema

    # -------------------------------------------------
    async def export_all_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ControlBancoParams = None,
    ) -> StreamingResponse:
        ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
        control_banco_docs = await self.control_banco_repo.find_by_filter(
            filters={"ejercicio": {"$in": ejercicios}},
        )

        if not control_banco_docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        siif = pd.DataFrame()
        sscc = pd.DataFrame()
        for ejercicio in ejercicios:
            df = await self.generate_banco_siif(ejercicio=ejercicio)
            siif = pd.concat([siif, df], ignore_index=True)
            df = await self.generate_banco_sscc(ejercicio=ejercicio)
            sscc = pd.concat([sscc, df], ignore_index=True)

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (pd.DataFrame(control_banco_docs), "siif_vs_sscc_db"),
                (sscc, "sscc_db"),
                (siif, "siif_db"),
            ],
            filename="control_banco.xlsx",
            spreadsheet_key="1CRQjzIVzHKqsZE8_E1t8aRQDfWfZALhbe64WcxHiSM4",
            upload_to_google_sheets=upload_to_google_sheets,
        )


ControlBancoServiceDependency = Annotated[ControlBancoService, Depends()]
