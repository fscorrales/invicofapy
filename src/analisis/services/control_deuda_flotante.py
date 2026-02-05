#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: SIIF Control de Deuda Flotante
Data required:
    - SIIF rdeu012
    - SIIF rvicon03
    - SIIF rcocc31
    - SSCC ctas_ctes (manual data)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1rYBKQhzz_5iahkFPXI5FtOwjqbJYLpNA_OMEfNR81us
"""

__all__ = ["ControlDeudaFlotanteService", "ControlDeudaFlotanteServiceDependency"]

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
    Rcocc31,
    Rdeu012,
    Rvicon03,
    login,
    logout,
)
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
    get_siif_rcocc31,
    get_siif_rdeu012_unified_cta_cte,
    get_siif_rvicon03,
)
from ..repositories.control_deuda_flotante import (
    ControlDeudaFlotanteRepositoryDependency,
)
from ..schemas.control_deuda_flotante import (
    ControlDeudaFlotanteParams,
    ControlDeudaFlotanteReport,
    ControlDeudaFlotanteSyncParams,
)


# --------------------------------------------------
@dataclass
class ControlDeudaFlotanteService:
    control_deuda_flotante_repo: ControlDeudaFlotanteRepositoryDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency
    siif_rcocc31_handler: Rcocc31 = field(init=False)  # No se pasa como argumento
    siif_rvicon03_handler: Rvicon03 = field(init=False)  # No se pasa como argumento
    siif_rdeu012_handler: Rdeu012 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_control_deuda_flotante_from_source(
        self,
        params: ControlDeudaFlotanteSyncParams = None,
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
                for ejercicio in ejercicios:
                    cuentas_contables = await get_siif_rvicon03(ejercicio=ejercicio)
                    cuentas_contables = cuentas_contables["cta_contable"].unique()
                    logger.info(
                        f"Se Bajaran las siguientes cuentas contables: {cuentas_contables}"
                    )
                    for cta_contable in cuentas_contables:
                        partial_schema = await self.siif_rcocc31_handler.download_and_sync_validated_to_repository(
                            ejercicio=ejercicio,
                            cta_contable=cta_contable,
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
        self, params: ControlDeudaFlotanteParams
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control Deuda Flotante
            partial_schema = await self.compute_control_deuda_flotante(params=params)
            return_schema.extend(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control de Deuda Flotante",
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
        params: ControlDeudaFlotanteParams,
    ) -> list[tuple[pd.DataFrame, str]]:
        ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))

        # control_debitos_bancarios_docs = (
        #     await self.control_debitos_bancarios_repo.find_by_filter(
        #         {"ejercicio": {"$in": ejercicios}}
        #     )
        # )

        # if not control_debitos_bancarios_docs:
        #     raise HTTPException(status_code=404, detail="No se encontraron registros")

        rvicon03 = pd.DataFrame()
        rcocc31 = pd.DataFrame()
        for ejercicio in ejercicios:
            df = await get_siif_rvicon03(ejercicio=ejercicio)
            rvicon03 = pd.concat([rvicon03, df], ignore_index=True)
            df = await get_siif_rcocc31(ejercicio=ejercicio)
            rcocc31 = pd.concat([rcocc31, df], ignore_index=True)

        return [
            # (pd.DataFrame(control_debitos_bancarios_docs), "siif_vs_sscc_db"),
            (rvicon03, "bd_rvicon03"),
            (rcocc31, "bd_rcocc31"),
        ]

    # -------------------------------------------------
    async def export_all_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ControlDeudaFlotanteParams = None,
    ) -> StreamingResponse:
        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=await self._build_dataframes_to_export(params=params),
            filename="control_deuda_flotante.xlsx",
            spreadsheet_key="1rYBKQhzz_5iahkFPXI5FtOwjqbJYLpNA_OMEfNR81us",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # -------------------------------------------------
    async def export_all_from_db_to_google(
        self,
        params: ControlDeudaFlotanteParams = None,
    ) -> GoogleExportResponse:
        return upload_multiple_dataframes_to_google_sheets(
            df_sheet_pairs=await self._build_dataframes_to_export(params),
            spreadsheet_key="1rYBKQhzz_5iahkFPXI5FtOwjqbJYLpNA_OMEfNR81us",
            title="Control Deuda Flotante",
        )

    # --------------------------------------------------
    async def generate_last_rdeu012(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        df = await get_siif_rdeu012_unified_cta_cte(ejercicio=ejercicio)
        df = df.reset_index(drop=True)
        df = df.loc[df["mes_hasta"].str.endswith(ejercicio), :]
        months = df["mes_hasta"].tolist()
        # Convertir cada elemento de la lista a un objeto datetime
        dates = [datetime.strptime(month, "%m/%Y") for month in months]
        # Obtener la fecha más reciente y Convertir la fecha mayor a un string en el formato 'MM/YYYY'
        gt_month = max(dates).strftime("%m/%Y")
        df = df.loc[df["mes_hasta"] == gt_month, :]
        df = df.rename(
            columns={"nro_origen": "nro_original", "saldo": "saldo_rdeu"},
        )
        # Elimino comprobantes específicos (error en SIIF)
        df = df.loc[
            ~df["nro_comprobante"].isin(["02749/11"])
        ]  # No debería estar en la RDEU
        return df

    # --------------------------------------------------
    async def generate_rcocc31_liabilities(
        self,
        ejercicio: int = None,
    ) -> pd.DataFrame:
        tipos_comprobantes = ["CAO", "CAP", "CAM", "CAD", "ANP", "AJU"]
        filters = {
            "tipo_comprobante": {"$in": tipos_comprobantes},
            "cta_contable": {"$regex": "/^2/"},
        }
        df = await get_siif_rcocc31(ejercicio=ejercicio, filters=filters)
        df = df.reset_index(drop=True)
        # df = df.loc[df["cta_contable"].str.startswith("2"), :]
        # df = df.loc[df["tipo_comprobante"].isin(tipos_comprobantes), :]
        df = df.rename(columns={"saldo": "saldo_contable"})
        return df

    # --------------------------------------------------
    async def generate_aju_not_in_rdue012(
        self,
        filter_rdeu: pd.DataFrame,
        rcocc31: pd.DataFrame,
        ejercicio: int = None,
    ) -> pd.DataFrame:

        aju = rcocc31.loc[rcocc31["tipo_comprobante"].isin(["AJU"])]
        aju["nro_comprobante"] = aju["nro_entrada"] + "/" + aju["ejercicio"].str[2:]

        # Elimino Amortizaciones Acum. del Pasivo (cta. contable empieza con 2241)
        aju = aju.loc[~aju["cta_contable"].str.startswith("2241")]

        # Elimino Otros Fondos de Terceros a Pagar (cta. contable 2113-2-9)
        aju = aju.loc[~aju["cta_contable"].isin(["2113-2-9"])]

        # Elimino comprobantes específicos (error en SIIF)
        aju = aju.loc[
            ~aju["nro_comprobante"].isin(["16535/11"])
        ]  # AJUSTE DE SUELDOS Y SALARIOS A PAGAR
        aju = aju.loc[
            ~aju["nro_comprobante"].isin(["15793/12"])
        ]  # AJUSTES RETENCIONES. COMPROB.225/2012
        aju = aju.loc[
            ~aju["nro_comprobante"].isin(["17773/16"])
        ]  # ERROR PAGO COMPROBANTE GTOS. 2749/2011
        aju = aju.loc[
            ~aju["nro_comprobante"].isin(["17096/13"])
        ]  # REGISTROS PAGOS A.R.T. ENERO Y FEBRERO/2013
        aju = aju.loc[
            ~aju["nro_comprobante"].isin(["17097/13"])
        ]  # PAGO COMPR. CAO 5413/13. MAP 5429/13. ERROR SISTEMA
        aju = aju.loc[
            ~aju["nro_comprobante"].isin(["19897/17"])
        ]  # AJUSTES DE LOS DRI DEL AÑO 2017. DEVOLUCIÓN RETENCIÓN IMP. GCIAS. 245
        aju = aju.loc[
            ~aju["nro_comprobante"].isin(["15142/21"])
        ]  # AJU SALIDA DE BANCO DEL DRI 497-10-09-2021
        aju = aju.loc[
            ~aju["nro_comprobante"].isin(["16986/24"])
        ]  # DEV RET INDEBIDAS IIBB- DRI 1348.AJUSTE CTA OTROS ANTICIPOS.

        # Conservo los AJU con saldo mayor a 0.1 y el 16536/11
        aju_keep = aju.loc[aju["nro_comprobante"].isin(["16536/11"])]
        # aju_keep = aju_keep.append(aju[aju['tipo_comprobante'] == 'DRI'])
        aju_keep = aju_keep.drop(columns=["nro_comprobante"])
        filtered_aju = aju.groupby("nro_original").sum()["saldo_contable"]
        filtered_aju = filtered_aju[abs(filtered_aju) > 0.1]
        aju = aju.merge(
            filtered_aju.reset_index()["nro_original"], on="nro_original", how="right"
        )
        aju = pd.concat([aju, aju_keep], axis=0)
        df = pd.DataFrame(columns=filter_rdeu.columns)
        df = df.drop(columns=["ejercicio", "nro_original"])
        df = pd.concat([df, aju], axis=1)
        df["ejercicio_contable"] = ejercicio
        df["fuente"] = "11"
        df["saldo_rdeu"] = df["saldo_contable"] * (-1)
        return df

    # --------------------------------------------------
    async def compute_control_deuda_flotante(
        self,
        params: ControlDeudaFlotanteParams,
    ) -> List[RouteReturnSchema]:
        return_schema = []
        try:
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                rdeu = await self.generate_last_rdeu012(ejercicio=ejercicio)
                rcocc31 = await self.generate_rcocc31_liabilities(ejercicio=ejercicio)
                cyo = rdeu["nro_comprobante"].tolist()
                cyo = list(map(lambda x: str(int(x[:-3])), cyo))
                aju = rcocc31.loc[rcocc31["tipo_comprobante"].isin(["AJU"])]
                df = rcocc31.loc[rcocc31["nro_original"].isin(cyo)]
                filter_rcocc31 = pd.concat([df, aju])
                rdeu["ejercicio_contable"] = ejercicio
                rdeu = rdeu[
                    ["ejercicio_contable"]
                    + [col for col in rdeu.columns if col != "ejercicio_contable"]
                ]
                # ctrl_rdeu.rdeu012 = pd.concat([ctrl_rdeu.rdeu012, rdeu])
                # ctrl_rdeu.rcocc31 = pd.concat([ctrl_rdeu.rcocc31, filter_rcocc31])
                rdeu = rdeu.loc[
                    :,
                    [
                        "ejercicio_contable",
                        "ejercicio",
                        "fuente",
                        "cta_cte",
                        "nro_original",
                        "saldo_rdeu",
                        "cuit",
                        "glosa",
                        "nro_expte",
                    ],
                ]
                ctrl_rdeu = rdeu.merge(
                    filter_rcocc31,
                    how="left",
                    on=["ejercicio", "nro_original"],
                )

                aju = await self.generate_aju_not_in_rdue012(
                    filter_rdeu=rdeu, rcocc31=filter_rcocc31, ejercicio=ejercicio
                )
                ctrl_rdeu = pd.concat([ctrl_rdeu, aju])

                # 🔹 Validar datos usando Pydantic
                validate_and_errors = validate_and_extract_data_from_df(
                    dataframe=ctrl_rdeu,
                    model=ControlDeudaFlotanteReport,
                    field_id="nro_entrada",
                )

                partial_schema = await sync_validated_to_repository(
                    repository=self.control_deuda_flotante_repo,
                    validation=validate_and_errors,
                    delete_filter={"ejercicio": ejercicio},
                    title=f"Control Deuda Flotante del ejercicio {ejercicio}",
                    logger=logger,
                    label=f"Control Deuda Flotante del ejercicio {ejercicio}",
                )
                return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control Deuda Flotante",
            )
        except Exception as e:
            logger.error(f"Error in Control Deuda Flotante: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in Control Deuda Flotante",
            )
        finally:
            return return_schema


ControlDeudaFlotanteServiceDependency = Annotated[
    ControlDeudaFlotanteService, Depends()
]
