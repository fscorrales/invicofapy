#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: SIIF Recursos vs SSCC depósitos
Data required:
    - SIIF rci02
    - SIIF ri102 (no obligatorio)
    - SSCC Consulta General de Movimiento
    - SSCC ctas_ctes (manual data)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1u_I5wN3w_rGX6rWIsItXkmwfIEuSox6ZsmKYbMZ2iUY
"""

__all__ = ["ControlRecursosService", "ControlRecursosServiceDependency"]

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
    login,
    logout,
)
from ...siif.repositories import Rci02RepositoryDependency
from ...sscc.services import BancoINVICOServiceDependency, CtasCtesServiceDependency
from ...utils import (
    BaseFilterParams,
    RouteReturnSchema,
    export_dataframe_as_excel_response,
    export_multiple_dataframes_to_excel,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..handlers import get_banco_invico_unified_cta_cte, get_siif_rci02_unified_cta_cte
from ..repositories.control_recursos import ControlRecursosRepositoryDependency
from ..schemas.control_recursos import (
    ControlRecursosDocument,
    ControlRecursosParams,
    ControlRecursosReport,
    ControlRecursosSyncParams,
)


# --------------------------------------------------
@dataclass
class ControlRecursosService:
    control_recursos_repo: ControlRecursosRepositoryDependency
    siif_comprobantes_recursos_repo: Rci02RepositoryDependency
    banco_invico_service: BancoINVICOServiceDependency
    ctas_ctes_service: CtasCtesServiceDependency
    siif_rf602_handler: Rci02 = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_control_recursos_from_source(
        self,
        params: ControlRecursosSyncParams = None,
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
        ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
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
                await self.siif_rci02_handler.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rci02_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio),
                    )
                    return_schema.append(partial_schema)

                # 🔹Banco INVICO
                partial_schema = (
                    await self.banco_invico_service.sync_banco_invico_from_sscc(
                        username=params.sscc_username,
                        password=params.sscc_password,
                        params=params,
                    )
                )
                return_schema.extend(partial_schema)

                # 🔹Ctas Ctes
                partial_schema = await self.ctas_ctes_service.sync_ctas_ctes_from_excel(
                    excel_path=params.ctas_ctes_excel_path,
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
        self, params: ControlRecursosParams
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control Recursos
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            for ejercicio in ejercicios:
                partial_schema = await self.compute_control_recursos(ejercicio=ejercicio)
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

        control_recursos_docs = await self.control_recursos_repo.get_all()

        if not control_recursos_docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (pd.DataFrame(control_recursos_docs), "new_control_recursos"),
                (
                    await self.generate_siif_comprobantes_recursos(),
                    "new_siif_recursos",
                ),
                (await self.generate_banco_invico(), "new_banco_ingresos"),
            ],
            filename="control_recursos.xlsx",
            spreadsheet_key="1hJyBOkA8sj5otGjYGVOzYViqSpmv_b4L8dXNju_GJ5Q",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # --------------------------------------------------
    async def generate_siif_comprobantes_recursos(
        self, ejercicio: int = None
    ) -> pd.DataFrame:
        df = await get_siif_rci02_unified_cta_cte(
            ejercicio=ejercicio, filters={"es_verificado": True}
        )
        if df.empty:
            raise HTTPException(
                status_code=404,
                detail="No se encontraron registros en la colección siif_comprobantes_recursos",
            )
        # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
        keep = ["MACRO"]
        df.loc[df.glosa.str.contains("|".join(keep)), "cta_cte"] = "Macro"
        # df["cta_cte"].loc[df.glosa.str.contains("|".join(keep))] = "Macro"
        df["grupo"] = np.where(
            df["cta_cte"] == "10270",
            "FONAVI",
            np.where(
                df["cta_cte"].isin(["130832-12", "334", "Macro", "Patagonia"]),
                "RECUPEROS",
                "OTROS",
            ),
        )
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    async def generate_banco_invico(self, ejercicio: int = None) -> pd.DataFrame:
        dep_transf_int = ["034", "004"]
        dep_pf = ["214", "215"]
        dep_otros = ["003", "055", "005", "013"]
        dep_cert_neg = ["18"]
        filters = {
            "movimiento": "DEPOSITO",
            "cod_imputacion__nin": dep_transf_int + dep_pf + dep_otros + dep_cert_neg,
        }
        df = await get_banco_invico_unified_cta_cte(
            ejercicio=ejercicio, filters=filters
        )
        if df.empty:
            raise HTTPException(
                status_code=404,
                detail="No se encontraron registros en la colección banco_invico",
            )
        # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
        df["grupo"] = np.where(
            df["cta_cte"] == "10270",
            "FONAVI",
            np.where(
                df["cta_cte"].isin(["130832-12", "334", "Macro", "Patagonia"]),
                "RECUPEROS",
                "OTROS",
            ),
        )
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    async def compute_control_recursos(
        self, ejercicio: int
    ) -> RouteReturnSchema:
        return_schema = RouteReturnSchema()
        try:
            group_by = ["ejercicio", "mes", "cta_cte", "grupo"]
            siif = await self.generate_siif_comprobantes_recursos(
                ejercicio=int(ejercicio)
            )
            # logger.info(f"siif.shape: {siif.shape}")
            siif = siif.loc[~siif["es_invico"]]
            siif = siif.loc[~siif["es_remanente"]]
            # logger.info(f"siif.shape: {siif.shape}")
            siif = siif.groupby(group_by)["importe"].sum()
            siif = siif.reset_index(drop=False)
            siif = siif.rename(columns={"importe": "recursos_siif"})
            # logger.info(f"siif.head: {siif.head()}")
            sscc = await self.generate_banco_invico(ejercicio=int(ejercicio))
            sscc = sscc.groupby(group_by)["importe"].sum()
            sscc = sscc.reset_index(drop=False)
            sscc = sscc.rename(columns={"importe": "depositos_banco"})
            # logger.info(f"sscc.head: {sscc.head()}")
            df = pd.merge(siif, sscc, how="outer")
            # logger.info(f"df.shape: {df.shape}, df.head: {df.head()}")
            # 🔹 Validar datos usando Pydantic
            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df, model=ControlRecursosReport, field_id="cta_cte"
            )

            return_schema = await sync_validated_to_repository(
                repository=self.control_recursos_repo,
                validation=validate_and_errors,
                delete_filter=None,
                title=f"Control de Recursos Ejercicio {ejercicio}",
                logger=logger,
                label=f"Control de Recursos Ejercicio {ejercicio}",
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
    ) -> List[ControlRecursosDocument]:
        return await self.control_recursos_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Reporte de Ejecución Presupuestaria SIIF con Descripción from the database",
        )

    # -------------------------------------------------
    async def export_control_recursos_from_db(
        self, upload_to_google_sheets: bool = True
    ) -> StreamingResponse:
        df = pd.DataFrame(await self.control_recursos_repo.get_all())

        return export_dataframe_as_excel_response(
            df,
            filename="control_recursos.xlsx",
            sheet_name="control_recursos",
            upload_to_google_sheets=upload_to_google_sheets,
            google_sheet_key="1u_I5wN3w_rGX6rWIsItXkmwfIEuSox6ZsmKYbMZ2iUY",
        )


ControlRecursosServiceDependency = Annotated[ControlRecursosService, Depends()]
