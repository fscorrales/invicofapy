#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Reporte Listado de Obras
Data required:
    - Icaro (OBRAS)
Google Sheet:
    - https://docs.google.com/spreadsheets/d/1KnKs7RXzN7QPjNjxkXQY89DWzzGecwcXoPpAYiNRxkU (Listado Obras)

"""

__all__ = [
    "ReporteListadoObrasService",
    "ReporteListadoObrasServiceDependency",
]

import os
from dataclasses import dataclass
from typing import Annotated, List

import pandas as pd
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import ValidationError

from ...config import logger
from ...icaro.handlers import IcaroMongoMigrator
from ...utils import (
    GoogleExportResponse,
    RouteReturnSchema,
    export_multiple_dataframes_to_excel,
    get_r_icaro_path,
    upload_multiple_dataframes_to_google_sheets,
)
from ..handlers import get_icaro_obras
from ..schemas.reporte_listado_obras import (
    ReporteListadoObrasParams,
    ReporteListadoObrasSyncParams,
)


# --------------------------------------------------
@dataclass
class ReporteListadoObrasService:
    # -------------------------------------------------
    async def sync_listado_obras_from_source(
        self,
        params: ReporteListadoObrasSyncParams = None,
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
        try:
            # 🔹 Icaro
            path = os.path.join(get_r_icaro_path(), "ICARO.sqlite")
            migrator = IcaroMongoMigrator(sqlite_path=path)
            return_schema.append(await migrator.migrate_obras())

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
            # try:
            #     await logout(connect=connect_siif)
            # except Exception as e:
            #     logger.warning(f"Logout falló o browser ya cerrado: {e}")
            return return_schema

    # -------------------------------------------------
    async def generate_all(
        self, params: ReporteListadoObrasParams = None
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Reporte Listado Obras Icaro
            partial_schema = await self.generate_reporte_listado_obras_icaro(
                params=params
            )
            return_schema.append(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Reporte Listado Obras",
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
        params: ReporteListadoObrasParams = None,
    ) -> list[tuple[pd.DataFrame, str]]:
        # ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))

        # for ejercicio in ejercicios:
        #     icaro = pd.concat(
        #         [
        #             icaro,
        #             await self.generate_reporte_listado_obras_icaro(
        #                 ejercicio=ejercicio
        #             ),
        #         ],
        #         ignore_index=True,
        #     )

        icaro = await self.generate_reporte_listado_obras_icaro()
        return [
            # (siif, "bd_siif"),
            (icaro, "icaro_new"),
        ]

    # -------------------------------------------------
    async def export_all_from_db(
        self,
        upload_to_google_sheets: bool = True,
        params: ReporteListadoObrasParams = None,
    ) -> StreamingResponse:
        df_sheet_pairs = await self._build_dataframes_to_export(params)

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=df_sheet_pairs,
            filename="listado_obras.xlsx",
            spreadsheet_key="1KnKs7RXzN7QPjNjxkXQY89DWzzGecwcXoPpAYiNRxkU",
            upload_to_google_sheets=upload_to_google_sheets,
        )

    # -------------------------------------------------
    async def export_all_from_db_to_google(
        self,
        params: ReporteListadoObrasParams = None,
    ) -> GoogleExportResponse:
        df_sheet_pairs = await self._build_dataframes_to_export(params)

        return upload_multiple_dataframes_to_google_sheets(
            df_sheet_pairs=df_sheet_pairs,
            spreadsheet_key="1KnKs7RXzN7QPjNjxkXQY89DWzzGecwcXoPpAYiNRxkU",
            title="Listado Obras",
        )

    # --------------------------------------------------
    async def generate_reporte_listado_obras_icaro(
        self,
    ) -> pd.DataFrame:
        df = await get_icaro_obras()

        if df.empty:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        df = df.drop(columns=["_id"])
        # # Supongamos que tienes un DataFrame df con una columna 'columna_con_numeros' que contiene los registros con la parte numérica al principio
        # df["desc_obra"] = df["desc_obra"].str.replace(r"^\d+-\d+", "", regex=True)
        # df["desc_obra"] = df["desc_obra"].str.lstrip()
        # df["imputacion"] = df["actividad"] + "-" + df["partida"]
        # df = pd.concat(
        #     [
        #         df[["desc_obra", "imputacion"]],
        #         df.drop(columns=["desc_obra", "imputacion"]),
        #     ],
        #     axis=1,
        # )
        # df["desc_obra"] = df["desc_obra"].str.slice(0, 85)
        # df["desc_obra"] = df["desc_obra"].str.strip()

        return df


ReporteListadoObrasServiceDependency = Annotated[ReporteListadoObrasService, Depends()]
