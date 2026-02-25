__all__ = ["BancoINVICOSdoFinalService", "BancoINVICOSdoFinalServiceDependency"]

import os
from dataclasses import dataclass, field
from typing import Annotated, List

import pandas as pd
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import ValidationError

from ...config import logger
from ...utils import (
    BaseFilterParams,
    RouteReturnSchema,
    export_dataframe_as_excel_response,
)
from ..handlers import BancoINVICOSdoFinal
from ..repositories import BancoINVICOSdoFinalRepositoryDependency
from ..schemas import BancoINVICOSdoFinalDocument


# -------------------------------------------------
@dataclass
class BancoINVICOSdoFinalService:
    repository: BancoINVICOSdoFinalRepositoryDependency
    banco_invico_sdo_final: BancoINVICOSdoFinal = field(
        init=False
    )  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.banco_invico_sdo_final = BancoINVICOSdoFinal()

    # -------------------------------------------------
    async def get_banco_invico_sdo_final_from_db(
        self, params: BaseFilterParams
    ) -> List[BancoINVICOSdoFinalDocument]:
        return await self.repository.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving BancoINVICOSdoFinal from the database",
        )

    # -------------------------------------------------
    async def sync_banco_invico_sdo_final_from_csv(
        self, csv_path: str
    ) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(csv_path):
            raise HTTPException(status_code=404, detail="Archivo CSV no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = (
                await self.banco_invico_sdo_final.sync_validated_csv_to_repository(
                    csv_path=csv_path
                )
            )
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
            return return_schema

    # -------------------------------------------------
    async def sync_banco_invico_sdo_final_from_sqlite(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = (
                await self.banco_invico_sdo_final.sync_validated_sqlite_to_repository(
                    sqlite_path=sqlite_path
                )
            )
        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400, detail="Invalid response format from SSCC"
            )
        except Exception as e:
            logger.error(f"Error during report processing: {e}")
            raise HTTPException(
                status_code=401,
                detail="Invalid credentials or unable to authenticate",
            )
        finally:
            return return_schema

    # ----------------------------------------------------
    async def export_banco_invico_sdo_final_from_db(self) -> StreamingResponse:
        docs = await self.repository.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        return export_dataframe_as_excel_response(
            df,
            filename="banco_invico_sdo_final.xlsx",
            sheet_name="banco_invico_sdo_final",
        )


BancoINVICOSdoFinalServiceDependency = Annotated[BancoINVICOSdoFinalService, Depends()]
