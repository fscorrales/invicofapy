__all__ = ["Rdeu012b2CuitService", "Rdeu012b2CuitServiceDependency"]

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
from ..handlers import Rdeu012b2Cuit
from ..repositories import Rdeu012b2CuitRepositoryDependency
from ..schemas import Rdeu012b2CuitDocument


# -------------------------------------------------
@dataclass
class Rdeu012b2CuitService:
    repository: Rdeu012b2CuitRepositoryDependency
    rdeu012b2_cuit: Rdeu012b2Cuit = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.rdeu012b2_cuit = Rdeu012b2Cuit()

    # -------------------------------------------------
    async def get_rdeu012b2_cuit_from_db(
        self, params: BaseFilterParams
    ) -> List[Rdeu012b2CuitDocument]:
        return await self.repository.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Rdeu012b2Cuit from the database",
        )

    # -------------------------------------------------
    async def sync_rdeu012b2_cuit_from_pdf(self, pdf_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(pdf_path):
            raise HTTPException(status_code=404, detail="Archivo PDF no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = await self.rdeu012b2_cuit.sync_validated_pdf_to_repository(
                pdf_path=pdf_path
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
    async def sync_rdeu012b2_cuit_from_sqlite(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = (
                await self.rdeu012b2_cuit.sync_validated_sqlite_to_repository(
                    sqlite_path=sqlite_path
                )
            )
        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400, detail="Invalid response format from SIIF"
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
    async def export_rdeu012b2_cuit_from_db(self) -> StreamingResponse:
        docs = await self.repository.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        return export_dataframe_as_excel_response(
            df,
            filename="rdeu012b2_cuit.xlsx",
            sheet_name="rdeu012b2_cuit",
        )


Rdeu012b2CuitServiceDependency = Annotated[Rdeu012b2CuitService, Depends()]
