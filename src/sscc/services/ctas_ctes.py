__all__ = ["CtasCtesService", "CtasCtesServiceDependency"]

import os
from dataclasses import dataclass
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
from ..handlers import CtasCtesMongoMigrator
from ..repositories import CtasCtesRepositoryDependency
from ..schemas import CtasCtesDocument


# -------------------------------------------------
@dataclass
class CtasCtesService:
    repository: CtasCtesRepositoryDependency

    # -------------------------------------------------
    async def get_ctas_ctes_from_db(
        self, params: BaseFilterParams
    ) -> List[CtasCtesDocument]:
        return await self.repository.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Cuentas Corrientes from the database",
        )

    # -------------------------------------------------
    async def sync_ctas_ctes_from_excel(self, excel_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(excel_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            ctas_ctes = CtasCtesMongoMigrator(excel_path=excel_path)
            return_schema = await ctas_ctes.sync_validated_excel_to_repository()
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
    async def export_ctas_ctes_from_db(self) -> StreamingResponse:
        docs = await self.repository.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        return export_dataframe_as_excel_response(
            df,
            filename="unified_ctas_ctes.xlsx",
            sheet_name="ctas_ctes",
        )


CtasCtesServiceDependency = Annotated[CtasCtesService, Depends()]
