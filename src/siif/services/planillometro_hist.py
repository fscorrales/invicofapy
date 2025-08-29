__all__ = ["PlanillometroHistService", "PlanillometroHistServiceDependency"]

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
from ..handlers import PlanillometroHistMongoMigrator
from ..repositories import PlanillometroHistRepositoryDependency
from ..schemas import PlanillometroHistDocument


# -------------------------------------------------
@dataclass
class PlanillometroHistService:
    repository: PlanillometroHistRepositoryDependency

    # -------------------------------------------------
    async def get_planillometro_hist_from_db(
        self, params: BaseFilterParams
    ) -> List[PlanillometroHistDocument]:
        return await self.repository.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Planillometro Historico from the database",
        )

    # -------------------------------------------------
    async def sync_planillometro_hist_from_excel(
        self, excel_path: str
    ) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(excel_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            planillometro_hist = PlanillometroHistMongoMigrator(excel_path=excel_path)
            return_schema = await planillometro_hist.sync_validated_excel_to_repository()
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
    async def export_planillometro_hist_from_db(self) -> StreamingResponse:
        docs = await self.repository.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        return export_dataframe_as_excel_response(
            df,
            filename="unified_planillometro_hist.xlsx",
            sheet_name="planillometro_hist",
        )


PlanillometroHistServiceDependency = Annotated[PlanillometroHistService, Depends()]
