__all__ = ["Rdeu012b2CService", "Rdeu012b2CServiceDependency"]

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
from ..handlers import Rdeu012b2CMongoMigrator
from ..repositories import Rdeu012b2CRepositoryDependency
from ..schemas import Rdeu012b2CDocument


# -------------------------------------------------
@dataclass
class Rdeu012b2CService:
    repository: Rdeu012b2CRepositoryDependency

    # -------------------------------------------------
    async def get_rdeu012b2_c_from_db(
        self, params: BaseFilterParams
    ) -> List[Rdeu012b2CDocument]:
        return await self.repository.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Rdeu012b2C from the database",
        )

    # -------------------------------------------------
    async def sync_rdeu012b2_c_from_excel(self, excel_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(excel_path):
            raise HTTPException(status_code=404, detail="Archivo Excel no encontrado")

        return_schema = RouteReturnSchema()
        try:
            rdeu012b2_c = Rdeu012b2CMongoMigrator(excel_path=excel_path)
            return_schema = await rdeu012b2_c.sync_validated_excel_to_repository()
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
    async def export_rdeu012b2_c_from_db(self) -> StreamingResponse:
        docs = await self.repository.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        return export_dataframe_as_excel_response(
            df,
            filename="rdeu012b2_c.xlsx",
            sheet_name="rdeu012b2_c",
        )


Rdeu012b2CServiceDependency = Annotated[Rdeu012b2CService, Depends()]
