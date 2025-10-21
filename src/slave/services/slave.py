__all__ = ["SlaveService", "SlaveServiceDependency"]

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
    export_multiple_dataframes_to_excel,
)
from ..handlers import SlaveMongoMigrator
from ..repositories import (
    FacturerosRepositoryDependency,
    HonorariosRepositoryDependency,
)
from ..schemas import (
    FacturerosDocument,
    HonorariosDocument,
)


# -------------------------------------------------
@dataclass
class SlaveService:
    factureros_repo: FacturerosRepositoryDependency
    honorarios_repo: HonorariosRepositoryDependency

    # -------------------------------------------------
    async def get_factureros_from_db(
        self, params: BaseFilterParams
    ) -> List[FacturerosDocument]:
        return await self.factureros_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Factureros from the database",
        )

    # -------------------------------------------------
    async def get_honorarios_from_db(
        self, params: BaseFilterParams
    ) -> List[HonorariosDocument]:
        return await self.honorarios_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Honorarios from the database",
        )

    # -------------------------------------------------
    async def sync_all_from_access(self, access_path: str) -> List[RouteReturnSchema]:
        # ✅ Validación temprana
        if not os.path.exists(access_path):
            raise HTTPException(status_code=404, detail="Archivo Access no encontrado")

        return_schema = []
        try:
            slave = SlaveMongoMigrator(access_path=access_path)
            return_schema = await slave.migrate_all()
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
    async def sync_factureros_from_access(self, access_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(access_path):
            raise HTTPException(status_code=404, detail="Archivo Access no encontrado")

        return_schema = RouteReturnSchema()
        try:
            access = SlaveMongoMigrator(access_path=access_path)
            return_schema = await access.migrate_factureros()
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
    async def export_factureros_from_db(self) -> StreamingResponse:
        docs = await self.factureros_repo.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        return export_dataframe_as_excel_response(
            df,
            filename="slave_factureros.xlsx",
            sheet_name="factureros",
        )

    # -------------------------------------------------
    async def export_all_from_db(self) -> StreamingResponse:
        factureros_docs = await self.factureros_repo.get_all()
        honorarios_docs = await self.honorarios_repo.get_all()

        if not factureros_docs and not honorarios_docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (pd.DataFrame(factureros_docs), "factureros"),
                (pd.DataFrame(honorarios_docs), "honorarios"),
            ],
            filename="slave.xlsx",
            spreadsheet_key=None,
            upload_to_google_sheets=False,
        )


SlaveServiceDependency = Annotated[SlaveService, Depends()]
