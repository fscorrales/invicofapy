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
    export_multiple_dataframes_to_excel,
)
from ..handlers import IcaroMongoMigrator
from ..repositories import (
    CargaRepositoryDependency,
    CertificadosRepositoryDependency,
    CtasCtesRepositoryDependency,
    EstructurasRepositoryDependency,
    FuentesRepositoryDependency,
    ObrasRepositoryDependency,
    PartidasRepositoryDependency,
    ProveedoresRepositoryDependency,
    ResumenRendObrasRepositoryDependency,
    RetencionesRepositoryDependency,
)
from ..schemas import CtasCtesDocument


# -------------------------------------------------
@dataclass
class CtasCtesService:
    obras_repo: ObrasRepositoryDependency
    carga_repo: CargaRepositoryDependency
    certificados_repo: CertificadosRepositoryDependency
    ctas_ctes_repo: CtasCtesRepositoryDependency
    estructuras_repo: EstructurasRepositoryDependency
    fuentes_repo: FuentesRepositoryDependency
    partidas_repo: PartidasRepositoryDependency
    proveedores_repo: ProveedoresRepositoryDependency
    resumen_rend_obras_repo: ResumenRendObrasRepositoryDependency
    retenciones_repo: RetencionesRepositoryDependency

    # -------------------------------------------------
    async def get_obras_from_db(
        self, params: BaseFilterParams
    ) -> List[CtasCtesDocument]:
        return await self.obras_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Obras from the database",
        )

    # -------------------------------------------------
    async def sync_all_from_sqlite(self, sqlite_path: str) -> List[RouteReturnSchema]:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = []
        try:
            icaro = IcaroMongoMigrator(sqlite_path=sqlite_path)
            return_schema = await icaro.migrate_all()
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
    async def sync_obras_from_sqlite(self, sqlite_path: str) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            icaro = IcaroMongoMigrator(sqlite_path=sqlite_path)
            return_schema = await icaro.migrate_obras()
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
    async def export_obras_from_db(self) -> StreamingResponse:
        docs = await self.obras_repo.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        return export_dataframe_as_excel_response(
            df,
            filename="icaro_obras.xlsx",
            sheet_name="obras",
        )

    # -------------------------------------------------
    async def export_all_from_db(self) -> StreamingResponse:
        # ejecucion_obras.reporte_planillometro_contabilidad (planillometro_contabilidad)
        obras_docs = await self.obras_repo.get_all()
        carga_docs = await self.carga_repo.get_all()
        certificados_docs = await self.certificados_repo.get_all()
        ctas_ctes_docs = await self.ctas_ctes_repo.get_all()
        estructuras_docs = await self.estructuras_repo.get_all()
        fuentes_docs = await self.fuentes_repo.get_all()
        partidas_docs = await self.partidas_repo.get_all()
        proveedores_docs = await self.proveedores_repo.get_all()
        resumen_rend_obras_docs = await self.resumen_rend_obras_repo.get_all()
        retenciones_docs = await self.retenciones_repo.get_all()

        if (
            not obras_docs
            and not carga_docs
            and not certificados_docs
            and not ctas_ctes_docs
            and not estructuras_docs
            and not fuentes_docs
            and not partidas_docs
            and not proveedores_docs
            and not resumen_rend_obras_docs
            and not retenciones_docs
        ):
            raise HTTPException(status_code=404, detail="No se encontraron registros")

        return export_multiple_dataframes_to_excel(
            df_sheet_pairs=[
                (pd.DataFrame(obras_docs), "obras"),
                (pd.DataFrame(carga_docs), "carga"),
                (pd.DataFrame(certificados_docs), "certificados"),
                (pd.DataFrame(ctas_ctes_docs), "ctas_ctes"),
                (pd.DataFrame(estructuras_docs), "estructuras"),
                (pd.DataFrame(fuentes_docs), "fuentes"),
                (pd.DataFrame(partidas_docs), "partidas"),
                (pd.DataFrame(proveedores_docs), "proveedores"),
                (pd.DataFrame(resumen_rend_obras_docs), "resumen_rend_obras"),
                (pd.DataFrame(retenciones_docs), "retenciones"),
            ],
            filename="icaro.xlsx",
            spreadsheet_key=None,
            upload_to_google_sheets=False,
        )


CtasCtesServiceDependency = Annotated[CtasCtesService, Depends()]
