__all__ = ["IcaroService", "IcaroServiceDependency"]

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
    ProgramasRepositoryDependency,
    SubprogramasRepositoryDependency,
    ProyectosRepositoryDependency,
    ActividadesRepositoryDependency,
    FuentesRepositoryDependency,
    ObrasRepositoryDependency,
    PartidasRepositoryDependency,
    ProveedoresRepositoryDependency,
    ResumenRendObrasRepositoryDependency,
    RetencionesRepositoryDependency,
)
from ..schemas import (
    CargaDocument,
    CertificadosDocument,
    CtasCtesDocument,
    EstructurasDocument,
    ProgramasDocument,
    SubprogramasDocument,
    ProyectosDocument,
    ActividadesDocument,
    FuentesDocument,
    ObrasDocument,
    PartidasDocument,
    ProveedoresDocument,
    ResumenRendObrasDocument,
    RetencionesDocument,
)


# -------------------------------------------------
@dataclass
class IcaroService:
    obras_repo: ObrasRepositoryDependency
    carga_repo: CargaRepositoryDependency
    certificados_repo: CertificadosRepositoryDependency
    ctas_ctes_repo: CtasCtesRepositoryDependency
    estructuras_repo: EstructurasRepositoryDependency
    programas_repo: ProgramasRepositoryDependency
    subprogramas_repo: SubprogramasRepositoryDependency
    proyectos_repo: ProyectosRepositoryDependency
    actividades_repo: ActividadesRepositoryDependency
    fuentes_repo: FuentesRepositoryDependency
    partidas_repo: PartidasRepositoryDependency
    proveedores_repo: ProveedoresRepositoryDependency
    resumen_rend_obras_repo: ResumenRendObrasRepositoryDependency
    retenciones_repo: RetencionesRepositoryDependency

    # -------------------------------------------------
    async def get_obras_from_db(self, params: BaseFilterParams) -> List[ObrasDocument]:
        return await self.obras_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Obras from the database",
        )

    # -------------------------------------------------
    async def get_carga_from_db(self, params: BaseFilterParams) -> List[CargaDocument]:
        return await self.carga_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Carga from the database",
        )

    # -------------------------------------------------
    async def get_certificados_from_db(
        self, params: BaseFilterParams
    ) -> List[CertificadosDocument]:
        return await self.certificados_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Certificados from the database",
        )

    # -------------------------------------------------
    async def get_ctas_ctes_from_db(
        self, params: BaseFilterParams
    ) -> List[CtasCtesDocument]:
        return await self.ctas_ctes_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving CtasCtes from the database",
        )

    # -------------------------------------------------
    async def get_estructuras_from_db(
        self, params: BaseFilterParams
    ) -> List[EstructurasDocument]:
        return await self.estructuras_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Estructuras from the database",
        )

    # -------------------------------------------------
    async def get_programas_from_db(
        self, params: BaseFilterParams
    ) -> List[ProgramasDocument]:
        return await self.programas_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Programas from the database",
        )

    # -------------------------------------------------
    async def get_subprogramas_from_db(
        self, params: BaseFilterParams
    ) -> List[SubprogramasDocument]:
        return await self.subprogramas_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Subprogramas from the database",
        )

    # -------------------------------------------------
    async def get_proyectos_from_db(
        self, params: BaseFilterParams
    ) -> List[ProyectosDocument]:
        return await self.proyectos_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Proyectos from the database",
        )

    # -------------------------------------------------
    async def get_actividades_from_db(
        self, params: BaseFilterParams
    ) -> List[ActividadesDocument]:
        return await self.actividades_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Actividades from the database",
        )

    # -------------------------------------------------
    async def get_fuentes_from_db(
        self, params: BaseFilterParams
    ) -> List[FuentesDocument]:
        return await self.fuentes_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Fuentes from the database",
        )

    # -------------------------------------------------
    async def get_partidas_from_db(
        self, params: BaseFilterParams
    ) -> List[PartidasDocument]:
        return await self.partidas_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Partidas from the database",
        )

    # -------------------------------------------------
    async def get_proveedores_from_db(
        self, params: BaseFilterParams
    ) -> List[ProveedoresDocument]:
        return await self.proveedores_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Proveedores from the database",
        )

    # -------------------------------------------------
    async def get_resumen_rend_obras_from_db(
        self, params: BaseFilterParams
    ) -> List[ResumenRendObrasDocument]:
        return await self.resumen_rend_obras_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving ResumenRendObras from the database",
        )

    # -------------------------------------------------
    async def get_retenciones_from_db(
        self, params: BaseFilterParams
    ) -> List[RetencionesDocument]:
        return await self.retenciones_repo.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving Retenciones from the database",
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
        programas_docs = await self.programas_repo.get_all()
        subprogramas_docs = await self.subprogramas_repo.get_all()
        proyectos_docs = await self.proyectos_repo.get_all()
        actividades_docs = await self.actividades_repo.get_all()
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
            and not programas_docs
            and not subprogramas_docs
            and not proyectos_docs
            and not actividades_docs
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
                (pd.DataFrame(programas_docs), "programas"),
                (pd.DataFrame(subprogramas_docs), "subprogramas"),
                (pd.DataFrame(proyectos_docs), "proyectos"),
                (pd.DataFrame(actividades_docs), "actividades"),
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


IcaroServiceDependency = Annotated[IcaroService, Depends()]
