__all__ = ["ResumenRendProvService", "ResumenRendProvServiceDependency"]

import asyncio
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, List

import pandas as pd
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ValidationError

from ...config import logger
from ...utils import (
    BaseFilterParams,
    RouteReturnSchema,
    ValidationResultSchema,
    export_dataframe_as_excel_response,
    get_download_sgf_path,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..handlers import ResumenRendProv, login
from ..repositories import ResumenRendProvRepositoryDependency
from ..schemas import (
    ResumenRendProvDocument,
    ResumenRendProvParams,
    ResumenRendProvReport,
)


# -------------------------------------------------
class ValidateAndErrors(BaseModel):
    origen: str
    validation_result: ValidationResultSchema


# -------------------------------------------------
@dataclass
class ResumenRendProvService:
    repository: ResumenRendProvRepositoryDependency
    resumen_rend_prov: ResumenRendProv = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    async def sync_resumen_rend_prov_from_sgf(
        self,
        username: str,
        password: str,
        params: ResumenRendProvParams = None,
    ) -> List[RouteReturnSchema]:
        """Downloads a report from SGF, processes it, validates the data,
        and stores it in MongoDB if valid.

        Args:
            ejercicio (int, optional): The fiscal year for the report. Defaults to the current year.

        Returns:
            RouteReturnSchema
        """
        if username is None or password is None:
            raise HTTPException(
                status_code=401,
                detail="Missing username or password",
            )
        return_schema = []
        try:
            loop = asyncio.get_running_loop()
            origenes = params.origenes or [params.origen.value]  # adaptar según schema
            validate_list = await loop.run_in_executor(
                None,
                lambda: self._blocking_download_and_process_multi(
                    username,
                    password,
                    ejercicio=params.ejercicio,
                    origenes=origenes,
                ),
            )

            # 🔹 Si hay registros validados, eliminar los antiguos e insertar los nuevos
            for origen, validate_and_errors in validate_list:
                if validate_and_errors.validated:
                    partial_schema = await sync_validated_to_repository(
                        repository=self.repository,
                        validation=validate_and_errors,
                        delete_filter={
                            "ejercicio": params.ejercicio,
                            "origen": origen,
                        },
                        title=f"SGF Resumen Rend Prov. del origen {origen} y ejercicio {params.ejercicio}",
                        logger=logger,
                        label=f"origin {origen} del ejercicio {params.ejercicio} del SGF Resumen Rend Prov",
                    )
                    return_schema.append(partial_schema)
                    # logger.info(
                    #     f"Procesado origen {origen} para ejercicio {params.ejercicio}. Errores: {len(validate_and_errors.errors)}"
                    # )
                    # delete_dict = {
                    #     "ejercicio": params.ejercicio,
                    #     "origen": origen,
                    # }
                    # # Contar los documentos existentes antes de eliminarlos
                    # deleted_count = await self.repository.count_by_fields(delete_dict)
                    # await self.repository.delete_by_fields(delete_dict)
                    # # await self.collection.delete_many({"ejercicio": ejercicio})
                    # data_to_store = jsonable_encoder(validate_and_errors.validated)
                    # inserted_records = await self.repository.save_all(data_to_store)
                    # logger.info(
                    #     f"Inserted {len(inserted_records.inserted_ids)} records into MongoDB."
                    # )
                    # return_schema.deleted += deleted_count
                    # return_schema.added += len(data_to_store)
                    # return_schema.errors += validate_and_errors.errors

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400, detail="Invalid response format from SGF"
            )
        except Exception as e:
            logger.error(f"Error during report processing: {e}")
            raise HTTPException(
                status_code=401,
                detail="Invalid credentials or unable to authenticate",
            )
        finally:
            return return_schema

    # -------------------------------------------------
    def _blocking_download_and_process(self, username, password, params):
        save_path = Path(
            os.path.join(get_download_sgf_path(), "Resumen de Rendiciones SGF")
        )
        filename = (
            f"{params.ejercicio} Resumen de Rendiciones {params.origen.value}.csv"
        )
        full_path = Path(save_path / filename)

        with login(username, password) as conn:
            resumen_rend_prov = ResumenRendProv(sgf=conn)
            resumen_rend_prov.download_report(
                dir_path=save_path,
                ejercicios=str(params.ejercicio),
                origenes=params.origen.value,
            )

            # Esperar hasta que el archivo exista, con timeout
            for _ in range(10):  # Máximo 10 intentos (~5 segundos)
                if full_path.exists():
                    break
                time.sleep(0.5)
            else:
                raise FileNotFoundError(
                    f"No se encontró el archivo descargado en: {full_path}"
                )

            try:
                resumen_rend_prov.read_csv_file(full_path)
            except FileNotFoundError as e:
                logger.error(f"No se pudo leer el archivo: {full_path}. Error: {e}")
                raise

            resumen_rend_prov.process_dataframe()

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=resumen_rend_prov.clean_df,
                model=ResumenRendProvReport,
                field_id="libramiento_sgf",
            )
            return validate_and_errors

    # -------------------------------------------------
    def _blocking_download_and_process_multi(
        username: str,
        password: str,
        ejercicio: int,
        origenes: List[str],
    ) -> List[ValidateAndErrors]:
        results = []
        conn = login(username, password)

        try:
            resumen_rend_prov = ResumenRendProv(sgf=conn)
            for origen in origenes:
                try:
                    resumen_rend_prov.download_report(
                        dir_path=Path(
                            os.path.join(
                                get_download_sgf_path(), "Resumen de Rendiciones SGF"
                            )
                        ),
                        ejercicios=str(ejercicio),
                        origenes=origen,
                    )

                    file_path = Path(f"{ejercicio} Resumen de Rendiciones {origen}.csv")
                    for _ in range(10):
                        if file_path.exists():
                            break
                        time.sleep(0.5)
                    else:
                        raise FileNotFoundError(f"No se encontró archivo: {file_path}")

                    resumen_rend_prov.read_csv_file(file_path)
                    resumen_rend_prov.process_dataframe()

                    validation = validate_and_extract_data_from_df(
                        dataframe=resumen_rend_prov.clean_df,
                        model=ResumenRendProvReport,
                        field_id="libramiento_sgf",
                    )
                    results.append((origen, validation))

                except Exception as e:
                    logger.error(f"Error procesando origen '{origen}': {e}")
        finally:
            conn.quit()

        return results

    # -------------------------------------------------
    async def get_resumen_rend_prov_from_db(
        self, params: BaseFilterParams
    ) -> List[ResumenRendProvDocument]:
        return await self.repository.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving SGF's Resumen Rend Prov. from database",
        )

    # -------------------------------------------------
    async def sync_resumen_rend_prov_from_sqlite(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = (
                await self.resumen_rend_prov.sync_validated_sqlite_to_repository(
                    sqlite_path=sqlite_path
                )
            )
        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400, detail="Invalid response format from SGF"
            )
        except Exception as e:
            logger.error(f"Error during report processing: {e}")
            raise HTTPException(
                status_code=401,
                detail="Invalid credentials or unable to authenticate",
            )
        finally:
            return return_schema

    # -------------------------------------------------
    async def export_resumen_rend_prov_from_db(
        self, ejercicio: int = None
    ) -> StreamingResponse:
        if ejercicio is not None:
            docs = await self.repository.get_by_fields({"ejercicio": ejercicio})
        else:
            docs = await self.repository.get_all()

        if not docs:
            raise HTTPException(status_code=404, detail="No se encontraron registros")
        df = pd.DataFrame(docs)

        return export_dataframe_as_excel_response(
            df,
            filename=f"sgf_resumen_rend_prov_{ejercicio or 'all'}.xlsx",
            sheet_name="resumen_rend_prov",
        )


ResumenRendProvServiceDependency = Annotated[ResumenRendProvService, Depends()]
