__all__ = ["ResumenRendProvService", "ResumenRendProvServiceDependency"]

import asyncio
import getpass
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, List

from fastapi import Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError

from ...config import logger
from ...utils import (
    BaseFilterParams,
    RouteReturnSchema,
    get_download_sgf_path,
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
    ) -> RouteReturnSchema:
        """Downloads a report from SGF, processes it, validates the data,
        and stores it in MongoDB if valid.

        Args:
            ejercicio (int, optional): The fiscal year for the report. Defaults to the current year.

        Returns:
            RouteReturnSchema
        """
        return_schema = RouteReturnSchema()
        try:
            loop = asyncio.get_running_loop()
            validate_and_errors = await loop.run_in_executor(
                None,
                lambda: self._blocking_download_and_process(username, password, params),
            )
            # save_path = os.path.join(
            #     get_download_sgf_path(), "Resumen de Rendiciones SGF"
            # )
            # resumen_rend_prov = ResumenRendProv(sgf=conn)
            # resumen_rend_prov.download_report(
            #     dir_path=save_path,
            #     ejercicios=str(params.ejercicio),
            #     origenes=params.origen.value,
            # )
            # filename = (
            #     str(params.ejercicio)
            #     + " Resumen de Rendiciones "
            #     + params.origen
            #     + ".csv"
            # )
            # full_path = Path(os.path.join(save_path, filename))
            # logger.info(f"Usuario: {getpass.getuser()}")
            # logger.info(f"Working directory: {os.getcwd()}")
            # logger.info(f"Archivo esperado: {full_path}")
            # # Esperar hasta que el archivo exista, con timeout
            # for _ in range(10):  # Máximo 10 intentos (~5 segundos)
            #     if full_path.exists():
            #         break
            #     time.sleep(0.5)
            # else:
            #     raise FileNotFoundError(
            #         f"No se encontró el archivo descargado en: {full_path}"
            #     )

            # try:
            #     resumen_rend_prov.read_csv_file(full_path)
            # except FileNotFoundError as e:
            #     logger.error(f"No se pudo leer el archivo: {full_path}. Error: {e}")
            #     raise

            # resumen_rend_prov.process_dataframe()

            # # 🔹 Validar datos usando Pydantic
            # validate_and_errors = validate_and_extract_data_from_df(
            #     dataframe=resumen_rend_prov.clean_df,
            #     model=ResumenRendProvReport,
            #     field_id="libramiento_sgf",
            # )

            # 🔹 Si hay registros validados, eliminar los antiguos e insertar los nuevos
            if validate_and_errors.validated:
                logger.info(
                    f"Procesado origen {params.origen.value} para ejercicio {params.ejercicio}. Errores: {len(validate_and_errors.errors)}"
                )
                delete_dict = {
                    "ejercicio": params.ejercicio,
                    "origen": params.origen.value,
                }
                # Contar los instrumentos existentes antes de eliminarlos
                deleted_count = await self.repository.count_by_fields(delete_dict)
                await self.repository.delete_by_fields(delete_dict)
                # await self.collection.delete_many({"ejercicio": ejercicio})
                data_to_store = jsonable_encoder(validate_and_errors.validated)
                inserted_records = await self.repository.save_all(data_to_store)
                logger.info(
                    f"Inserted {len(inserted_records.inserted_ids)} records into MongoDB."
                )
                return_schema.deleted = deleted_count
                return_schema.added = len(data_to_store)
                return_schema.errors = validate_and_errors.errors

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

    # -------------------------------------------------
    def _blocking_download_and_process(self, username, password, params):
        save_path = os.path.join(get_download_sgf_path(), "Resumen de Rendiciones SGF")
        filename = (
            f"{params.ejercicio} Resumen de Rendiciones {params.origen.value}.csv"
        )
        full_path = Path(os.path.join(save_path, filename))

        with login(username, password) as conn:
            resumen_rend_prov = ResumenRendProv(sgf=conn)
            resumen_rend_prov.download_report(
                dir_path=save_path,
                ejercicios=str(params.ejercicio),
                origenes=params.origen.value,
            )

            logger.info(f"Usuario: {getpass.getuser()}")
            logger.info(f"Working directory: {os.getcwd()}")
            logger.info(f"Archivo esperado: {full_path}")
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
    async def get_resumen_rend_prov_from_db(
        self, params: BaseFilterParams
    ) -> List[ResumenRendProvDocument]:
        try:
            return await self.instruments.find_with_filter_params(params=params)
        except Exception as e:
            logger.error(
                f"Error retrieving SGF's Resumen Rend Prov. from database: {e}"
            )
            raise HTTPException(
                status_code=500,
                detail="Error retrieving SGF's Resumen Rend Prov. from the database",
            )


ResumenRendProvServiceDependency = Annotated[ResumenRendProvService, Depends()]


# class Rf602Service:
#     def __init__(self) -> None:
#         assert (collection_name := "siif_rf602") in settings.COLLECTIONS
#         self.collection = db[collection_name]
#         self.rf602 = Rf602()

#     async def download_and_update(
#         self, ejercicio: int = dt.datetime.now().year
#     ) -> Rf602ValidationOutput:
#         """Downloads a report from SIIF, processes it, validates the data,
#         and stores it in MongoDB if valid.

#         Args:
#             ejercicio (int, optional): The fiscal year for the report. Defaults to the current year.

#         Returns:
#             ValidationResultRf602: A structured response containing validated data and errors.
#         """
#         async with async_playwright() as p:
#             try:
#                 await self.rf602.login(
#                     username=settings.SIIF_USERNAME,
#                     password=settings.SIIF_PASSWORD,
#                     playwright=p,
#                     headless=False,
#                 )
#                 await self.rf602.go_to_reports()
#                 await self.rf602.go_to_specific_report()
#                 await self.rf602.download_report(ejercicio=str(ejercicio))
#                 await self.rf602.read_xls_file()
#                 df = await self.rf602.process_dataframe()

#                 # 🔹 Validar datos usando Pydantic
#                 validate_and_errors = validate_and_extract_data_from_df(
#                     dataframe=df, model=StoredRf602, field_id="estructura"
#                 )

#                 # 🔹 Si hay registros validados, eliminar los antiguos e insertar los nuevos
#                 if validate_and_errors.validated:
#                     await self.collection.delete_many({"ejercicio": ejercicio})
#                     validated_records = jsonable_encoder(validate_and_errors.validated)
#                     inserted_records = await self.collection.insert_many(
#                         validated_records
#                     )
#                     logger.info(
#                         f"Inserted {len(inserted_records.inserted_ids)} records into MongoDB."
#                     )

#             except ValidationError as e:
#                 logger.error(f"Validation Error: {e}")
#             except Exception as e:
#                 logger.error(f"Error during report processing: {e}")
#             finally:
#                 if hasattr(self.rf602, "logout"):
#                     await self.rf602.logout()
#         return validate_and_errors


# Rf602ServiceDependency = Annotated[Rf602Service, Depends()]
