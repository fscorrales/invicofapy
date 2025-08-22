__all__ = ["BancoINVICOService", "BancoINVICOServiceDependency"]

import asyncio
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, List

import pandas as pd
from fastapi import Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ValidationError

from ...config import logger
from ...utils import (
    BaseFilterParams,
    RouteReturnSchema,
    ValidationResultSchema,
    export_dataframe_as_excel_response,
    get_download_sscc_path,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..handlers import BancoINVICO, login
from ..repositories import BancoINVICORepositoryDependency
from ..schemas import (
    BancoINVICODocument,
    BancoINVICOParams,
    BancoINVICOReport,
)


# -------------------------------------------------
class ValidateAndErrors(BaseModel):
    ejercicio: int
    validation_result: ValidationResultSchema


# -------------------------------------------------
@dataclass
class BancoINVICOService:
    repository: BancoINVICORepositoryDependency
    banco_invico: BancoINVICO = field(init=False)  # No se pasa como argumento

    # -------------------------------------------------
    def __post_init__(self):
        self.banco_invico = BancoINVICO()

    # -------------------------------------------------
    async def sync_banco_invico_from_sscc(
        self,
        username: str,
        password: str,
        params: BancoINVICOParams = None,
    ) -> List[RouteReturnSchema]:
        """Downloads a report from SSCC, processes it, validates the data,
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
            ejercicios = list(range(params.ejercicio_desde, params.ejercicio_hasta + 1))
            validate_list = await loop.run_in_executor(
                None,
                lambda: self._blocking_download_and_process_multi(username, password, ejercicios=ejercicios),
            )

            # 🔹 Si hay registros validados, eliminar los antiguos e insertar los nuevos
            for ejercicio, validate_and_errors in validate_list:
                if validate_and_errors.validated:
                    partial_schema = await sync_validated_to_repository(
                        repository=self.repository,
                        validation=validate_and_errors,
                        delete_filter={
                            "ejercicio": ejercicio,
                        },
                        title=f"SSCC Banco Invico del ejercicio {ejercicio}",
                        logger=logger,
                        label=f"Ejercicio {ejercicio} del SSCC Banco Invico",
                    )
                    return_schema.append(partial_schema)

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

    # -------------------------------------------------
    def _blocking_download_and_process(self, username, password, params):
        save_path = Path(
            os.path.join(get_download_sscc_path(), "Movimientos Generales SSCC")
        )
        filename = f"{params.ejercicio}  - Bancos - Consulta General de Movimientos.csv"
        full_path = Path(save_path / filename)

        with login(username, password) as conn:
            banco_invico = BancoINVICO(sscc=conn)
            banco_invico.download_report(
                dir_path=save_path,
                ejercicios=str(params.ejercicio),
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
                banco_invico.read_csv_file(full_path)
            except FileNotFoundError as e:
                logger.error(f"No se pudo leer el archivo: {full_path}. Error: {e}")
                raise

            banco_invico.process_dataframe()

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=banco_invico.clean_df,
                model=BancoINVICOReport,
            )
            return validate_and_errors

    # -------------------------------------------------
    @staticmethod
    def _blocking_download_and_process_multi(
        username: str,
        password: str,
        ejercicios: List[int],
    ) -> List[ValidateAndErrors]:
        results = []
        conn = login(username, password)
        if not isinstance(ejercicios, list):
            ejercicios = [ejercicios]
        try:
            banco_invico = BancoINVICO(sscc=conn)
            for ejercicio in ejercicios:
                try:
                    save_path = Path(
                        os.path.join(get_download_sscc_path(), "Movimientos Generales SSCC")
                    )
                    banco_invico.download_report(
                        dir_path=save_path,
                        ejercicios=str(ejercicio),
                    )
                    filename = f"{ejercicio} - Bancos - Consulta General de Movimientos.csv"
                    file_path = Path(os.path.join(save_path, filename))
                    for _ in range(10):
                        if file_path.exists():
                            break
                        time.sleep(0.5)
                    else:
                        raise FileNotFoundError(f"No se encontró archivo: {file_path}")

                    banco_invico.read_csv_file(file_path)
                    banco_invico.process_dataframe()

                    validation = validate_and_extract_data_from_df(
                        dataframe=banco_invico.clean_df,
                        model=BancoINVICOReport,
                        field_id="cod_imputacion",
                    )
                    results.append((ejercicio, validation))

                except Exception as e:
                    logger.error(f"Error procesando el ejercicio '{ejercicio}': {e}")
        finally:
            conn.quit()

        return results

    # -------------------------------------------------
    async def get_banco_invico_from_db(
        self, params: BaseFilterParams
    ) -> List[BancoINVICODocument]:
        return await self.repository.safe_find_with_filter_params(
            params=params,
            error_title="Error retrieving SSCC's Banco INVICO from the database",
        )

    # -------------------------------------------------
    async def sync_banco_invico_from_sqlite(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        # ✅ Validación temprana
        if not os.path.exists(sqlite_path):
            raise HTTPException(status_code=404, detail="Archivo SQLite no encontrado")

        return_schema = RouteReturnSchema()
        try:
            return_schema = await self.banco_invico.sync_validated_sqlite_to_repository(
                sqlite_path=sqlite_path
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

    # -------------------------------------------------
    async def export_banco_invico_from_db(
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
            filename=f"banco_invico_{ejercicio or 'all'}.xlsx",
            sheet_name="banco_invico",
        )


BancoINVICOServiceDependency = Annotated[BancoINVICOService, Depends()]
