__all__ = ["Rf602Service", "Rf602ServiceDependency"]

import datetime as dt
from typing import Annotated

from fastapi import Depends
from fastapi.encoders import jsonable_encoder
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import settings, db, logger
from ...utils import validate_and_extract_data_from_df
from ..handlers import Rf602
from ..schemas import Rf602ValidationOutput, StoredRf602
from ..repositories import Rf602RepositoryDependency


# -------------------------------------------------
@dataclass
class Rf602Service:
    documents: Rf602RepositoryDependency

    # -------------------------------------------------
    async def sync_rf602_from_siif(self, username: str, password: str) -> List[FCI]:
        async with AsyncClient() as c:
            try:
                # Intentar obtener el token
                connect_iol = await get_token(username, password, httpxAsyncClient=c)
                # Intentar obtener el estado de cuenta
                documents = await get_fcis(iol=connect_iol, httpxAsyncClient=c)

                documents_to_store = [FCI(**document.model_dump()) for document in documents]

                await self.documents.delete_all()
                await self.documents.save_all(documents_to_store)

                return documents
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

    # -------------------------------------------------
    async def get_siif_from_db(self) -> List[StoredRf602]:
        try:
            return await self.documents.get_all(limit=100)
        except Exception as e:
            logger.error(f"Error retrieving SIIF's rf602 from database: {e}")
            raise HTTPException(
                status_code=500, detail="Error retrieving SIIF's rf602 from the database"
            )


Rf602ServiceDependency = Annotated[Rf602Service, Depends()]


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