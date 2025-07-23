__all__ = [
    "validate_and_extract_data_from_df",
    "ErrorsWithDocId",
    "PyObjectId",
    "validate_not_empty",
    "RouteReturnSchema",
    "sync_validated_to_repository",
    "validate_excel_file",
    "ValidationResultSchema",
]

import argparse
import os
from typing import Any, List, Optional

import pandas as pd
from bson import ObjectId
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, GetCoreSchemaHandler, ValidationError
from pydantic_core import core_schema

from .safe_get import sanitize_dataframe_for_json


# -------------------------------------------------
class ErrorsDetails(BaseModel):
    loc: str
    msg: str
    error_type: str


# -------------------------------------------------
class ErrorsWithDocId(BaseModel):
    doc_id: str
    details: List[ErrorsDetails]


# -------------------------------------------------
class ValidationResultSchema(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[BaseModel]  # 🔹 Lista de modelos Pydantic


# -------------------------------------------------
class RouteReturnSchema(BaseModel):
    title: Optional[str] = None
    deleted: int = 0
    added: int = 0
    errors: List[ErrorsWithDocId] = []


# -------------------------------------------------
def validate_and_extract_data_from_df(
    dataframe: pd.DataFrame, model: BaseModel, field_id: str = "doc_id"
) -> ValidationResultSchema:
    """Validates and extracts data from a pandas DataFrame using a Pydantic model.

    This function iterates over the DataFrame, validates each row against the specified
    Pydantic model, and categorizes the results into valid data and errors.

    Args:
        dataframe (pd.DataFrame): The DataFrame containing the data to validate.
        model (BaseModel): The Pydantic model used for validation.
        field_id (str, optional): The column name that identifies each document in the error list. Defaults to "doc_id".

    Returns:
        ValidationResultSchema: A dictionary-like object containing:
            - `errors` (List[ErrorsWithDocId]): A list of records that failed validation, including their error details.
            - `validated` (List[BaseModel]): A list of validated records that passed the model validation.
    """
    errors_list: List[ErrorsWithDocId] = []
    validated_list: List[model] = []
    # duplicates = dataframe.columns[dataframe.columns.duplicated()]
    # print("Columnas duplicadas:", duplicates)
    dataframe = sanitize_dataframe_for_json(dataframe)
    df_dict = dataframe.to_dict(orient="records")
    for record in df_dict:
        try:
            validated_doc = model.model_validate(record)
            validated_list.append(
                validated_doc
            )  # 🔹 No es necesario hacer `model_dump()`
        except ValidationError as e:
            doc_id = str(record.get(field_id, "unknown"))  # 🔹 Evita `None` en el ID
            error_details = [
                ErrorsDetails(
                    loc=str(err["loc"]), msg=err["msg"], error_type=err["type"]
                )
                for err in e.errors()
            ]
            errors_list.append(ErrorsWithDocId(doc_id=doc_id, details=error_details))
    return ValidationResultSchema(errors=errors_list, validated=validated_list)


# --------------------------------------------------
async def sync_validated_to_repository(
    repository,
    validation: ValidationResultSchema,
    delete_filter: Optional[dict] = None,
    title: Optional[str] = None,
    logger: Optional[object] = None,
    label: str = "document",
) -> RouteReturnSchema:
    """
    Sincroniza datos validados con MongoDB: borra registros antiguos e inserta los nuevos.

    Args:
        repository: Repositorio que implementa delete_by_fields, count_by_fields y save_all.
        validation (ValidationResultSchema): Resultado de la validación con errores y validados.
        delete_filter (Optional[dict]): Filtro para eliminar registros previos.
        title (Optional[str]): Título para el resumen de la operación.
        logger (Optional[object]): Logger para trazar acciones opcionalmente.
        label (str): Etiqueta para identificar el conjunto de datos en los logs.

    Returns:
        RouteReturnSchema: Resumen con cantidades insertadas, eliminadas y errores.
    """

    schema = RouteReturnSchema()

    if validation.validated:
        if logger:
            logger.info(
                f"Procesando {label}. Registros válidos: {len(validation.validated)}. "
                f"Errores: {len(validation.errors)}"
            )

        if not delete_filter:
            deleted_count = await repository.delete_all()
        else:
            deleted_count = await repository.count_by_fields(delete_filter)
            await repository.delete_by_fields(delete_filter)

        docs = jsonable_encoder(validation.validated)
        inserted = await repository.save_all(docs)

        if logger:
            logger.info(
                f"{label} → Eliminados: {deleted_count} | Insertados: {len(inserted.inserted_ids)}"
            )

        schema.title = title
        schema.deleted += deleted_count
        schema.added += len(docs)
        schema.errors += validation.errors

    return schema


# -------------------------------------------------
def validate_not_empty(field: str) -> str:
    if not field:
        raise ValueError("Field cannot be empty or zero")
    return field


# -------------------------------------------------
class PyObjectId(ObjectId):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return core_schema.json_or_python_schema(
            python_schema=core_schema.with_info_plain_validator_function(cls.validate),
            json_schema=core_schema.with_info_plain_validator_function(cls.validate),
            serialization=core_schema.plain_serializer_function_ser_schema(str),
        )

    @classmethod
    def validate(cls, v, _info):
        if isinstance(v, ObjectId):
            return v
        if ObjectId.is_valid(v):
            return ObjectId(v)
        raise ValueError("Invalid ObjectId")


def validate_excel_file(path):
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"El archivo {path} no existe")
    if not path.endswith(".xlsx") and not path.endswith(".xls"):
        raise argparse.ArgumentTypeError(
            f"El archivo {path} no parece ser un archivo Excel"
        )
    try:
        pd.read_excel(path, nrows=1)  # Solo intenta leer la primera fila
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Error al abrir el archivo Excel {path}: {e}")
    return path
