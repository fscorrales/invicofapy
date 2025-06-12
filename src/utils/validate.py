__all__ = [
    "validate_and_extract_data_from_df",
    "ErrorsWithDocId",
    "PyObjectId",
    "validate_not_empty",
    "RouteReturnSchema",
]

from typing import Any, List

import pandas as pd
from bson import ObjectId
from pydantic import BaseModel, GetCoreSchemaHandler, ValidationError
from pydantic_core import core_schema


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
