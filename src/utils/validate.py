__all__ = ["validate_and_extract_data_from_df", "ErrorsWithDocId"]

from typing import List

import pandas as pd
from pydantic import BaseModel, ValidationError


class ErrorsDetails(BaseModel):
    loc: str
    msg: str
    error_type: str


class ErrorsWithDocId(BaseModel):
    doc_id: str
    details: List[ErrorsDetails]


class ValidationResultSchema(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[BaseModel]  # 🔹 Lista de modelos Pydantic


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
