__all__ = [
    "FuentesReport",
    "FuentesDocument",
    "FuentesValidationOutput",
    "FuentesParams",
    "FuentesFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class FuentesParams(BaseModel):
    pass


# -------------------------------------------------
class FuentesReport(BaseModel):
    fuente: str
    desc_fuente: str
    abreviatura: str


# -------------------------------------------------
class FuentesDocument(FuentesReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class FuentesFilter(BaseFilterParams):
    nro_subprog: Optional[str] = None
    desc_subprog: Optional[str] = None


# -------------------------------------------------
class FuentesValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[FuentesDocument]
