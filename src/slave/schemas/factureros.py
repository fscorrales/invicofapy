__all__ = [
    "FacturerosReport",
    "FacturerosDocument",
    "FacturerosValidationOutput",
    "FacturerosParams",
    "FacturerosFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class FacturerosParams(BaseModel):
    pass


# -------------------------------------------------
class FacturerosReport(BaseModel):
    razon_social: str
    actividad: str
    partida: str


# -------------------------------------------------
class FacturerosDocument(FacturerosReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class FacturerosFilter(BaseFilterParams):
    razon_social: Optional[str] = None
    actividad: Optional[str] = None
    partida: Optional[str] = None


# -------------------------------------------------
class FacturerosValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[FacturerosDocument]
