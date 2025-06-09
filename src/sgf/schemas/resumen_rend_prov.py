__all__ = [
    "ResumenRendProvParams",
    "ResumenRendProvReport",
    "ResumenRendProvDocument",
]

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, NonNegativeFloat, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId
from .common import Origen


# --------------------------------------------------
class ResumenRendProvParams(BaseModel):
    origen: Origen


# -------------------------------------------------
class ResumenRendProvReport(BaseModel):
    ejercicio: int
    estructura: str
    fuente: str
    programa: str
    subprograma: str
    proyecto: str
    actividad: str
    grupo: str
    partida: str
    org: str
    credito_original: NonNegativeFloat
    credito_vigente: NonNegativeFloat
    comprometido: NonNegativeFloat
    ordenado: NonNegativeFloat
    saldo: float
    pendiente: float


# -------------------------------------------------
class ResumenRendProvDocument(ResumenRendProvReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ResumenRendProvFilter(BaseFilterParams):
    ejercicio: Optional[int] = None


# -------------------------------------------------
class ResumenRendProvValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ResumenRendProvDocument]
