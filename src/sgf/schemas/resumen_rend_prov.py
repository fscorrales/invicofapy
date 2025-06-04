__all__ = [
    "ResumenRendProvParams",
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
class Rf602Report(BaseModel):
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
class Rf602Document(Rf602Report):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rf602Filter(BaseFilterParams):
    ejercicio: Optional[int] = None


# -------------------------------------------------
class Rf602ValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[Rf602Document]
