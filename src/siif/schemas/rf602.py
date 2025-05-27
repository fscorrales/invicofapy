__all__ = ["Rf602", "StoredRf602", "Rf602ValidationOutput"]

from typing import List

from pydantic import BaseModel, NonNegativeFloat, Field
from pydantic_mongo import PydanticObjectId

from ...utils import ErrorsWithDocId


# -------------------------------------------------
class Rf602(BaseModel):
    ejercicio: str
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
class StoredRf602(FCI):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rf602ValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[StoredRf602]
