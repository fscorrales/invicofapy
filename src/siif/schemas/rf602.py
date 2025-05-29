__all__ = ["Rf602", "Rf602Document", "Rf602ValidationOutput", "Rf602Params", "Rf602Filter"]

from typing import List, Optional

from pydantic import BaseModel, NonNegativeFloat, Field
from pydantic_mongo import PydanticObjectId

from ...utils import ErrorsWithDocId, BaseFilterParams


# --------------------------------------------------
class Rf602Params(BaseModel):
    ejercicio: Optional[str] = None


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
class Rf602Document(Rf602):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rf602Filter(BaseFilterParams):
    ejercicio: Optional[int] = None

# -------------------------------------------------
class Rf602ValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[Rf602Document]


