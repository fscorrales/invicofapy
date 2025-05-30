__all__ = [
    "Rf602Report",
    "Rf602Document",
    "Rf602ValidationOutput",
    "Rf602Params",
    "Rf602Filter",
]

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, NonNegativeFloat, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class Rf602Params(BaseModel):
    ejercicio: int = Field(
        default_factory=lambda: date.today().year,
        alias="ejercicio",
        description="Año del ejercicio fiscal (entre 2010 y el año actual)",
        example=2025,
    )

    @field_validator("ejercicio")
    @classmethod
    def validate_value(cls, v):
        current_year = date.today().year
        if not (2010 <= v <= current_year):
            raise ValueError(f"Ejercicio debe estar entre 2010 y {current_year}")
        return v

    def __int__(self):
        return self.ejercicio


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
