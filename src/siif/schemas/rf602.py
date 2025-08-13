__all__ = [
    "Rf602Report",
    "Rf602Document",
    "Rf602ValidationOutput",
    "Rf602Params",
    "Rf602Filter",
]

from datetime import date
from typing import List, Optional

from pydantic import (
    BaseModel,
    Field,
    NonNegativeFloat,
    field_validator,
    model_validator,
)
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId
from .common import FuenteFinanciamientoSIIF


# --------------------------------------------------
class Rf602Params(BaseModel):
    ejercicio_from: int = Field(default=date.today().year, alias="ejercicioDesde")
    ejercicio_to: int = Field(default=date.today().year, alias="ejercicioHasta")
    # ejercicio_from: int = date.today().year
    # ejercicio_to: int = date.today().year

    @field_validator("ejercicio_from", "ejercicio_to")
    @classmethod
    def validate_ejercicio_range(cls, v: int) -> int:
        current_year = date.today().year
        if not (2010 <= v <= current_year):
            raise ValueError(f"El ejercicio debe estar entre 2010 y {current_year}")
        return v

    @model_validator(mode="after")
    def check_range(self) -> "Rf602Params":
        if self.ejercicio_to < self.ejercicio_from:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class Rf602Report(BaseModel):
    ejercicio: int
    estructura: str
    fuente: FuenteFinanciamientoSIIF
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
