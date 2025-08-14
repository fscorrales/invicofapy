__all__ = [
    "Rf610Report",
    "Rf610Document",
    "Rf610ValidationOutput",
    "Rf610Params",
    "Rf610Filter",
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


# --------------------------------------------------
class Rf610Params(BaseModel):
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
    def check_range(self) -> "Rf610Params":
        if self.ejercicio_to < self.ejercicio_from:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class Rf610Report(BaseModel):
    ejercicio: int
    estructura: str
    programa: str
    desc_programa: Optional[str] = None
    subprograma: str
    desc_subprograma: Optional[str] = None
    proyecto: str
    desc_proyecto: Optional[str] = None
    actividad: str
    desc_actividad: Optional[str] = None
    grupo: str
    desc_grupo: str
    partida: str
    desc_partida: str
    credito_original: NonNegativeFloat
    credito_vigente: NonNegativeFloat
    comprometido: NonNegativeFloat
    ordenado: NonNegativeFloat
    saldo: float


# -------------------------------------------------
class Rf610Document(Rf610Report):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rf610Filter(BaseFilterParams):
    ejercicio: Optional[int] = None


# -------------------------------------------------
class Rf610ValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[Rf610Document]
