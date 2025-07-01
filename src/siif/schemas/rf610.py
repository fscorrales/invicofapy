__all__ = [
    "Rf610Report",
    "Rf610Document",
    "Rf610ValidationOutput",
    "Rf610Params",
    "Rf610Filter",
]

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, NonNegativeFloat, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class Rf610Params(BaseModel):
    ejercicio: int = date.today().year
    # ejercicio: int = Field(
    #     default_factory=lambda: date.today().year,
    #     alias="ejercicio",
    #     description="Año del ejercicio fiscal (entre 2010 y el año actual)",
    #     example=2025,
    # )

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
class Rf610Report(BaseModel):
    ejercicio: int
    estructura: str
    programa: str
    desc_programa: str
    subprograma: str
    desc_subprograma: str
    proyecto: str
    desc_proyecto: str
    actividad: str
    desc_actividad: str
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
