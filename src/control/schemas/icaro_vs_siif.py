__all__ = [
    "ControlAnualReport",
    "ControlAnualDocument",
    "ControlAnualParams",
    "ControlAnualFilter",
]

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams


# --------------------------------------------------
class ControlAnualParams(BaseModel):
    ejercicio: int = date.today().year

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
class ControlAnualReport(BaseModel):
    ejercicio: int
    estructura: str
    fuente: int
    ejecucion_siif: float
    ejecucion_icaro: float
    diferencia: float
    desc_actividad: str
    desc_programa: str
    desc_subprograma: str
    desc_proyecto: str


# -------------------------------------------------
class ControlAnualDocument(ControlAnualReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ControlAnualFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None
