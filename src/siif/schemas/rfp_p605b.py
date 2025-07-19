__all__ = [
    "RfpP605bReport",
    "RfpP605bDocument",
    "RfpP605bParams",
    "RfpP605bFilter",
]

from datetime import date
from typing import Optional

from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
)
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams


# --------------------------------------------------
class RfpP605bParams(BaseModel):
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
    def check_range(self) -> "RfpP605bParams":
        if self.ejercicio_to < self.ejercicio_from:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class RfpP605bReport(BaseModel):
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
    partida: str
    formulado: float


# -------------------------------------------------
class RfpP605bDocument(RfpP605bReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class RfpP605bFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
