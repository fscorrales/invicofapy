__all__ = [
    "Rci02Report",
    "Rci02Document",
    "Rci02Params",
    "Rci02Filter",
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
class Rci02Params(BaseModel):
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
    def check_range(self) -> "Rci02Params":
        if self.ejercicio_to < self.ejercicio_from:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class Rci02Report(BaseModel):
    ejercicio: int
    mes: str
    fecha: date
    fuente: str
    cta_cte: str
    nro_entrada: str
    importe: float
    glosa: str
    es_remanente: bool
    es_invico: bool
    es_verificado: bool
    clase_reg: str
    clase_mod: str


# -------------------------------------------------
class Rci02Document(Rci02Report):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rci02Filter(BaseFilterParams):
    ejercicio: Optional[int] = None
