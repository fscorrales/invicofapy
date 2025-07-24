__all__ = [
    "Rvicon03Report",
    "Rvicon03Document",
    "Rvicon03Params",
    "Rvicon03Filter",
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
class Rvicon03Params(BaseModel):
    ejercicio_from: int = Field(default=date.today().year, alias="ejercicioDesde")
    ejercicio_to: int = Field(default=date.today().year, alias="ejercicioHasta")

    @field_validator("ejercicio_from", "ejercicio_to")
    @classmethod
    def validate_ejercicio_range(cls, v: int) -> int:
        current_year = date.today().year
        if not (2010 <= v <= current_year):
            raise ValueError(f"El ejercicio debe estar entre 2010 y {current_year}")
        return v

    @model_validator(mode="after")
    def check_range(self) -> "Rvicon03Params":
        if self.ejercicio_to < self.ejercicio_from:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class Rvicon03Report(BaseModel):
    ejercicio: int
    nivel: str
    desc_nivel: str
    cta_contable: str
    desc_cta_contable: str
    saldo_inicial: float
    debe: float
    haber: float
    ajuste_debe: float
    ajuste_haber: float
    fondos_debe: float
    fondos_haber: float
    saldo_final: float


# -------------------------------------------------
class Rvicon03Document(Rvicon03Report):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rvicon03Filter(BaseFilterParams):
    ejercicio: Optional[int] = None
    nivel: Optional[str] = None
    cta_contable: Optional[str] = None
