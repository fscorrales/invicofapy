__all__ = [
    "Ri102Report",
    "Ri102Document",
    "Ri102Params",
    "Ri102Filter",
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
class Ri102Params(BaseModel):
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
    def check_range(self) -> "Ri102Params":
        if self.ejercicio_to < self.ejercicio_from:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class Ri102Report(BaseModel):
    ejercicio: int
    tipo: str
    clase: str
    cod_recurso: str
    desc_recurso: str
    fuente: str
    org_fin: str
    ppto_inicial: float
    ppto_modif: float
    ppto_vigente: float
    ingreso: float
    saldo: float


# -------------------------------------------------
class Ri102Document(Ri102Report):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Ri102Filter(BaseFilterParams):
    ejercicio: Optional[int] = None
