__all__ = [
    "ReporteEjecucionGastosParams",
    "ReporteEjecucionGastosSyncParams",
]


from datetime import date
from typing import Optional

from pydantic import Field, field_validator, model_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, CamelModel


# --------------------------------------------------
class ReporteEjecucionGastosParams(CamelModel):
    ejercicio_desde: int = Field(default=date.today().year)
    ejercicio_hasta: int = Field(default=date.today().year)

    @field_validator("ejercicio_desde", "ejercicio_hasta")
    @classmethod
    def validate_ejercicio_range(cls, v: int) -> int:
        current_year = date.today().year
        if not (2010 <= v <= current_year):
            raise ValueError(f"El ejercicio debe estar entre 2010 y {current_year}")
        return v

    @model_validator(mode="after")
    def check_range(self) -> "ReporteEjecucionGastosParams":
        if self.ejercicio_hasta < self.ejercicio_desde:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self

# --------------------------------------------------
class ReporteEjecucionGastosSyncParams(ReporteEjecucionGastosParams):
    siif_username: Optional[str] = None
    siif_password: Optional[str] = None
