__all__ = [
    "SaldosBarriosEvolucionReport",
    "SaldosBarriosEvolucionDocument",
    "SaldosBarriosEvolucionParams",
    "SaldosBarriosEvolucionFilter",
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
class SaldosBarriosEvolucionParams(BaseModel):
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
    def check_range(self) -> "SaldosBarriosEvolucionParams":
        if self.ejercicio_to < self.ejercicio_from:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class SaldosBarriosEvolucionReport(BaseModel):
    ejercicio: int
    cod_barrio: str
    barrio: str
    saldo_inicial: float
    amortizacion: float
    cambios: float
    saldo_final: float


# -------------------------------------------------
class SaldosBarriosEvolucionDocument(SaldosBarriosEvolucionReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class SaldosBarriosEvolucionFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    cod_barrio: Optional[str] = None
    barrio: Optional[str] = None
