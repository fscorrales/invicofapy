__all__ = [
    "Rcocc31Report",
    "Rcocc31Document",
    "Rcocc31Params",
    "Rcocc31Filter",
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
class Rcocc31Params(BaseModel):
    ejercicio_from: int = Field(default=date.today().year, alias="ejercicioDesde")
    ejercicio_to: int = Field(default=date.today().year, alias="ejercicioHasta")
    cta_contable: str = Field(default="1112-2-6", alias="ctaContable")

    @field_validator("ejercicio_from", "ejercicio_to")
    @classmethod
    def validate_ejercicio_range(cls, v: int) -> int:
        current_year = date.today().year
        if not (2010 <= v <= current_year):
            raise ValueError(f"El ejercicio debe estar entre 2010 y {current_year}")
        return v

    @field_validator("cta_contable")
    @classmethod
    def validate_cta_contable(cls, v: str) -> str:
        parts = v.split("-")
        if len(parts) != 3:
            raise ValueError("ctaContable debe tener 3 partes separadas por '-'")

        if not (parts[0].isdigit() and len(parts[0]) == 4):
            raise ValueError("La primera parte de ctaContable debe tener 4 dígitos")

        if not (parts[1].isdigit() and 1 <= len(parts[1]) <= 2):
            raise ValueError("La segunda parte de ctaContable debe tener 1 o 2 dígitos")

        if not (parts[2].isdigit() and 1 <= len(parts[2]) <= 2):
            raise ValueError("La tercera parte de ctaContable debe tener 1 o 2 dígitos")

        return v

    @model_validator(mode="after")
    def check_range(self) -> "Rcocc31Params":
        if self.ejercicio_to < self.ejercicio_from:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class Rcocc31Report(BaseModel):
    ejercicio: int
    mes: str
    fecha: date
    fecha_aprobado: date
    cta_contable: str
    nro_entrada: str
    nro_original: str
    auxiliar_1: str
    auxiliar_2: str
    tipo_comprobante: str
    creditos: float
    debitos: float
    saldo: float


# -------------------------------------------------
class Rcocc31Document(Rcocc31Report):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rcocc31Filter(BaseFilterParams):
    ejercicio: Optional[int] = None
    mes: Optional[str] = None
    cta_contable: Optional[str] = None
    auxiliar_1: Optional[str] = None
    auxiliar_2: Optional[str] = None
    tipo_comprobante: Optional[str] = None
    nro_entrada: Optional[str] = None
    nro_original: Optional[str] = None
