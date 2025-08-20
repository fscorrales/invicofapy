__all__ = [
    "ResumenRendProvParams",
    "ResumenRendProvReport",
    "ResumenRendProvDocument",
    "ResumenRendProvFilter",
]


from datetime import date, datetime
from typing import List, Optional

from pydantic import (
    BaseModel,
    Field,
    NonNegativeFloat,
    field_validator,
    model_validator,
)
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, CamelModel, ErrorsWithDocId
from .common import Origen


# --------------------------------------------------
class ResumenRendProvParams(CamelModel):
    ejercicio_desde: int = Field(default=date.today().year)
    ejercicio_hasta: int = Field(default=date.today().year)
    origen: Origen

    @field_validator("ejercicio_desde", "ejercicio_hasta")
    @classmethod
    def validate_ejercicio_range(cls, v: int) -> int:
        current_year = date.today().year
        if not (2010 <= v <= current_year):
            raise ValueError(f"El ejercicio debe estar entre 2010 y {current_year}")
        return v

    @model_validator(mode="after")
    def check_range(self) -> "ResumenRendProvParams":
        if self.ejercicio_desde < self.ejercicio_hasta:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class ResumenRendProvReport(BaseModel):
    origen: Origen
    ejercicio: int
    mes: str
    fecha: datetime
    beneficiario: str
    destino: str
    libramiento_sgf: str
    movimiento: str
    cta_cte: str
    importe_bruto: NonNegativeFloat
    gcias: NonNegativeFloat
    sellos: NonNegativeFloat
    iibb: NonNegativeFloat
    suss: NonNegativeFloat
    invico: NonNegativeFloat
    seguro: NonNegativeFloat
    salud: NonNegativeFloat
    mutual: NonNegativeFloat
    otras: NonNegativeFloat
    retenciones: NonNegativeFloat
    importe_neto: NonNegativeFloat


# -------------------------------------------------
class ResumenRendProvDocument(ResumenRendProvReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ResumenRendProvFilter(BaseFilterParams):
    origen: Optional[Origen] = None
    ejercicio: Optional[int] = None
    beneficiario: Optional[str] = None
    cta_cte: Optional[str] = None


# -------------------------------------------------
class ResumenRendProvValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ResumenRendProvDocument]
