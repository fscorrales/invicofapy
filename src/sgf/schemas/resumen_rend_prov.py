__all__ = [
    "ResumenRendProvParams",
    "ResumenRendProvReport",
    "ResumenRendProvDocument",
    "ResumenRendProvFilter",
]


from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, NonNegativeFloat, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId
from .common import Origen


# --------------------------------------------------
class ResumenRendProvParams(BaseModel):
    origen: Origen
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
class ResumenRendProvReport(BaseModel):
    origen: Origen
    ejercicio: int
    mes: str
    fecha: date
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
    ejercicio: Optional[int] = None


# -------------------------------------------------
class ResumenRendProvValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ResumenRendProvDocument]
