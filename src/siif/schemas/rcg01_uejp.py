__all__ = [
    "Rcg01UejpReport",
    "Rcg01UejpDocument",
    "Rcg01UejpValidationOutput",
    "Rcg01UejpParams",
    "Rcg01UejpFilter",
]

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, NonNegativeFloat, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class Rcg01UejpParams(BaseModel):
    ejercicio: int = date.today().year
    # ejercicio: int = Field(
    #     default_factory=lambda: date.today().year,
    #     alias="ejercicio",
    #     description="Año del ejercicio fiscal (entre 2010 y el año actual)",
    #     example=2025,
    # )

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
class Rcg01UejpReport(BaseModel):
    ejercicio: int
    mes: str
    fecha: date
    nro_comprobante: str
    importe: float
    fuente: str
    cta_cte: str
    cuit: str
    nro_expte: str
    nro_fondo: str
    nro_entrada: str
    nro_origen: str
    clase_reg: str
    clase_mod: str
    clase_gto: str
    beneficiario: str
    es_comprometido: bool
    es_verificado: bool
    es_aprobado: bool
    es_pagado: bool


# -------------------------------------------------
class Rcg01UejpDocument(Rcg01Uejp2Report):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rcg01UejpFilter(BaseFilterParams):
    ejercicio: Optional[int] = None


# -------------------------------------------------
class Rcg01UejpValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[Rf602Document]
