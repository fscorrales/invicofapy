__all__ = [
    "Rcg01UejpReport",
    "Rcg01UejpDocument",
    "Rcg01UejpValidationOutput",
    "Rcg01UejpParams",
    "Rcg01UejpFilter",
]

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId
from .common import FuenteFinanciamientoSIIF


# --------------------------------------------------
class Rcg01UejpParams(BaseModel):
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
    def check_range(self) -> "Rcg01UejpParams":
        if self.ejercicio_to < self.ejercicio_from:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class Rcg01UejpReport(BaseModel):
    ejercicio: int
    mes: str
    fecha: date
    nro_comprobante: str
    importe: float
    fuente: FuenteFinanciamientoSIIF
    cta_cte: str
    cuit: str
    nro_expte: str
    nro_fondo: Optional[str] = None
    nro_entrada: Optional[str] = None
    nro_origen: Optional[str] = None
    clase_reg: str
    clase_mod: str
    clase_gto: str
    beneficiario: str
    es_comprometido: bool
    es_verificado: bool
    es_aprobado: bool
    es_pagado: bool


# -------------------------------------------------
class Rcg01UejpDocument(Rcg01UejpReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rcg01UejpFilter(BaseFilterParams):
    ejercicio: Optional[int] = None


# -------------------------------------------------
class Rcg01UejpValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[Rcg01UejpDocument]
