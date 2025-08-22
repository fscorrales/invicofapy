__all__ = [
    "BancoINVICOParams",
    "BancoINVICOReport",
    "BancoINVICODocument",
    "BancoINVICOFilter",
]


from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class BancoINVICOParams(BaseModel):
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
    def check_range(self) -> "BancoINVICOParams":
        if self.ejercicio_hasta < self.ejercicio_desde:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class BancoINVICOReport(BaseModel):
    ejercicio: int
    mes: str
    fecha: datetime
    cta_cte: str
    movimiento: Optional[str] = None
    es_cheque: bool
    beneficiario: Optional[str] = None
    importe: float
    concepto: Optional[str] = None
    moneda: Optional[str] = None
    libramiento: Optional[str] = None
    cod_imputacion: str
    imputacion: str


# -------------------------------------------------
class BancoINVICODocument(BancoINVICOReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class BancoINVICOFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    cta_cte: Optional[str] = None


# -------------------------------------------------
class BancoINVICOValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[BancoINVICODocument]
