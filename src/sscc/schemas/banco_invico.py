__all__ = [
    "BancoINVICOParams",
    "BancoINVICOReport",
    "BancoINVICODocument",
    "BancoINVICOFilter",
]


from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class BancoINVICOParams(BaseModel):
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
class BancoINVICOReport(BaseModel):
    ejercicio: int
    mes: str
    fecha: date
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
