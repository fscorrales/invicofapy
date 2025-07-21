__all__ = [
    "ControlRecursosParams",
    "ControlRecursosSyncParams",
    "ControlRecursosReport",
    "ControlRecursosDocument",
    "ControlRecursosFilter",
]

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, CamelModel


# --------------------------------------------------
class ControlRecursosParams(CamelModel):
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


# --------------------------------------------------
class ControlRecursosSyncParams(ControlRecursosParams):
    siif_username: Optional[str] = None
    siif_password: Optional[str] = None
    sscc_username: Optional[str] = None
    sscc_password: Optional[str] = None


# -------------------------------------------------
class ControlRecursosReport(BaseModel):
    ejercicio: int
    mes: str
    cta_cte: str
    grupo: str
    recursos_siif: float
    depositos_banco: float


# -------------------------------------------------
class ControlRecursosDocument(ControlRecursosReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ControlRecursosFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None
