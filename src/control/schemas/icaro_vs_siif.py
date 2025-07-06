__all__ = [
    "ControlCompletoParams",
    "ControlAnualReport",
    "ControlAnualDocument",
    "ControlAnualFilter",
    "ControlComprobantesReport",
    "ControlComprobantesDocument",
    "ControlComprobantesFilter",
]

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams


# --------------------------------------------------
class ControlCompletoParams(BaseModel):
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
class ControlAnualReport(BaseModel):
    ejercicio: int
    estructura: str
    fuente: int
    ejecucion_siif: float
    ejecucion_icaro: float
    diferencia: float
    desc_actividad: str
    desc_programa: str
    desc_subprograma: str
    desc_proyecto: str


# -------------------------------------------------
class ControlAnualDocument(ControlAnualReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ControlAnualFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None


# -------------------------------------------------
class ControlComprobantesReport(BaseModel):
    ejercicio: int
    siif_nro: str
    icaro_nro: str
    err_nro: bool
    siif_tipo: str
    icaro_tipo: str
    err_tipo: bool
    siif_fuente: str
    icaro_fuente: str
    err_fuente: bool
    siif_importe: float
    icaro_importe: float
    err_importe: bool
    siif_mes: str
    icaro_mes: str
    err_mes: bool
    siif_cta_cte: str
    icaro_cta_cte: str
    err_cta_cte: bool
    siif_cuit: str
    icaro_cuit: str
    err_cuit: bool
    siif_partida: str
    icaro_partida: str
    err_partida: bool


# -------------------------------------------------
class ControlComprobantesDocument(ControlAnualReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ControlComprobantesFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None
