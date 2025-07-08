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
    estructura: Optional[str] = None
    fuente: int
    ejecucion_siif: float
    ejecucion_icaro: float
    diferencia: float
    desc_actividad: Optional[str] = None
    desc_programa: Optional[str] = None
    desc_subprograma: Optional[str] = None
    desc_proyecto: Optional[str] = None


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
    siif_nro: Optional[str] = None
    icaro_nro: Optional[str] = None
    err_nro: bool
    siif_tipo: Optional[str] = None
    icaro_tipo: Optional[str] = None
    err_tipo: bool
    siif_fuente: Optional[str] = None
    icaro_fuente: Optional[str] = None
    err_fuente: bool
    siif_importe: Optional[float] = None
    icaro_importe: Optional[float] = None
    err_importe: bool
    siif_mes: Optional[str] = None
    icaro_mes: Optional[str] = None
    err_mes: bool
    siif_cta_cte: Optional[str] = None
    icaro_cta_cte: Optional[str] = None
    err_cta_cte: bool
    siif_cuit: Optional[str] = None
    icaro_cuit: Optional[str] = None
    err_cuit: bool
    siif_partida: Optional[str] = None
    icaro_partida: Optional[str] = None
    err_partida: bool


# -------------------------------------------------
class ControlComprobantesDocument(ControlComprobantesReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ControlComprobantesFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None
