__all__ = [
    "ReporteModulosBasicosIcaroParams",
    "ReporteModulosBasicosIcaroReport",
    "ReporteModulosBasicosIcaroDocument",
    "ReporteModulosBasicosIcaroFilter",
]

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams


# --------------------------------------------------
class ReporteModulosBasicosIcaroParams(BaseModel):
    ejercicio: int = date.today().year
    es_ejercicio_to: bool = (True,)
    es_desc_siif: bool = True
    es_neto_pa6: bool = True

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
class ReporteModulosBasicosIcaroReport(BaseModel):
    id_carga: str
    nro_comprobante: str
    ejercicio: int
    mes: str
    fecha: date
    cuit: str
    desc_obra: str
    fuente: str
    cta_cte: str
    actividad: str
    partida: str
    importe: float
    fondo_reparo: float
    nro_certificado: Optional[str] = None
    avance: float
    origen: Optional[str] = None
    tipo: str
    localidad: str
    norma_legal: Optional[str] = None
    info_adicional: Optional[str] = None
    desc_act: str
    desc_prog: str
    desc_subprog: str
    desc_proy: str
    proveedor: str


# -------------------------------------------------
class ReporteModulosBasicosIcaroDocument(ReporteModulosBasicosIcaroReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ReporteModulosBasicosIcaroFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None
