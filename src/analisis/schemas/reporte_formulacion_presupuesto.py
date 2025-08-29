__all__ = [
    "ReporteFormulacionPresupuestoParams",
    "ReporteFormulacionPresupuestoSyncParams",
    "ReporteFormulacionPresupuestoReport",
    "ReporteFormulacionPresupuestoDocument",
    "ReporteFormulacionPresupuestoFilter",
]

import os
from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, get_siif_planillometro_hist_path


# --------------------------------------------------
class ReporteFormulacionPresupuestoParams(BaseModel):
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
class ReporteFormulacionPresupuestoSyncParams(ReporteFormulacionPresupuestoParams):
    siif_username: Optional[str] = None
    siif_password: Optional[str] = None
    planillometro_hist_excel_path: Optional[str] = Field(
        default=os.path.join(get_siif_planillometro_hist_path(), "planillometro_hist.xlsx"),
        description="Ruta al archivo Planillometro Histórico EXCEL",
    )


# -------------------------------------------------
class ReporteFormulacionPresupuestoReport(BaseModel):
    ejercicio: int
    estructura: str
    programa: str
    desc_programa: str
    desc_subprograma: str
    desc_proyecto: str
    desc_actividad: str
    grupo: str
    partida: str
    fuente: str
    credito_original: float
    credito_vigente: float
    comprometido: float
    ordenado: float
    saldo: float


# -------------------------------------------------
class ReporteFormulacionPresupuestoDocument(ReporteFormulacionPresupuestoReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ReporteFormulacionPresupuestoFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None
