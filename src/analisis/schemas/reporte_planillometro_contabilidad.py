__all__ = [
    "ReportePlanillometroContabildadParams",
    "ReportePlanillometroContabilidadSyncParams",
]

import os
from datetime import date
from typing import Optional

from pydantic import Field, field_validator, model_validator

from ...utils import CamelModel, get_siif_planillometro_hist_path


# --------------------------------------------------
class ReportePlanillometroContabildadParams(CamelModel):
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
    def check_range(self) -> "ReportePlanillometroContabildadParams":
        if self.ejercicio_hasta < self.ejercicio_desde:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# --------------------------------------------------
class ReportePlanillometroContabilidadSyncParams(ReportePlanillometroContabildadParams):
    siif_username: Optional[str] = None
    siif_password: Optional[str] = None
    sgv_username: Optional[str] = None
    sgv_password: Optional[str] = None
    planillometro_hist_excel_path: Optional[str] = Field(
        default=os.path.join(
            get_siif_planillometro_hist_path(), "planillometro_hist.xlsx"
        ),
        description="Ruta al archivo Planillometro Histórico EXCEL",
    )
