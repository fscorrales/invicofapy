__all__ = [
    "ReporteRemanenteParams",
    "ReporteRemanenteSyncParams",
]

import os
from datetime import date
from typing import Optional

from pydantic import Field, field_validator, model_validator

from ...utils import (
    CamelModel,
    get_siif_rdeu012b2_cuit_path,
    get_sscc_cta_cte_path,
    get_sscc_saldos_path,
)


# --------------------------------------------------
class ReporteRemanenteParams(CamelModel):
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
    def check_range(self) -> "ReporteRemanenteParams":
        if self.ejercicio_hasta < self.ejercicio_desde:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# --------------------------------------------------
class ReporteRemanenteSyncParams(ReporteRemanenteParams):
    siif_username: Optional[str] = None
    siif_password: Optional[str] = None
    rdeu012b2cuit_pdf_path: Optional[str] = Field(
        default=os.path.join(get_siif_rdeu012b2_cuit_path(), "rdeu012b2_cuit.pdf"),
        description="Ruta al archivo Rdeu012b2Cuit PDF",
    )
    saldos_csv_path: Optional[str] = Field(
        default=os.path.join(get_sscc_saldos_path(), "saldos_sscc.csv"),
        description="Ruta al archivo Saldos CSV",
    )
    ctas_ctes_excel_path: Optional[str] = Field(
        default=os.path.join(get_sscc_cta_cte_path(), "cta_cte.xlsx"),
        description="Ruta al archivo Ctas Ctes EXCEL",
    )
