__all__ = [
    "ControlDeudaFlotanteParams",
    "ControlDeudaFlotanteSyncParams",
    "ControlDeudaFlotanteReport",
    "ControlDeudaFlotanteFilter",
]

import os
from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, CamelModel, get_sscc_cta_cte_path


# --------------------------------------------------
class ControlDeudaFlotanteParams(CamelModel):
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
    def check_range(self) -> "ControlDeudaFlotanteParams":
        if self.ejercicio_hasta < self.ejercicio_desde:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# --------------------------------------------------
class ControlDeudaFlotanteSyncParams(ControlDeudaFlotanteParams):
    siif_username: Optional[str] = None
    siif_password: Optional[str] = None
    ctas_ctes_excel_path: Optional[str] = Field(
        default=os.path.join(get_sscc_cta_cte_path(), "cta_cte.xlsx"),
        description="Ruta al archivo Ctas Ctes EXCEL",
    )


# -------------------------------------------------
class ControlDeudaFlotanteReport(BaseModel):
    ejercicio_contable: int
    ejercicio: int
    fuente: int
    cta_cte: str
    nro_original: str
    saldo_rdeu: float
    cuit: str
    glosa: str
    nro_expte: Optional[str] = None
    mes: str
    fecha: datetime
    fecha_aprobado: datetime
    cta_contable: str
    nro_entrada: str
    auxiliar_1: str
    auxiliar_2: str
    tipo_comprobante: str
    creditos: float
    debitos: float
    saldo_contable: float
    nro_comprobante: Optional[str] = None


# -------------------------------------------------
class ControlDeudaFlotanteDocument(ControlDeudaFlotanteReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ControlDeudaFlotanteFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None
