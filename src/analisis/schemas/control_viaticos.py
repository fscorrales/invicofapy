__all__ = [
    "ControlViaticosParams",
    "ControlViaticosSyncParams",
    "ControlViaticosRendicionReport",
    "ControlViaticosRendicionDocument",
    # "ControlEscribanosSIIFvsSGFReport",
    # "ControlEscribanosSIIFvsSGFDocument",
    "ControlViaticosFilter",
]

import os
from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, CamelModel, get_sscc_cta_cte_path


# --------------------------------------------------
class ControlViaticosParams(CamelModel):
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
    def check_range(self) -> "ControlViaticosParams":
        if self.ejercicio_hasta < self.ejercicio_desde:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# --------------------------------------------------
class ControlViaticosSyncParams(ControlViaticosParams):
    siif_username: Optional[str] = None
    siif_password: Optional[str] = None
    sscc_username: Optional[str] = None
    sscc_password: Optional[str] = None
    ctas_ctes_excel_path: Optional[str] = Field(
        default=os.path.join(get_sscc_cta_cte_path(), "cta_cte.xlsx"),
        description="Ruta al archivo Ctas Ctes EXCEL",
    )


# -------------------------------------------------
class ControlViaticosRendicionReport(BaseModel):
    ejercicio: int
    mes: str
    nro_expte: str
    siif_anticipo: float
    siif_rendicion: float
    siif_reversion: float
    siif_saldo_anticipo: float
    siif_reembolso: float
    siif_gasto_total: float
    sscc_anticipo: float
    sscc_reversion: float
    sscc_reembolso: float
    sscc_gasto_total: float
    dif_gasto_total: float


# -------------------------------------------------
class ControlViaticosRendicionDocument(ControlViaticosRendicionReport):
    id: PydanticObjectId = Field(alias="_id")


# # -------------------------------------------------
# class ControlEscribanosSIIFvsSGFReport(BaseModel):
#     ejercicio: Optional[int] = None
#     mes: Optional[str] = None
#     cuit: Optional[str] = None
#     carga_fei: Optional[float] = None
#     pagos_fei: Optional[float] = None
#     fei_impagos: Optional[float] = None
#     pagos_sgf: Optional[float] = None
#     dif_pagos: Optional[float] = None


# # -------------------------------------------------
# class ControlEscribanosSIIFvsSGFDocument(ControlEscribanosSIIFvsSGFReport):
#     id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ControlViaticosFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None
