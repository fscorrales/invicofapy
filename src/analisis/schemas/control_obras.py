__all__ = [
    "ControlObrasParams",
    "ControlObrasSyncParams",
    "ControlObrasReport",
    "ControlObrasDocument",
    "ControlObrasFilter",
]

import os
from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_mongo import PydanticObjectId

from ...sgf.schemas.common import Origen
from ...utils import BaseFilterParams, CamelModel, get_sscc_cta_cte_path


# --------------------------------------------------
class ControlObrasParams(CamelModel):
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
    def check_range(self) -> "ControlObrasParams":
        if self.ejercicio_hasta < self.ejercicio_desde:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# --------------------------------------------------
class ControlObrasSyncParams(ControlObrasParams):
    origen: Optional[Origen] = None
    siif_username: Optional[str] = None
    siif_password: Optional[str] = None
    sscc_username: Optional[str] = None
    sscc_password: Optional[str] = None
    sgf_username: Optional[str] = None
    sgf_password: Optional[str] = None
    ctas_ctes_excel_path: Optional[str] = Field(
        default=os.path.join(get_sscc_cta_cte_path(), "cta_cte.xlsx"),
        description="Ruta al archivo Ctas Ctes EXCEL",
    )


# -------------------------------------------------
class ControlObrasReport(BaseModel):
    ejercicio: int
    mes: str
    cta_cte: str
    grupo: str
    recursos_siif: float
    depositos_banco: float


# -------------------------------------------------
class ControlObrasDocument(ControlObrasReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ControlObrasFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None
