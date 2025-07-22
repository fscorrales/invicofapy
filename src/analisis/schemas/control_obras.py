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

from pydantic import BaseModel, Field, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, CamelModel, get_sqlite_path


# --------------------------------------------------
class ControlObrasParams(CamelModel):
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
class ControlObrasSyncParams(ControlObrasParams):
    siif_username: Optional[str] = None
    siif_password: Optional[str] = None
    sscc_username: Optional[str] = None
    sscc_password: Optional[str] = None
    sgf_username: Optional[str] = None
    sgf_password: Optional[str] = None
    ctas_ctes_excel_path: Optional[str] = Field(
        default=os.path.join(get_sqlite_path(), "SIIF.sqlite"),
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
