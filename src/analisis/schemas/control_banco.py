__all__ = [
    "ControlBancoParams",
    "ControlBancoSyncParams",
    "ControlBancoFilter",
    "ControlBancoReport",
    "ControlBancoDocument",
]

import os
from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, get_sscc_cta_cte_path


# --------------------------------------------------
class ControlBancoParams(BaseModel):
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
class ControlBancoSyncParams(ControlBancoParams):
    siif_username: Optional[str] = None
    siif_password: Optional[str] = None
    sscc_username: Optional[str] = None
    sscc_password: Optional[str] = None
    ctas_ctes_excel_path: Optional[str] = Field(
        default=os.path.join(get_sscc_cta_cte_path(), "cta_cte.xlsx"),
        description="Ruta al archivo Ctas Ctes EXCEL",
    )


# -------------------------------------------------
class ControlBancoReport(BaseModel):
    ejercicio: int
    mes: str
    clase: str
    cta_cte: str
    siif_importe: float
    sscc_importe: float
    diferencia: float


# -------------------------------------------------
class ControlBancoDocument(ControlBancoReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ControlBancoFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None
