__all__ = [
    "ReporteLibroDiarioParams",
    "ReporteLibroDiarioSyncParams",
    "ReporteLibroDiarioFilter",
]

from datetime import date
from typing import Optional

from pydantic import BaseModel, field_validator

from ...utils import BaseFilterParams


# --------------------------------------------------
class ReporteLibroDiarioParams(BaseModel):
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
class ReporteLibroDiarioSyncParams(ReporteLibroDiarioParams):
    siif_username: Optional[str] = None
    siif_password: Optional[str] = None


# -------------------------------------------------
class ReporteLibroDiarioFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None
