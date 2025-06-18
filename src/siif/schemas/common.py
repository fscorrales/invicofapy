__all__ = ["EjercicioSIIF", "GrupoPartidaSIIF"]

from datetime import date
from enum import Enum

from pydantic import BaseModel, Field, field_validator


# -------------------------------------------------
class EjercicioSIIF(BaseModel):
    """
    Representa el año fiscal del SIIF. Debe ser un año entre 2010 y el actual.
    """

    value: int = Field(
        default_factory=lambda: date.today().year,
        alias="ejercicio",
        description="Año del ejercicio fiscal (entre 2010 y el año actual)",
        example=2025,
    )

    @field_validator("value")
    @classmethod
    def validate_value(cls, v):
        current_year = date.today().year
        if not (2010 <= v <= current_year):
            raise ValueError(f"Ejercicio debe estar entre 2010 y {current_year}")
        return v

    def __int__(self):
        return self.value


# -------------------------------------------------
class GrupoPartidaSIIF(str, Enum):
    """
    Enum para representar los grupos de partidas del SIIF.
    """

    sueldos = "1"
    bienes_consumo = "2"
    servicios = "3"
    bienes_capital = "4"
