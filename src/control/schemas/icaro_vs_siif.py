__all__ = [
    "ControlEjecucionAnualReport",
    "ControlEjecucionAnualDocument",
    "ControlEjecucionAnualValidationOutput",
    "ControlEjecucionAnualParams",
    "ControlEjecucionAnualFilter",
]

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, NonNegativeFloat, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class ControlEjecucionAnualParams(BaseModel):
    ejercicio: int = date.today().year
    # ejercicio: int = Field(
    #     default_factory=lambda: date.today().year,
    #     alias="ejercicio",
    #     description="Año del ejercicio fiscal (entre 2010 y el año actual)",
    #     example=2025,
    # )

    @field_validator("ejercicio")
    @classmethod
    def validate_value(cls, v):
        current_year = date.today().year
        if not (2010 <= v <= current_year):
            raise ValueError(f"Ejercicio debe estar entre 2010 y {current_year}")
        return v

    def __int__(self):
        return self.ejercicio


# -------------------------------------------------
class ControlEjecucionAnualReport(BaseModel):
    ejercicio: int	
    estructura: str	
    fuente: int	
    ejecucion_siif: float	
    ejecucion_icaro: float	
    diferencia: float	
    desc_actividad: str	
    desc_programa: str
    desc_subprograma:str	
    desc_proyecto:str


# -------------------------------------------------
class ControlEjecucionAnualDocument(ControlEjecucionAnualReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ControlEjecucionAnualFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    fuente: Optional[int] = None


# -------------------------------------------------
class ControlEjecucionAnualValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ControlEjecucionAnualDocument]
