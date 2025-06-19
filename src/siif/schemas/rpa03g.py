__all__ = [
    "Rpa03gReport",
    "Rpa03gDocument",
    "Rpa03gValidationOutput",
    "Rpa03gParams",
    "Rpa03gFilter",
]

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId
from .common import GrupoPartidaSIIF


# --------------------------------------------------
class Rpa03gParams(BaseModel):
    ejercicio: int = date.today().year
    grupo_partida: GrupoPartidaSIIF = GrupoPartidaSIIF.bienes_capital
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
class Rpa03gReport(BaseModel):
    ejercicio: int
    mes: str
    fecha: date
    nro_comprobante: str
    importe: float
    grupo: str
    partida: str
    nro_entrada: Optional[str] = None
    nro_origen: Optional[str] = None
    nro_expte: str
    glosa: str
    beneficiario: str


# -------------------------------------------------
class Rpa03gDocument(Rpa03gReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rpa03gFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    grupo_partida: GrupoPartidaSIIF = None


# -------------------------------------------------
class Rpa03gValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[Rpa03gDocument]
