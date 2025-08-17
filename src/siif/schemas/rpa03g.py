__all__ = [
    "Rpa03gReport",
    "Rpa03gDocument",
    "Rpa03gValidationOutput",
    "Rpa03gParams",
    "Rpa03gFilter",
]

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, CamelModel, ErrorsWithDocId
from .common import GrupoPartidaSIIF

grupo_partida: GrupoPartidaSIIF = GrupoPartidaSIIF.bienes_capital


# --------------------------------------------------
class Rpa03gParams(CamelModel):
    ejercicio_desde: int = Field(default=date.today().year)
    ejercicio_hasta: int = Field(default=date.today().year)
    grupo_partida_desde: GrupoPartidaSIIF = GrupoPartidaSIIF.sueldos
    grupo_partida_hasta: GrupoPartidaSIIF = GrupoPartidaSIIF.bienes_capital

    @field_validator("ejercicio_desde", "ejercicio_hasta")
    @classmethod
    def validate_ejercicio_range(cls, v: int) -> int:
        current_year = date.today().year
        if not (2010 <= v <= current_year):
            raise ValueError(f"El ejercicio debe estar entre 2010 y {current_year}")
        return v

    @field_validator("grupo_partida_desde", "grupo_partida_hasta")
    @classmethod
    def validate_grupo_partida_range(cls, v: int) -> int:
        desde = int(GrupoPartidaSIIF.sueldos.value)
        hasta = int(GrupoPartidaSIIF.bienes_capital.value)
        if not (desde <= v <= hasta):
            raise ValueError(f"El grupo partida debe estar entre {desde} y {hasta}")
        return v

    @model_validator(mode="after")
    def check_range(self) -> "Rpa03gParams":
        if self.ejercicio_hasta < self.ejercicio_desde:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        if int(self.grupo_partida_hasta) < int(self.grupo_partida_desde):
            raise ValueError(
                "Grupo partida Desde no puede ser menor que Grupo partida Hasta"
            )
        return self


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
