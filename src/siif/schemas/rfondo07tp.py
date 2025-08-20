__all__ = [
    "Rfondo07tpReport",
    "Rfondo07tpDocument",
    "Rfondo07tpValidationOutput",
    "Rfondo07tpParams",
    "Rfondo07tpFilter",
]

from datetime import datetime, date
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, CamelModel, ErrorsWithDocId
from .common import TipoComprobanteSIIF


# --------------------------------------------------
class Rfondo07tpParams(CamelModel):
    ejercicio_desde: int = Field(default=date.today().year)
    ejercicio_hasta: int = Field(default=date.today().year)
    tipo_comprobante: TipoComprobanteSIIF = TipoComprobanteSIIF.adelanto_contratista

    @field_validator("ejercicio_desde", "ejercicio_hasta")
    @classmethod
    def validate_ejercicio_range(cls, v: int) -> int:
        current_year = date.today().year
        if not (2010 <= v <= current_year):
            raise ValueError(f"El ejercicio debe estar entre 2010 y {current_year}")
        return v

    @model_validator(mode="after")
    def check_range(self) -> "Rfondo07tpParams":
        if self.ejercicio_hasta < self.ejercicio_desde:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class Rfondo07tpReport(BaseModel):
    ejercicio: int
    mes: str
    fecha: datetime
    tipo_comprobante: str
    nro_comprobante: str
    nro_fondo: str
    glosa: str
    ingresos: float
    egresos: float
    saldo: float


# -------------------------------------------------
class Rfondo07tpDocument(Rfondo07tpReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rfondo07tpFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    # tipo_comprobante: TipoComprobanteSIIF = None


# -------------------------------------------------
class Rfondo07tpValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[Rfondo07tpDocument]
