__all__ = [
    "Rfondos04Report",
    "Rfondos04Document",
    "Rfondos04ValidationOutput",
    "Rfondos04Params",
    "Rfondos04Filter",
]

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, CamelModel, ErrorsWithDocId
from .common import TipoComprobanteSIIF


# --------------------------------------------------
class Rfondos04Params(CamelModel):
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
    def check_range(self) -> "Rfondos04Params":
        if self.ejercicio_hasta < self.ejercicio_desde:
            raise ValueError("Ejercicio Desde no puede ser menor que Ejercicio Hasta")
        return self


# -------------------------------------------------
class Rfondos04Report(BaseModel):
    ejercicio: int
    mes: str
    fecha: datetime
    tipo_comprobante: str
    nro_comprobante: str
    nro_fondo: str
    glosa: str
    importe: float
    saldo_c01: float
    saldo_asiento: float


# -------------------------------------------------
class Rfondos04Document(Rfondos04Report):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rfondos04Filter(BaseFilterParams):
    ejercicio: Optional[int] = None
    # tipo_comprobante: TipoComprobanteSIIF = None


# -------------------------------------------------
class Rfondos04ValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[Rfondos04Document]
