__all__ = [
    "Rfondo07tpReport",
    "Rfondo07tpDocument",
    "Rfondo07tpValidationOutput",
    "Rfondo07tpParams",
    "Rfondo07tpFilter",
]

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId
from .common import TipoComprobanteSIIF


# --------------------------------------------------
class Rfondo07tpParams(BaseModel):
    ejercicio: int = date.today().year
    tipo_comprobante: TipoComprobanteSIIF = TipoComprobanteSIIF.adelanto_contratista
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
class Rfondo07tpReport(BaseModel):
    ejercicio: int
    mes: str
    fecha: date
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
    tipo_comprobante: TipoComprobanteSIIF = None


# -------------------------------------------------
class Rfondo07tpValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[Rfondo07tpDocument]
