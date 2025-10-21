__all__ = [
    "HonorariosReport",
    "HonorariosDocument",
    "HonorariosValidationOutput",
    "HonorariosParams",
    "HonorariosFilter",
]

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class HonorariosParams(BaseModel):
    pass


# -------------------------------------------------
class HonorariosReport(BaseModel):
    ejercicio: int
    mes: str
    fecha: datetime
    nro_comprobante: str
    tipo: str
    razon_social: str
    actividad: str
    partida: str
    importe_bruto: float
    iibb: float
    lp: float
    sellos: float
    seguro: float
    otras_retenciones: float
    anticipo: float
    descuento: float
    mutual: float
    embargo: float


# -------------------------------------------------
class HonorariosDocument(HonorariosReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class HonorariosFilter(BaseFilterParams):
    razon_social: Optional[str] = None
    actividad: Optional[str] = None
    partida: Optional[str] = None


# -------------------------------------------------
class HonorariosValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[HonorariosDocument]
