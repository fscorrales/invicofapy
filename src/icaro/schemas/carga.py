__all__ = [
    "CargaReport",
    "CargaDocument",
    "CargaValidationOutput",
    "CargaParams",
    "CargaFilter",
]

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class CargaParams(BaseModel):
    pass


# -------------------------------------------------
class CargaReport(BaseModel):
    fecha: date
    fuente: str
    cuit: str
    importe: float
    fondo_reparo: float
    cta_cte: str
    avance: float
    nro_certificado: Optional[str] = None
    nro_comprobante: str
    desc_obra: str
    origen: str
    tipo: str
    actividad: str
    partida: str
    id_carga: str
    ejercicio: int
    mes: str


# -------------------------------------------------
class CargaDocument(CargaReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class CargaFilter(BaseFilterParams):
    nro_comprobante: Optional[str] = None
    cuit: Optional[str] = None
    actividad: Optional[str] = None
    desc_obra: Optional[str] = None


# -------------------------------------------------
class CargaValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[CargaDocument]
