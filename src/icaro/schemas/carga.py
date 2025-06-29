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
    nro_fuente: str
    cuit: str
    importe: float
    fondo_reparo: float
    nro_cta_cte: str
    avance: float
    nro_certificado: str
    nro_comprobante: str
    desc_obra: str
    origen: str
    tipo: str
    nro_act: str
    nro_partida: str
    id_carga: str
    ejercicio: str
    mes: str


# -------------------------------------------------
class CargaDocument(CargaReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class CargaFilter(BaseFilterParams):
    nro_subprog: Optional[str] = None
    desc_subprog: Optional[str] = None


# -------------------------------------------------
class CargaValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[CargaDocument]
