__all__ = [
    "ResumenRendObrasReport",
    "ResumenRendObrasDocument",
    "ResumenRendObrasValidationOutput",
    "ResumenRendObrasParams",
    "ResumenRendObrasFilter",
]

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class ResumenRendObrasParams(BaseModel):
    pass


# -------------------------------------------------
class ResumenRendObrasReport(BaseModel):
    id_carga: str
    ejercicio: str
    mes: str
    fecha: date
    origen: str
    cod_obra: str
    desc_obra: str
    beneficiario: str
    nro_libramiento_sgf: str
    importe_bruto: float
    iibb: float
    tl: float
    sellos: float
    suss: float
    gcias: float
    seguro: float
    salud: float
    mutual: float
    importe_neto: float
    destino: str
    movimiento: str


# -------------------------------------------------
class ResumenRendObrasDocument(ResumenRendObrasReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ResumenRendObrasFilter(BaseFilterParams):
    nro_subprog: Optional[str] = None
    desc_subprog: Optional[str] = None


# -------------------------------------------------
class ResumenRendObrasValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ResumenRendObrasDocument]
