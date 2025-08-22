__all__ = [
    "ResumenRendObrasReport",
    "ResumenRendObrasDocument",
    "ResumenRendObrasValidationOutput",
    "ResumenRendObrasParams",
    "ResumenRendObrasFilter",
]

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class ResumenRendObrasParams(BaseModel):
    pass


# -------------------------------------------------
class ResumenRendObrasReport(BaseModel):
    id_carga: Optional[str] = None
    ejercicio: Optional[int] = None
    mes: Optional[str] = None
    fecha: Optional[datetime] = None
    origen: Optional[str] = None
    cod_obra: Optional[str] = None
    desc_obra: Optional[str] = None
    beneficiario: Optional[str] = None
    nro_libramiento_sgf: Optional[str] = None
    importe_bruto: float
    iibb: Optional[float] = None
    tl: Optional[float] = None
    sellos: Optional[float] = None
    suss: Optional[float] = None
    gcias: Optional[float] = None
    seguro: Optional[float] = None
    salud: Optional[float] = None
    mutual: Optional[float] = None
    importe_neto: Optional[float] = None
    destino: Optional[str] = None
    movimiento: Optional[str] = None


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
