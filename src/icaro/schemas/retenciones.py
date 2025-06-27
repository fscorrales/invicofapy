__all__ = [
    "RetencionesReport",
    "RetencionesDocument",
    "RetencionesValidationOutput",
    "RetencionesParams",
    "RetencionesFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class RetencionesParams(BaseModel):
    pass


# -------------------------------------------------
class RetencionesReport(BaseModel):
    codigo: str
    importe: float
    id_carga: str


# -------------------------------------------------
class RetencionesDocument(RetencionesReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class RetencionesFilter(BaseFilterParams):
    nro_subprog: Optional[str] = None
    desc_subprog: Optional[str] = None


# -------------------------------------------------
class RetencionesValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[RetencionesDocument]
