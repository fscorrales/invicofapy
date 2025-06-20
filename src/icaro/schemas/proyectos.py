__all__ = [
    "ProyectoReport",
    "ProyectoDocument",
    "ProyectoValidationOutput",
    "ProyectoParams",
    "ProyectoFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class ProyectoParams(BaseModel):
    pass


# -------------------------------------------------
class ProyectoReport(BaseModel):
    nro_proy: str
    desc_proy: str
    nro_subprog: str


# -------------------------------------------------
class ProyectoDocument(ProyectoReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ProyectoFilter(BaseFilterParams):
    nro_proy: Optional[str] = None
    desc_proy: Optional[str] = None


# -------------------------------------------------
class ProyectoValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ProyectoDocument]
