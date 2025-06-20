__all__ = [
    "ProyectosReport",
    "ProyectosDocument",
    "ProyectosValidationOutput",
    "ProyectosParams",
    "ProyectosFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class ProyectosParams(BaseModel):
    pass


# -------------------------------------------------
class ProyectosReport(BaseModel):
    nro_proy: str
    desc_proy: str
    nro_subprog: str


# -------------------------------------------------
class ProyectosDocument(ProyectosReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ProyectosFilter(BaseFilterParams):
    nro_proy: Optional[str] = None
    desc_proy: Optional[str] = None


# -------------------------------------------------
class ProyectosValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ProyectosDocument]
