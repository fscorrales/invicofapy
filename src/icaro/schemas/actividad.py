__all__ = [
    "ActividadReport",
    "ActividadDocument",
    "ActividadValidationOutput",
    "ActividadParams",
    "ActividadFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class ActividadParams(BaseModel):
    pass


# -------------------------------------------------
class ActividadReport(BaseModel):
    nro_act: str
    desc_act: str
    nro_proy: str


# -------------------------------------------------
class ActividadDocument(ActividadReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ActividadFilter(BaseFilterParams):
    nro_act: Optional[str] = None
    desc_act: Optional[str] = None


# -------------------------------------------------
class ActividadValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ActividadDocument]
