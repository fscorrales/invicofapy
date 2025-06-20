__all__ = [
    "SubprogramaReport",
    "SubprogramaDocument",
    "SubprogramaValidationOutput",
    "SubprogramaParams",
    "SubprogramaFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class SubprogramaParams(BaseModel):
    pass


# -------------------------------------------------
class SubprogramaReport(BaseModel):
    nro_subprog: str
    desc_subprog: str
    nro_prog: str


# -------------------------------------------------
class SubprogramaDocument(SubprogramaReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class SubprogramaFilter(BaseFilterParams):
    nro_subprog: Optional[str] = None
    desc_subprog: Optional[str] = None


# -------------------------------------------------
class SubprogramaValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[SubprogramaDocument]
