__all__ = [
    "SubprogramasReport",
    "SubprogramasDocument",
    "SubprogramasValidationOutput",
    "SubprogramasParams",
    "SubprogramasFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class SubprogramasParams(BaseModel):
    pass


# -------------------------------------------------
class SubprogramasReport(BaseModel):
    nro_subprog: str
    desc_subprog: str
    nro_prog: str


# -------------------------------------------------
class SubprogramasDocument(SubprogramasReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class SubprogramasFilter(BaseFilterParams):
    nro_subprog: Optional[str] = None
    desc_subprog: Optional[str] = None


# -------------------------------------------------
class SubprogramasValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[SubprogramasDocument]
