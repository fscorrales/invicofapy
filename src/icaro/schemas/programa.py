__all__ = [
    "ProgramaReport",
    "ProgramaDocument",
    "ProgramaValidationOutput",
    "ProgramaParams",
    "ProgramaFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class ProgramaParams(BaseModel):
    pass


# -------------------------------------------------
class ProgramaReport(BaseModel):
    nro_prog: str
    desc_prog: str


# -------------------------------------------------
class ProgramaDocument(ProgramaReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ProgramaFilter(BaseFilterParams):
    nro_prog: Optional[str] = None
    desc_prog: Optional[str] = None


# -------------------------------------------------
class ProgramaValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ProgramaDocument]
