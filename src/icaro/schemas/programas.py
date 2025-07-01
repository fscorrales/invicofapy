__all__ = [
    "ProgramasReport",
    "ProgramasDocument",
    "ProgramasValidationOutput",
    "ProgramasParams",
    "ProgramasFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class ProgramasParams(BaseModel):
    pass


# -------------------------------------------------
class ProgramasReport(BaseModel):
    programa: str
    desc_programa: str


# -------------------------------------------------
class ProgramasDocument(ProgramasReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ProgramasFilter(BaseFilterParams):
    nro_prog: Optional[str] = None
    desc_prog: Optional[str] = None


# -------------------------------------------------
class ProgramasValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ProgramasDocument]
