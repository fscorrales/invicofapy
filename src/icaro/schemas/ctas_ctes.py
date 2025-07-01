__all__ = [
    "CtasCtesReport",
    "CtasCtesDocument",
    "CtasCtesValidationOutput",
    "CtasCtesParams",
    "CtasCtesFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class CtasCtesParams(BaseModel):
    pass


# -------------------------------------------------
class CtasCtesReport(BaseModel):
    cta_cte_anterior: str
    cta_cte: str
    desc_cta_cte: str
    banco: str


# -------------------------------------------------
class CtasCtesDocument(CtasCtesReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class CtasCtesFilter(BaseFilterParams):
    nro_subprog: Optional[str] = None
    desc_subprog: Optional[str] = None


# -------------------------------------------------
class CtasCtesValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[CtasCtesDocument]
