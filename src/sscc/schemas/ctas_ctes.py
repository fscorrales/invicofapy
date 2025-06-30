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
    map_to: str
    sscc_cta_cte: Optional[str] = None
    real_cta_cte: Optional[str] = None
    siif_recursos_cta_cte: Optional[str] = None
    siif_gastos_cta_cte: Optional[str] = None
    siif_contabilidad_cta_cte: Optional[str] = None
    sgf_cta_cte: Optional[str] = None
    siif_cta_cte: Optional[str] = None
    icaro_cta_cte: Optional[str] = None


# -------------------------------------------------
class CtasCtesDocument(CtasCtesReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class CtasCtesFilter(BaseFilterParams):
    nro_prog: Optional[str] = None
    desc_prog: Optional[str] = None


# -------------------------------------------------
class CtasCtesValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[CtasCtesDocument]
