__all__ = [
    "ObrasReport",
    "ObrasDocument",
    "ObrasValidationOutput",
    "ObrasParams",
    "ObrasFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class ObrasParams(BaseModel):
    pass


# -------------------------------------------------
class ObrasReport(BaseModel):
    localidad: str
    cuit: str
    actividad: str
    partida: str
    fuente: str
    monto_contrato: Optional[float] = None
    monto_adicional: Optional[float] = None
    cta_cte: str
    norma_legal: Optional[str] = None
    desc_obra: str
    info_adicional: Optional[str] = None


# -------------------------------------------------
class ObrasDocument(ObrasReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ObrasFilter(BaseFilterParams):
    desc_obra: Optional[str] = None
    activadad: Optional[str] = None


# -------------------------------------------------
class ObrasValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ObrasDocument]
