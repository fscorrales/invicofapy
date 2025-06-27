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
    nro_act: str
    nro_partida: str
    nro_fuente: str
    monto_contrato: float
    monto_adicional: float
    nro_cta_cte: str
    norma_legal: str
    desc_obra: str
    info_adicional: str


# -------------------------------------------------
class ObrasDocument(ObrasReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ObrasFilter(BaseFilterParams):
    nro_subprog: Optional[str] = None
    desc_subprog: Optional[str] = None


# -------------------------------------------------
class ObrasValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ObrasDocument]
