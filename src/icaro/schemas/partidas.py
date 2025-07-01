__all__ = [
    "PartidasReport",
    "PartidasDocument",
    "PartidasValidationOutput",
    "PartidasParams",
    "PartidasFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class PartidasParams(BaseModel):
    pass


# -------------------------------------------------
class PartidasReport(BaseModel):
    grupo: str
    desc_grupo: str
    partida_parcial: str
    desc_partida_parcial: str
    partida: str
    desc_partida: str


# -------------------------------------------------
class PartidasDocument(PartidasReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class PartidasFilter(BaseFilterParams):
    nro_subprog: Optional[str] = None
    desc_subprog: Optional[str] = None


# -------------------------------------------------
class PartidasValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[PartidasDocument]
