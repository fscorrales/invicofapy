__all__ = [
    "PlanillometroHistReport",
    "PlanillometroHistDocument",
    "PlanillometroHistParams",
    "PlanillometroHistFilter",
]

from typing import Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams


# --------------------------------------------------
class PlanillometroHistParams(BaseModel):
    pass


# -------------------------------------------------
class PlanillometroHistReport(BaseModel):
    desc_programa: str
    desc_subprograma: Optional[str] = None
    desc_proyecto: Optional[str] = None
    desc_actividad: Optional[str] = None
    estructura_actividad: Optional[str] = None
    partida: Optional[str] = None
    estructura: Optional[str] = None
    alta: Optional[int] = None
    acum_2008: Optional[float] = None


# -------------------------------------------------
class PlanillometroHistDocument(PlanillometroHistReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class PlanillometroHistFilter(BaseFilterParams):
    estructura: Optional[str] = None
