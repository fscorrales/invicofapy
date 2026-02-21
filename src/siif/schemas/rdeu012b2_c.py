__all__ = [
    "Rdeu012b2CReport",
    "Rdeu012b2CDocument",
    "Rdeu012b2CParams",
    "Rdeu012b2CFilter",
]

from typing import Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams


# --------------------------------------------------
class Rdeu012b2CParams(BaseModel):
    pass


# -------------------------------------------------
class Rdeu012b2CReport(BaseModel):
    desc_programa: str
    desc_subprograma: Optional[str] = None
    desc_proyecto: Optional[str] = None
    desc_actividad: Optional[str] = None
    actividad: Optional[str] = None
    partida: Optional[str] = None
    estructura: Optional[str] = None
    alta: Optional[str] = None
    acum_2008: Optional[float] = None


# -------------------------------------------------
class Rdeu012b2CDocument(Rdeu012b2CReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rdeu012b2CFilter(BaseFilterParams):
    estructura: Optional[str] = None
