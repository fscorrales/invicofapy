__all__ = [
    "Rdeu012b2CuitReport",
    "Rdeu012b2CuitDocument",
    "Rdeu012b2CuitParams",
    "Rdeu012b2CuitFilter",
]

from typing import Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams


# --------------------------------------------------
class Rdeu012b2CuitParams(BaseModel):
    pass


# -------------------------------------------------
class Rdeu012b2CuitReport(BaseModel):
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
class Rdeu012b2CuitDocument(Rdeu012b2CuitReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rdeu012b2CuitFilter(BaseFilterParams):
    estructura: Optional[str] = None
