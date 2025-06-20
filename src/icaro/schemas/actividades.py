__all__ = [
    "ActividadesReport",
    "ActividadesDocument",
    "ActividadesValidationOutput",
    "ActividadesParams",
    "ActividadesFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class ActividadesParams(BaseModel):
    pass


# -------------------------------------------------
class ActividadesReport(BaseModel):
    nro_act: str
    desc_act: str
    nro_proy: str


# -------------------------------------------------
class ActividadesDocument(ActividadesReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ActividadesFilter(BaseFilterParams):
    nro_act: Optional[str] = None
    desc_act: Optional[str] = None


# -------------------------------------------------
class ActividadesValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ActividadesDocument]
