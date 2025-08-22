__all__ = [
    "ProveedoresReport",
    "ProveedoresDocument",
    "ProveedoresValidationOutput",
    "ProveedoresParams",
    "ProveedoresFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class ProveedoresParams(BaseModel):
    pass


# -------------------------------------------------
class ProveedoresReport(BaseModel):
    codigo: int
    desc_proveedor: str
    domicilio: Optional[str] = None
    localidad: Optional[str] = None
    telefono: Optional[str] = None
    cuit: str
    condicion_iva: Optional[str] = None


# -------------------------------------------------
class ProveedoresDocument(ProveedoresReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ProveedoresFilter(BaseFilterParams):
    cuit: Optional[str] = None
    desc_proveedor: Optional[str] = None


# -------------------------------------------------
class ProveedoresValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ProveedoresDocument]
