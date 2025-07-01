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
    codigo: str
    desc_proveedor: str
    domicilio: str
    localidad: str
    telefono: str
    cuit: str
    condicion_iva: str


# -------------------------------------------------
class ProveedoresDocument(ProveedoresReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class ProveedoresFilter(BaseFilterParams):
    nro_subprog: Optional[str] = None
    desc_subprog: Optional[str] = None


# -------------------------------------------------
class ProveedoresValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[ProveedoresDocument]
