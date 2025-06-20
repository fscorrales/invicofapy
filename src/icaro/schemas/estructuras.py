"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 20-jun-2025
Purpose: Unified schema for Estructura (Prog + Subprog + Proy + Act) to be used in the future.
"""

__all__ = [
    "EstructurasReport",
    "EstructurasDocument",
    "EstructurasValidationOutput",
    "EstructurasParams",
    "EstructurasFilter",
]


from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class EstructurasParams(BaseModel):
    pass


# -------------------------------------------------
class EstructurasReport(BaseModel):
    nro_estructura: (
        str  # Example: 11, 11-00, 11-00-02, 11-00-02-79 (all in the same field)
    )
    desc_estructura: str  # Example: "Programa de Salud", "Subprograma de Salud", "Proyecto de Salud", "Actividad de Salud"


# -------------------------------------------------
class EstructurasDocument(EstructurasReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class EstructurasFilter(BaseFilterParams):
    nro_estructura: Optional[str] = None
    desc_estructura: Optional[str] = None


# -------------------------------------------------
class EstructurasValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[EstructurasDocument]
