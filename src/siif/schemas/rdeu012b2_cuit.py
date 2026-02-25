__all__ = [
    "Rdeu012b2CuitReport",
    "Rdeu012b2CuitDocument",
    "Rdeu012b2CuitParams",
    "Rdeu012b2CuitFilter",
]

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams


# --------------------------------------------------
class Rdeu012b2CuitParams(BaseModel):
    pass


# -------------------------------------------------
class Rdeu012b2CuitReport(BaseModel):
    ejercicio: Optional[int] = None
    mes_hasta: Optional[str] = None
    entidad: Optional[str] = None
    ejercicio_deuda: Optional[int] = None
    fuente: Optional[str] = None
    nro_entrada: Optional[str] = None
    nro_origen: Optional[str] = None
    importe: Optional[float] = None
    saldo: Optional[float] = None
    org_fin: Optional[str] = None
    nro_expte: Optional[str] = None
    cta_cte: Optional[str] = None
    glosa: Optional[str] = None
    fecha_desde: Optional[datetime] = None
    fecha_hasta: Optional[datetime] = None


# -------------------------------------------------
class Rdeu012b2CuitDocument(Rdeu012b2CuitReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rdeu012b2CuitFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
