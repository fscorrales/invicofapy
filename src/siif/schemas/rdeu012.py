__all__ = [
    "Rdeu012Report",
    "Rdeu012Document",
    "Rdeu012Params",
    "Rdeu012Filter",
]

from datetime import date
from typing import Optional

from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
)
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams


# -------------------------------------------------
class Rdeu012Params(BaseModel):
    mes_from: str = Field(alias="añoMesDesde", default=date.today().strftime("%Y%m"))
    mes_to: str = Field(alias="añoMesHasta", default=date.today().strftime("%Y%m"))

    @field_validator("mes_from", "mes_to")
    @classmethod
    def validate_mes_format(cls, v: str) -> str:
        import re

        # Aceptamos "yyyymm" o "yyyy-mm"
        pattern = r"^\d{6}$|^\d{4}-\d{2}$"
        if not re.match(pattern, v):
            raise ValueError(
                "El formato debe ser 'yyyymm' o 'yyyy-mm' (ej: 202505 o 2025-05)"
            )

        # Normalizamos el formato a "yyyymm"
        mes = v.replace("-", "")

        current = date.today()
        current_yyyymm = int(f"{current.year}{current.month:02d}")

        if not (201001 <= int(mes) <= current_yyyymm):
            raise ValueError(f"El mes debe estar entre 201001 y {current_yyyymm}")

        return mes

    @model_validator(mode="after")
    def validate_range(self) -> "Rdeu012Params":
        if int(self.mes_to) < int(self.mes_from):
            raise ValueError("mesHasta no puede ser menor que mesDesde")
        return self


# -------------------------------------------------
class Rdeu012Report(BaseModel):
    ejercicio: int
    mes: str
    fecha: date
    mes_hasta: str
    fuente: str
    cta_cte: str
    nro_comprobante: str
    importe: float
    saldo: float
    cuit: str
    beneficiario: str
    glosa: str
    nro_expte: str
    nro_entrada: str
    nro_origen: str
    fecha_aprobado: str
    fecha_desde: date
    fecha_hasta: date
    org_fin: str


# -------------------------------------------------
class Rdeu012Document(Rdeu012Report):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class Rdeu012Filter(BaseFilterParams):
    mes: Optional[str] = Field(alias="añoMes", default=date.today().strftime("%Y%m"))
