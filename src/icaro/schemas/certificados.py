__all__ = [
    "CertificadosReport",
    "CertificadosDocument",
    "CertificadosValidationOutput",
    "CertificadosParams",
    "CertificadosFilter",
]

from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class CertificadosParams(BaseModel):
    pass


# -------------------------------------------------
class CertificadosReport(BaseModel):
    id_carga: Optional[str] = None
    origen: str
    ejercicio: int
    beneficiario: str
    desc_obra: str
    nro_certificado: str
    monto_certificado: float
    fondo_reparo: Optional[float] = None
    importe_bruto: float
    iibb: Optional[float] = None
    lp: Optional[float] = None
    suss: Optional[float] = None
    gcias: Optional[float] = None
    invico: Optional[float] = None
    otras_retenciones: Optional[float] = None
    importe_neto: float


# -------------------------------------------------
class CertificadosDocument(CertificadosReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class CertificadosFilter(BaseFilterParams):
    nro_subprog: Optional[str] = None
    desc_subprog: Optional[str] = None


# -------------------------------------------------
class CertificadosValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[CertificadosDocument]
