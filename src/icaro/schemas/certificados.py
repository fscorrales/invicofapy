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
    id_carga: str
    origen: str
    ejercicio: int
    beneficiario: str
    desc_obra: str
    nro_certificado: str
    monto_certificado: float
    fondo_reparo: float
    importe_bruto: float
    iibb: float
    lp: float
    suss: float
    gcias: float
    invico: float
    otras_retenciones: float
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
