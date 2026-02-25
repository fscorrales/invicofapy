__all__ = [
    "BancoINVICOSdoFinalParams",
    "BancoINVICOSdoFinalReport",
    "BancoINVICOSdoFinalDocument",
    "BancoINVICOSdoFinalFilter",
]


from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic_mongo import PydanticObjectId

from ...utils import BaseFilterParams, ErrorsWithDocId


# --------------------------------------------------
class BancoINVICOSdoFinalParams(BaseModel):
    pass


# -------------------------------------------------
class BancoINVICOSdoFinalReport(BaseModel):
    ejercicio: int
    cta_cte: str
    desc_cta_cte: str
    desc_banco: str
    saldo: float


# -------------------------------------------------
class BancoINVICOSdoFinalDocument(BancoINVICOSdoFinalReport):
    id: PydanticObjectId = Field(alias="_id")


# -------------------------------------------------
class BancoINVICOSdoFinalFilter(BaseFilterParams):
    ejercicio: Optional[int] = None
    cta_cte: Optional[str] = None


# -------------------------------------------------
class BancoINVICOSdoFinalValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[BancoINVICOSdoFinalDocument]
