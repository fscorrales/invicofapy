__all__ = ["StoredRf602", "Rf602ValidationOutput"]

from typing import List

from pydantic import BaseModel, NonNegativeFloat

from ...utils import ErrorsWithDocId


class StoredRf602(BaseModel):
    ejercicio: str
    estructura: str
    fuente: str
    programa: str
    subprograma: str
    proyecto: str
    actividad: str
    grupo: str
    partida: str
    org: str
    credito_original: NonNegativeFloat
    credito_vigente: NonNegativeFloat
    comprometido: NonNegativeFloat
    ordenado: NonNegativeFloat
    saldo: float
    pendiente: float


class Rf602ValidationOutput(BaseModel):
    errors: List[ErrorsWithDocId]
    validated: List[StoredRf602]
