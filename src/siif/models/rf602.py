__all__ = ["StoredRf602"]

from pydantic import BaseModel, PositiveFloat


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
    credito_original: PositiveFloat
    credito_vigente: PositiveFloat
    comprometido: PositiveFloat
    ordenado: PositiveFloat
    saldo: float
    pendiente: float
