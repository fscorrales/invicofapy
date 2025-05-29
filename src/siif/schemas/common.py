__all__ = ["EjercicioSIIF"]

from pydantic import BaseModel, validator
import datetime

class EjercicioSIIF(BaseModel):
    ejercicio: int =  datetime.date.today().year

    @validator('ejercicio')
    def validate_ejercicio(cls, v):
        if v < 2010 or v > datetime.date.today().year:
            raise ValueError('Ejercicio debe ser un año entre 2010 y la actualidad')
        return v