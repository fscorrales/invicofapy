__all__ = ["CamelModel"]

from pydantic import BaseModel


# ----------------------------------------
# 1. Función para convertir snake_case → camelCase
def to_camel(string: str) -> str:
    parts = string.split("_")
    return parts[0] + "".join(word.capitalize() for word in parts[1:])


# ----------------------------------------
# 2. Clase base para tus modelos con esta configuración
class CamelModel(BaseModel):
    class Config:
        alias_generator = to_camel
        populate_by_name = True
        # Esto permite usar .field_name en tu código
        # aunque el cliente use camelCase
