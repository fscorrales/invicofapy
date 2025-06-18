__all__ = ["Origen"]

from enum import Enum


# --------------------------------------------------
class Origen(str, Enum):
    epam = "EPAM"
    obras = "OBRAS"
    funcionamiento = "FUNCIONAMIENTO"
