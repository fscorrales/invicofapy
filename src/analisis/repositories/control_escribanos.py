__all__ = [
    "ControlEscribanosSIIFvsSGFRepositoryDependency",
    "ControlEscribanosSGFvsSSCCRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_escribanos import (
    ControlEscribanosSGFvsSSCCReport,
    ControlEscribanosSIIFvsSGFReport,
)


# -------------------------------------------------
class ControlEscribanosSIIFvsSGFRepository(
    BaseRepository[ControlEscribanosSIIFvsSGFReport]
):
    collection_name = "control_escribanos_siif_vs_sgf"
    model = ControlEscribanosSIIFvsSGFReport


ControlEscribanosSIIFvsSGFRepositoryDependency = Annotated[
    ControlEscribanosSIIFvsSGFRepository, Depends()
]


# -------------------------------------------------
class ControlEscribanosSGFvsSSCCRepository(
    BaseRepository[ControlEscribanosSGFvsSSCCReport]
):
    collection_name = "control_escribanos_sgf_vs_sscc"
    model = ControlEscribanosSGFvsSSCCReport


ControlEscribanosSGFvsSSCCRepositoryDependency = Annotated[
    ControlEscribanosSGFvsSSCCRepository, Depends()
]
