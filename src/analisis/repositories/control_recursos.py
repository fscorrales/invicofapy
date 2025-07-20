__all__ = [
    "ControlRecursosRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_recursos import (
    ControlRecursosReport,
)


# -------------------------------------------------
class ControlRecursosRepository(BaseRepository[ControlRecursosReport]):
    collection_name = "control_recursos"
    model = ControlRecursosReport


ControlRecursosRepositoryDependency = Annotated[ControlRecursosRepository, Depends()]
