__all__ = [
    "ControlHaberesRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_haberes import (
    ControlHaberesReport,
)


# -------------------------------------------------
class ControlHaberesRepository(BaseRepository[ControlHaberesReport]):
    collection_name = "control_haberes"
    model = ControlHaberesReport


ControlHaberesRepositoryDependency = Annotated[ControlHaberesRepository, Depends()]
