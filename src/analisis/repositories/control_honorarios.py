__all__ = [
    "ControlHonorariosRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_honorarios import (
    ControlHonorariosReport,
)


# -------------------------------------------------
class ControlHonorariosRepository(BaseRepository[ControlHonorariosReport]):
    collection_name = "control_honorarios"
    model = ControlHonorariosReport


ControlHonorariosRepositoryDependency = Annotated[
    ControlHonorariosRepository, Depends()
]
