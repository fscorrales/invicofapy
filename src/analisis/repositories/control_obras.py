__all__ = [
    "ControlObrasRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_obras import (
    ControlObrasReport,
)


# -------------------------------------------------
class ControlObrasRepository(BaseRepository[ControlObrasReport]):
    collection_name = "control_obras"
    model = ControlObrasReport


ControlObrasRepositoryDependency = Annotated[ControlObrasRepository, Depends()]
