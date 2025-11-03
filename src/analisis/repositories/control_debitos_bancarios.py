__all__ = [
    "ControlDebitosBancariosRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_debitos_bancarios import (
    ControlDebitosBancariosReport,
)


# -------------------------------------------------
class ControlDebitosBancariosRepository(BaseRepository[ControlDebitosBancariosReport]):
    collection_name = "control_debitos_bancarios"
    model = ControlDebitosBancariosReport


ControlDebitosBancariosRepositoryDependency = Annotated[
    ControlDebitosBancariosRepository, Depends()
]
