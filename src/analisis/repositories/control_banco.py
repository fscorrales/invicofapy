__all__ = [
    "ControlBancoRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_banco import (
    ControlBancoReport,
)


# -------------------------------------------------
class ControlBancoRepository(BaseRepository[ControlBancoReport]):
    collection_name = "control_banco"
    model = ControlBancoReport


ControlBancoRepositoryDependency = Annotated[ControlBancoRepository, Depends()]
