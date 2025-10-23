__all__ = [
    "ControlHonorariosSIIFvsSlaveRepositoryDependency",
    "ControlHonorariosSGFvsSlaveRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_honorarios import (
    ControlHonorariosSGFvsSlaveReport,
    ControlHonorariosSIIFvsSlaveReport,
)


# -------------------------------------------------
class ControlHonorariosSIIFvsSlaveRepository(
    BaseRepository[ControlHonorariosSIIFvsSlaveReport]
):
    collection_name = "control_honorarios_siif_vs_slave"
    model = ControlHonorariosSIIFvsSlaveReport


ControlHonorariosSIIFvsSlaveRepositoryDependency = Annotated[
    ControlHonorariosSIIFvsSlaveRepository, Depends()
]


# -------------------------------------------------
class ControlHonorariosSGFvsSlaveRepository(
    BaseRepository[ControlHonorariosSGFvsSlaveReport]
):
    collection_name = "control_honorarios_sgf_vs_slave"
    model = ControlHonorariosSGFvsSlaveReport


ControlHonorariosSGFvsSlaveRepositoryDependency = Annotated[
    ControlHonorariosSGFvsSlaveRepository, Depends()
]
