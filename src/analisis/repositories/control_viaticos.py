__all__ = [
    "ControlViaticosPA3RepositoryDependency",
    # "ControlEscribanosSGFvsSSCCRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_viaticos import (
    ControlViaticosPA3Report,
)


# -------------------------------------------------
class ControlViaticosPA3Repository(BaseRepository[ControlViaticosPA3Report]):
    collection_name = "control_viaticos_pa3"
    model = ControlViaticosPA3Report


ControlViaticosPA3RepositoryDependency = Annotated[
    ControlViaticosPA3Repository, Depends()
]


# # -------------------------------------------------
# class ControlEscribanosSGFvsSSCCRepository(
#     BaseRepository[ControlEscribanosSGFvsSSCCReport]
# ):
#     collection_name = "control_escribanos_sgf_vs_sscc"
#     model = ControlEscribanosSGFvsSSCCReport


# ControlEscribanosSGFvsSSCCRepositoryDependency = Annotated[
#     ControlEscribanosSGFvsSSCCRepository, Depends()
# ]
