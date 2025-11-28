__all__ = [
    "ControlViaticosRendicionRepositoryDependency",
    # "ControlEscribanosSGFvsSSCCRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_viaticos import (
    ControlViaticosRendicionReport,
)


# -------------------------------------------------
class ControlViaticosRendicionRepository(
    BaseRepository[ControlViaticosRendicionReport]
):
    collection_name = "control_viaticos_rendicion"
    model = ControlViaticosRendicionReport


ControlViaticosRendicionRepositoryDependency = Annotated[
    ControlViaticosRendicionRepository, Depends()
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
