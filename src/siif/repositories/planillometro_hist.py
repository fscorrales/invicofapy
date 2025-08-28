__all__ = ["PlanillometroHistRepositoryDependency", "PlanillometroHistRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import PlanillometroHistReport


class PlanillometroHistRepository(BaseRepository[PlanillometroHistReport]):
    collection_name = "siif_planillometro_hist"
    model = PlanillometroHistReport


PlanillometroHistRepositoryDependency = Annotated[
    PlanillometroHistRepository, Depends()
]
