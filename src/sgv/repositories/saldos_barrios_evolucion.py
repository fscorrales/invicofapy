__all__ = [
    "SaldosBarriosEvolucionRepositoryDependency",
    "SaldosBarriosEvolucionRepository",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import SaldosBarriosEvolucionReport


class SaldosBarriosEvolucionRepository(BaseRepository[SaldosBarriosEvolucionReport]):
    collection_name = "sgv_saldos_barrios_evolucion"
    model = SaldosBarriosEvolucionReport


SaldosBarriosEvolucionRepositoryDependency = Annotated[
    SaldosBarriosEvolucionRepository, Depends()
]
