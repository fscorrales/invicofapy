__all__ = ["Rfondo07tpRepositoryDependency", "Rfondo07tpRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rfondo07tpReport


class Rfondo07tpRepository(BaseRepository[Rfondo07tpReport]):
    collection_name = "siif_rfondo07tp"
    model = Rfondo07tpReport


Rfondo07tpRepositoryDependency = Annotated[Rfondo07tpRepository, Depends()]
