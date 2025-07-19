__all__ = ["RfpP605bRepositoryDependency", "RfpP605bRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import RfpP605bReport


class RfpP605bRepository(BaseRepository[RfpP605bReport]):
    collection_name = "siif_rfp_p605b"
    model = RfpP605bReport


RfpP605bRepositoryDependency = Annotated[RfpP605bRepository, Depends()]
