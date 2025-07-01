__all__ = ["CtasCtesRepositoryDependency", "CtasCtesRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import CtasCtesReport


class CtasCtesRepository(BaseRepository[CtasCtesReport]):
    collection_name = "icaro_ctas_ctes"
    model = CtasCtesReport


CtasCtesRepositoryDependency = Annotated[CtasCtesRepository, Depends()]
