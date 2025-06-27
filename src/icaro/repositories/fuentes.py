__all__ = ["FuentesRepositoryDependency", "FuentesRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import FuentesReport


class FuentesRepository(BaseRepository[FuentesReport]):
    collection_name = "icaro_fuentes"
    model = FuentesReport


FuentesRepositoryDependency = Annotated[FuentesRepository, Depends()]
