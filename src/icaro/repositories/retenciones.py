__all__ = ["RetencionesRepositoryDependency", "RetencionesRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import RetencionesReport


class RetencionesRepository(BaseRepository[RetencionesReport]):
    collection_name = "icaro_retenciones"
    model = RetencionesReport


RetencionesRepositoryDependency = Annotated[RetencionesRepository, Depends()]
