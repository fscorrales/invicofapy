__all__ = ["ActividadesRepositoryDependency", "ActividadesRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ActividadReport


class ActividadesRepository(BaseRepository[ActividadReport]):
    collection_name = "icaro_actividades"
    model = ActividadReport


ActividadesRepositoryDependency = Annotated[ActividadesRepository, Depends()]
