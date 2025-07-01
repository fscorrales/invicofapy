__all__ = ["ActividadesRepositoryDependency", "ActividadesRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ActividadesReport


class ActividadesRepository(BaseRepository[ActividadesReport]):
    collection_name = "icaro_actividades"
    model = ActividadesReport


ActividadesRepositoryDependency = Annotated[ActividadesRepository, Depends()]
