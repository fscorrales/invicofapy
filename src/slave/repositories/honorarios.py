__all__ = ["HonorariosRepositoryDependency", "HonorariosRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import HonorariosReport


class HonorariosRepository(BaseRepository[HonorariosReport]):
    collection_name = "slave_honorarios"
    model = HonorariosReport


HonorariosRepositoryDependency = Annotated[HonorariosRepository, Depends()]
