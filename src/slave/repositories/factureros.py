__all__ = ["FacturerosRepositoryDependency", "FacturerosRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import FacturerosReport


class FacturerosRepository(BaseRepository[FacturerosReport]):
    collection_name = "slave_factureros"
    model = FacturerosReport


FacturerosRepositoryDependency = Annotated[FacturerosRepository, Depends()]
