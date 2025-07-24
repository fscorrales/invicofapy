__all__ = ["Rvicon03RepositoryDependency", "Rvicon03Repository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rvicon03Report


class Rvicon03Repository(BaseRepository[Rvicon03Report]):
    collection_name = "siif_rvicon03"
    model = Rvicon03Report


Rvicon03RepositoryDependency = Annotated[Rvicon03Repository, Depends()]
