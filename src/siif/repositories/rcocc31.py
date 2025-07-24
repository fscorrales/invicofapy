__all__ = ["Rcocc31RepositoryDependency", "Rcocc31Repository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rcocc31Report


class Rcocc31Repository(BaseRepository[Rcocc31Report]):
    collection_name = "siif_rcocc31"
    model = Rcocc31Report


Rcocc31RepositoryDependency = Annotated[Rcocc31Repository, Depends()]
