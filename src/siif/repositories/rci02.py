__all__ = ["Rci02RepositoryDependency", "Rci02Repository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rci02Report


class Rci02Repository(BaseRepository[Rci02Report]):
    collection_name = "siif_rci02"
    model = Rci02Report


Rci02RepositoryDependency = Annotated[Rci02Repository, Depends()]
