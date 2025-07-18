__all__ = ["Ri102RepositoryDependency", "Ri102Repository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Ri102Report


class Ri102Repository(BaseRepository[Ri102Report]):
    collection_name = "siif_ri102"
    model = Ri102Report


Ri102RepositoryDependency = Annotated[Ri102Repository, Depends()]
