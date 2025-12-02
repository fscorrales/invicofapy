__all__ = ["Rfondos04RepositoryDependency", "Rfondos04Repository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rfondos04Report


class Rfondos04Repository(BaseRepository[Rfondos04Report]):
    collection_name = "siif_rfondos04"
    model = Rfondos04Report


Rfondos04RepositoryDependency = Annotated[Rfondos04Repository, Depends()]
