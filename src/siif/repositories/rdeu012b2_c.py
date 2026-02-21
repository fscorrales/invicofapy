__all__ = ["Rdeu012b2CRepositoryDependency", "Rdeu012b2CRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rdeu012b2CReport


class Rdeu012b2CRepository(BaseRepository[Rdeu012b2CReport]):
    collection_name = "siif_rdeu012b2_c"
    model = Rdeu012b2CReport


Rdeu012b2CRepositoryDependency = Annotated[Rdeu012b2CRepository, Depends()]
