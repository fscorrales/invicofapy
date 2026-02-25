__all__ = ["Rdeu012b2CuitRepositoryDependency", "Rdeu012b2CuitRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rdeu012b2CuitReport


class Rdeu012b2CuitRepository(BaseRepository[Rdeu012b2CuitReport]):
    collection_name = "siif_rdeu012b2_cuit"
    model = Rdeu012b2CuitReport


Rdeu012b2CuitRepositoryDependency = Annotated[Rdeu012b2CuitRepository, Depends()]
