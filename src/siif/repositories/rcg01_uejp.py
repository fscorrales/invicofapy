__all__ = ["Rcg01UejpRepositoryDependency"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rcg01UejpReport


class Rcg01UejpRepository(BaseRepository[Rcg01UejpReport]):
    collection_name = "siif_rcg01_uejp"
    model = Rcg01UejpReport


Rcg01UejpRepositoryDependency = Annotated[Rcg01UejpRepository, Depends()]
