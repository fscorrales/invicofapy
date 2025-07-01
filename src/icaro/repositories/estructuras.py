__all__ = ["EstructurasRepositoryDependency", "EstructurasRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import EstructurasReport


class EstructurasRepository(BaseRepository[EstructurasReport]):
    collection_name = "icaro_estructuras"
    model = EstructurasReport


EstructurasRepositoryDependency = Annotated[EstructurasRepository, Depends()]
