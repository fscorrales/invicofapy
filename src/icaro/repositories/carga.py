__all__ = ["CargaRepositoryDependency", "CargaRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import CargaReport


class CargaRepository(BaseRepository[CargaReport]):
    collection_name = "icaro_carga"
    model = CargaReport


CargaRepositoryDependency = Annotated[CargaRepository, Depends()]
