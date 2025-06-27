__all__ = ["ProveedoresRepositoryDependency", "ProveedoresRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ProveedoresReport


class ProveedoresRepository(BaseRepository[ProveedoresReport]):
    collection_name = "icaro_proveedores"
    model = ProveedoresReport


ProveedoresRepositoryDependency = Annotated[ProveedoresRepository, Depends()]
