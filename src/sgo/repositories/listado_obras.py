__all__ = [
    "ListadoObrasRepositoryDependency",
    "ListadoObrasRepository",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ListadoObrasReport


class ListadoObrasRepository(BaseRepository[ListadoObrasReport]):
    collection_name = "sgo_listado_obras"
    model = ListadoObrasReport


ListadoObrasRepositoryDependency = Annotated[ListadoObrasRepository, Depends()]
