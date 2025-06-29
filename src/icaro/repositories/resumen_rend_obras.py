__all__ = ["ResumenRendObrasRepositoryDependency", "ResumenRendObrasRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ResumenRendObrasReport


class ResumenRendObrasRepository(BaseRepository[ResumenRendObrasReport]):
    collection_name = "icaro_resumen_rend_obras"
    model = ResumenRendObrasReport


ResumenRendObrasRepositoryDependency = Annotated[ResumenRendObrasRepository, Depends()]
