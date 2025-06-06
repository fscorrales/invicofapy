__all__ = ["ResumenRendProvRepositoryDependency"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ResumenRendProvReport


class ResumenRendProvRepository(BaseRepository[ResumenRendProvReport]):
    collection_name = "sgf_resumen_rend_prov"
    model = ResumenRendProvReport


ResumenRendProvRepositoryDependency = Annotated[ResumenRendProvRepository, Depends()]
