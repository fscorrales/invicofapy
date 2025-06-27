__all__ = ["ObrasRepositoryDependency", "ObrasRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ObrasReport


class ObrasRepository(BaseRepository[ObrasReport]):
    collection_name = "icaro_obras"
    model = ObrasReport


ObrasRepositoryDependency = Annotated[ObrasRepository, Depends()]
