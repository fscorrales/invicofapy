__all__ = ["ControlAnualRepositoryDependency"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.icaro_vs_siif import ControlAnualReport


class ControlAnualRepository(BaseRepository[ControlAnualReport]):
    collection_name = "icaro_vs_siif_control_anual"
    model = ControlAnualReport


ControlAnualRepositoryDependency = Annotated[ControlAnualRepository, Depends()]
