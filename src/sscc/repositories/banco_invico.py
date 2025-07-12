__all__ = ["BancoINVICORepositoryDependency"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import BancoINVICOReport


class BancoINVICORepository(BaseRepository[BancoINVICOReport]):
    collection_name = "sscc_banco_invico"
    model = BancoINVICOReport


BancoINVICORepositoryDependency = Annotated[BancoINVICORepository, Depends()]
