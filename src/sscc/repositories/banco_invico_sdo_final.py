__all__ = ["BancoINVICOSdoFinalRepositoryDependency", "BancoINVICOSdoFinalRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import BancoINVICOSdoFinalReport


class BancoINVICOSdoFinalRepository(BaseRepository[BancoINVICOSdoFinalReport]):
    collection_name = "sscc_banco_invico_sdo_final"
    model = BancoINVICOSdoFinalReport


BancoINVICOSdoFinalRepositoryDependency = Annotated[
    BancoINVICOSdoFinalRepository, Depends()
]
