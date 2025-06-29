__all__ = ["CertificadosRepositoryDependency", "CertificadosRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import CertificadosReport


class CertificadosRepository(BaseRepository[CertificadosReport]):
    collection_name = "icaro_certificados"
    model = CertificadosReport


CertificadosRepositoryDependency = Annotated[CertificadosRepository, Depends()]
