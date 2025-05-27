__all__ = ["UsersRepository", "UsersRepositoryDependency"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import PrivateUser


class UsersRepository(BaseRepository[PrivateUser]):
    collection_name = "users"
    model = PrivateUser


UsersRepositoryDependency = Annotated[UsersRepository, Depends()]
