__all__ = ["UsersRepository", "UsersRepositoryDependency"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import CreateUser


class UsersRepository(BaseRepository[CreateUser]):
    collection_name = "users"
    model = CreateUser


UsersRepositoryDependency = Annotated[UsersRepository, Depends()]
