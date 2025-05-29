__all__ = [
    "CreateUser",
    "LoginUser",
    "PublicStoredUser",
    "PrivateStoredUser",
    "PrivateUser",
]

from datetime import datetime
from enum import Enum

from pydantic import AliasChoices, BaseModel, EmailStr, Field, field_validator

from ...utils import PyObjectId, validate_not_empty


# -------------------------------------------------
class RegisterRole(str, Enum):
    user = "user"


# -------------------------------------------------
class Role(str, Enum):
    admin = "admin"
    user = "user"


# -------------------------------------------------
class BaseUser(BaseModel):
    email: EmailStr


class RegisterUser(BaseUser):
    role: RegisterRole = RegisterRole.user
    password: str
    _not_empty = field_validator("email", "password", mode="after")(validate_not_empty)


# -------------------------------------------------
class CreateUser(RegisterUser):
    role: Role = Role.user
    _not_empty = field_validator("email", "password", mode="after")(validate_not_empty)


# -------------------------------------------------
class LoginUser(BaseUser):
    password: str


# -------------------------------------------------
class PrivateUser(BaseUser):
    role: Role
    hash_password: str
    _not_empty = field_validator("email", "hash_password", mode="after")(
        validate_not_empty
    )


# -------------------------------------------------
class PublicStoredUser(PrivateUser):
    deactivated_at: datetime | None = Field(default=None)
    id: PyObjectId = Field(validation_alias=AliasChoices("_id", "id"))


# -------------------------------------------------
class PrivateStoredUser(PublicStoredUser):
    hash_password: str
