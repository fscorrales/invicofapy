__all__ = [
    "Authentication",
    "AuthenticationDependency",
    "AuthorizationDependency",
    "OptionalAuthorizationDependency",
]

from datetime import timedelta
from typing import Annotated, Optional

from fastapi import Depends, HTTPException, Response, Security, status
from fastapi_jwt import JwtAccessBearer, JwtAuthorizationCredentials
from passlib.context import CryptContext

from ...config import settings
from ..schemas import LoginUser, PublicStoredUser

# Set token expiration time (1 day) locally
token_expiration_time = timedelta(days=1)

# Seguridad obligatoria (con candado 🔒)
access_security = JwtAccessBearer(
    secret_key=settings.JWT_SECRET,
    auto_error=True,
)

# Seguridad opcional (no lanza error si no hay token)
access_security_optional = JwtAccessBearer(
    secret_key=settings.JWT_SECRET,
    auto_error=False,
)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# -------------------------------------------------
class Authentication:
    # -------------------------------------------------
    @staticmethod
    def verify_password(plain_password, hashed_password):
        return pwd_context.verify(plain_password, hashed_password)

    # -------------------------------------------------
    @staticmethod
    def get_password_hash(password):
        return pwd_context.hash(password)

    # -------------------------------------------------
    def login_and_set_access_token(
        self, db_user: dict | None, user: LoginUser, response: Response
    ):
        if not db_user or not self.verify_password(
            user.password, db_user.get("hash_password")
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or credentials incorrect",
            )

        userdata = PublicStoredUser.model_validate(db_user).model_dump()
        access_token = access_security.create_access_token(
            subject=userdata, expires_delta=token_expiration_time
        )
        access_security.set_access_cookie(response, access_token)

        return {"access_token": access_token}


AuthenticationDependency = Annotated[
    Authentication,
    Depends(),
]


# -------------------------------------------------
class Authorization:
    # -------------------------------------------------
    def __init__(self, credentials: Optional[JwtAuthorizationCredentials] = None):
        self.auth_user_id = None
        self.auth_user_name = None
        self.auth_user_role = None

        if credentials:
            self.auth_user_id = credentials.subject.get("id")
            self.auth_user_name = credentials.subject.get("username")
            self.auth_user_role = credentials.subject.get("role")

    # -------------------------------------------------
    @property
    def is_admin(self) -> bool:
        return self.auth_user_role == "admin"

    # -------------------------------------------------
    @property
    def is_user(self) -> bool:
        return self.auth_user_role == "user"

    # -------------------------------------------------
    def is_admin_or_raise(self):
        if not self.is_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin access required",
            )


# ------------ DEPENDENCIAS ------------

# 🔒 Token obligatorio
AuthCredentials = Annotated[JwtAuthorizationCredentials, Security(access_security)]


def get_authorization(credentials: AuthCredentials) -> Authorization:
    return Authorization(credentials)


AuthorizationDependency = Annotated[Authorization, Depends(get_authorization)]

# 🟢 Token opcional
OptionalAuthCredentials = Annotated[
    Optional[JwtAuthorizationCredentials], Security(access_security_optional)
]


def get_optional_authorization(credentials: OptionalAuthCredentials) -> Authorization:
    return Authorization(credentials)


OptionalAuthorizationDependency = Annotated[
    Authorization, Depends(get_optional_authorization)
]
