import logging
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


# -------------------------------------------------
class BaseAppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parent.parent / ".env",
        env_file_encoding="utf-8",
        extra="allow",  # Permitir claves adicionales
    )
    APP_ENV: str = "dev"
    SIIF_USERNAME: str | None = None
    SIIF_PASSWORD: str | None = None
    SGF_USERNAME: str | None = None
    SGF_PASSWORD: str | None = None
    SSCC_USERNAME: str | None = None
    SSCC_PASSWORD: str | None = None
    SGV_USERNAME: str | None = None
    SGV_PASSWORD: str | None = None
    SGO_USERNAME: str | None = None
    SGO_PASSWORD: str | None = None
    ADMIN_EMAIL: str | None = None
    ADMIN_PASSWORD: str | None = None
    DB_URI: str = "mongodb://127.0.0.1:27017/invico"
    MONGO_DB_NAME: str = "invico"
    GOOGLE_CREDENTIALS: str | None = None  # JSON credentials for Google Sheets
    JWT_SECRET: str = "super_secret_key"
    # Otros valores opcionales...
    # HOST_URL: str = "localhost"
    # HOST_PORT: int = 8000
    # FRONTEND_HOST: str = "localhost"

    # -------------------------------------------------
    @property
    def debug(self) -> bool:
        return self.APP_ENV == "dev"


# class DevSettings(BaseAppSettings):
#     debug: bool = True


# class ProdSettings(BaseAppSettings):
#     debug: bool = False


# if APP_ENV == "prod":
#     settings = ProdSettings()
# else:
#     settings = DevSettings()

settings = BaseAppSettings()

# Logger
logger = logging.getLogger("uvicorn")
logger.setLevel(logging.DEBUG if settings.debug else logging.INFO)
logging.getLogger("passlib").setLevel(logging.ERROR)
