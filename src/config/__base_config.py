__all__ = [
    "SIIF_USERNAME",
    "SIIF_PASSWORD",
    "MONGODB_URI",
    "logger",
]

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

# Set environment variables
SIIF_USERNAME = os.getenv("SIIF_USERNAME", None)
SIIF_PASSWORD = os.getenv("SIIF_PASSWORD", None)
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://127.0.0.1:27017/invico")


# JWT_SECRET = os.getenv("JWT_SECRET", "super_secret_key")

# HOST_URL = os.getenv("HOST_URL", "localhost")

# FRONTEND_HOST = os.getenv("FRONTEND_HOST", "localhost")

# HOST_PORT = int(os.getenv("HOST_PORT") or 8000)

# UNSPLASH_ACCESS_KEY = os.getenv("UNSPLASH_ACCESS_KEY")

logger = logging.getLogger("uvicorn")
logger.setLevel(logging.DEBUG)

# Fixing a "bycript issue"
logging.getLogger("passlib").setLevel(logging.ERROR)
