from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .analisis.routes import control_router
from .auth.routes import auth_router
from .config import Database
from .icaro.routes import icaro_router
from .sgf.routes import sgf_router
from .siif.routes import siif_router
from .sscc.routes import sscc_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicializar MongoDB
    Database.initialize()
    print("✅ MongoDB initialized")

    yield  # Aquí corre la aplicación

    # Cerrar MongoDB al terminar
    if Database.client:
        Database.client.close()
        print("🛑 MongoDB connection closed")


# tags_metadata = [
#     {"name": "Auth"},
#     {"name": "Users"},
#     {"name": "Products"},
# ]

# app = FastAPI(title="Final Project API", openapi_tags=tags_metadata)
app = FastAPI(title="Final Project API", lifespan=lifespan)

# # Let's include our auth routes aside from the API routes
app.include_router(auth_router)
# Include our API routes
app.include_router(control_router)
app.include_router(siif_router)
app.include_router(sgf_router)
app.include_router(sscc_router)
app.include_router(icaro_router)


# Set up CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# uvicorn src.main:app --loop asyncio
