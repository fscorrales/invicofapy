from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from .siif.routes import siif_router

# tags_metadata = [
#     {"name": "Auth"},
#     {"name": "Users"},
#     {"name": "Products"},
# ]

# app = FastAPI(title="Final Project API", openapi_tags=tags_metadata)
app = FastAPI(title="Final Project API")

# Include our API routes
app.include_router(siif_router)
# # Let's include our auth routes aside from the API routes
# app.include_router(auth_router)


# Set up CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
def home(request: Request):
    return {"message": "Hello World"}
