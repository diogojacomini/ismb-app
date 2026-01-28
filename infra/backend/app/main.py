from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# register API router implemented in app/api/routes.py
from .api import routes as api_routes

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# mount the router under /api
app.include_router(api_routes.router, prefix="/api")
