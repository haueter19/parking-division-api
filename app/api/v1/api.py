from fastapi import APIRouter
from app.api.v1.endpoints import auth, uploads, health, transactions

api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(health.router, tags=["health"])
api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])
api_router.include_router(uploads.router, prefix="/files", tags=["file-uploads"])
api_router.include_router(transactions.router)