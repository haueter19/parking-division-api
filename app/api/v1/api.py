from fastapi import APIRouter
from app.api.v1.endpoints import auth, uploads, health, transactions, file_status, admin, users
from app.api.v1.endpoints import reports, cash_variance

api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(health.router, tags=["health"])
api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])
api_router.include_router(users.router, tags=["users"])
api_router.include_router(uploads.router, prefix="/files", tags=["file-uploads"])
api_router.include_router(file_status.router, prefix="/files", tags=["file-status"])
api_router.include_router(transactions.router)
api_router.include_router(reports.router)
api_router.include_router(admin.router)
api_router.include_router(cash_variance.router)
