from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1.api import api_router
from app.db.session import init_db
from app.config import settings
import os

# Create FastAPI application
app = FastAPI(
    title="Parking Division Operations & Revenue Tracking API",
    description="API for tracking parking division operations, revenue, and data uploads",
    version="1.0.0"
)

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router, prefix="/api/v1")

# Serve static files (for the web interface)
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Setup templates directory
templates_dir = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=templates_dir)


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    init_db()
    print("Database initialized successfully")


@app.get("/")
async def root(request: Request):
    """Serve the main web interface (login page)"""
    return templates.TemplateResponse(
        request=request,
        name="index.html"
    )


@app.get("/upload")
async def upload_page(request: Request):
    """Serve the file upload page"""
    return templates.TemplateResponse(
        request=request,
        name="upload.html"
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=True)
