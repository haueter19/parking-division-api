from datetime import datetime, timedelta
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from app.api.v1.api import api_router
from app.db.session import init_db, SessionLocalTraffic, SessionLocal
from app.config import settings
#from app.utils import etl_cache
from app.utils.etl_processor import ETLProcessor
import os
import logging

logger = logging.getLogger(__name__)

# Ensure basic logging is configured so logger.info/DEBUG messages appear
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

# Make sure uvicorn loggers are at INFO level as well
logging.getLogger("uvicorn.error").setLevel(logging.INFO)
logging.getLogger("uvicorn.access").setLevel(logging.INFO)

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
    """Initialize database and ETL caches on startup"""
    init_db()
    print("Database initialized successfully")
    
    # Initialize ETL lookup caches
    try:
        # Open primary and traffic DB sessions and provide them to cache initializer
        #traffic_db = SessionLocalTraffic()
        primary_db = SessionLocal()
        try:
            yesterday_date = datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d')
            # Check if ETL cache for ZMS Cash Regular data is populated; if not, process it
            result = primary_db.execute(text("""
                                    SELECT count(*) FROM PUReporting.app.fact_transaction
                                    WHERE staging_table = 'zms_cash_regular'
                                    AND settle_date = :process_date
                                    """), {"process_date": yesterday_date})
            record_count = result.scalar()
            if record_count and record_count > 0:
                print(f"ZMS Cash Regular data already processed for {yesterday_date}")
            else:
                processor = ETLProcessor(db = primary_db)
                success = processor.process_zms_cash(process_date=yesterday_date)
                print(f"Processed {success['records_processed']} records for zms_cash_regular with {success['records_failed']} failures.")

        finally:
            # Ensure both sessions are closed
            #traffic_db.close()
            primary_db.close()
        
    except Exception as e:
        logger.error(f"Error during ETL cache initialization: {e}", exc_info=True)
        print(f"Warning: Could not initialize ETL caches: {e}")


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

@app.get("/files/status", response_class=HTMLResponse)
async def file_status_page(request: Request):
    """
    Serve the file status dashboard page
    
    This endpoint renders the file status HTML template that allows users to:
    - View all uploaded files with their processing status
    - Filter by status and data source type
    - Load files to staging tables
    - Process files through ETL pipeline
    - View detailed error messages
    """
    return templates.TemplateResponse(
        request=request,
        name="file_status.html"
    )


@app.get("/reports/settle", response_class=HTMLResponse)
async def settle_report_page(request: Request):
    """Serve the settlement report page - date-range selector and results"""
    return templates.TemplateResponse(request=request, name="settle_report.html")


@app.get("/reports/sources", response_class=HTMLResponse)
async def settle_by_source_page(request: Request):
    """Serve the pivoted settled-by-source report page"""
    return templates.TemplateResponse(request=request, name="settle_by_source.html")


@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    """Serve the admin configuration page"""
    return templates.TemplateResponse(
        request=request,
        name="admin.html"
    )


@app.get("/cash_variance", response_class=HTMLResponse)
async def cash_variance_page(request: Request):
    """Serve the cash variance entry page"""
    return templates.TemplateResponse(
        request=request,
        name="cash_variance.html"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=True)
