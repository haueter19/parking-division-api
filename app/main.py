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
    """Serve the login page"""
    return templates.TemplateResponse(context={"request": request}, name="index.html")


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    """Main dashboard – post-login hub"""
    return templates.TemplateResponse(name="dashboard.html", context={"request": request})


# ── Revenue Section ──────────────────────────────────────────────────────────

@app.get("/revenue", response_class=HTMLResponse)
async def revenue_landing_page(request: Request):
    """Revenue section landing page"""
    return templates.TemplateResponse(name="revenue_landing.html", context={"request": request})

@app.get("/revenue/upload", response_class=HTMLResponse)
async def upload_page(request: Request):
    """File upload page"""
    return templates.TemplateResponse(name="upload.html", context={"request": request})

@app.get("/revenue/files/status", response_class=HTMLResponse)
async def file_status_page(request: Request):
    """File status dashboard"""
    return templates.TemplateResponse(name="file_status.html", context={"request": request})

@app.get("/revenue/cash-variance", response_class=HTMLResponse)
async def cash_variance_page(request: Request):
    """Cash variance entry page"""
    return templates.TemplateResponse(name="cash_variance.html", context={"request": request})

@app.get("/revenue/reports", response_class=HTMLResponse)
async def reports_page(request: Request):
    """Reports hub"""
    return templates.TemplateResponse(name="reports.html", context={"request": request})

@app.get("/revenue/reports/settle", response_class=HTMLResponse)
async def settle_report_page(request: Request):
    """Settlement report"""
    return templates.TemplateResponse(name="settle_report.html", context={"request": request})

@app.get("/revenue/reports/sources", response_class=HTMLResponse)
async def settle_by_source_page(request: Request):
    """Settled-by-source pivot report"""
    return templates.TemplateResponse(name="settle_by_source.html", context={"request": request})

@app.get("/revenue/reports/revenue", response_class=HTMLResponse)
async def revenue_report_page(request: Request):
    """Revenue by period report"""
    return templates.TemplateResponse(name="revenue_report.html", context={"request": request})


# ── Operations Section ───────────────────────────────────────────────────────

@app.get("/operations", response_class=HTMLResponse)
async def operations_landing_page(request: Request):
    """Operations section landing page"""
    return templates.TemplateResponse(name="operations_landing.html", context={"request": request})


# ── Cityworks Section ────────────────────────────────────────────────────────

@app.get("/cityworks", response_class=HTMLResponse)
async def cityworks_landing_page(request: Request):
    """Cityworks section landing page"""
    return templates.TemplateResponse(name="cityworks_landing.html", context={"request": request})

@app.get("/cityworks/work-orders", response_class=HTMLResponse)
async def cityworks_work_orders_page(request: Request):
    """Cityworks work orders list"""
    return templates.TemplateResponse(name="cityworks.html", context={"request": request})

@app.get("/cityworks/work-orders/detail", response_class=HTMLResponse)
async def cityworks_detail_page(request: Request):
    """Cityworks work order detail/processing"""
    return templates.TemplateResponse(name="cityworks_detail.html", context={"request": request})


# ── Enforcement Section ──────────────────────────────────────────────────────

@app.get("/enforcement", response_class=HTMLResponse)
async def enforcement_landing_page(request: Request):
    """Enforcement section landing page"""
    return templates.TemplateResponse(name="enforcement_landing.html", context={"request": request})


# ── Admin Section ────────────────────────────────────────────────────────────

@app.get("/admin", response_class=HTMLResponse)
async def admin_landing_page(request: Request):
    """Admin section landing page"""
    return templates.TemplateResponse(name="admin_landing.html", context={"request": request})

@app.get("/admin/config", response_class=HTMLResponse)
async def admin_config_page(request: Request):
    """Admin configuration page"""
    return templates.TemplateResponse(name="admin.html", context={"request": request})


# ── TDM Section ──────────────────────────────────────────────────────────────

@app.get("/tdm", response_class=HTMLResponse)
async def tdm_landing_page(request: Request):
    """TDM section landing page"""
    return templates.TemplateResponse(name="tdm_landing.html", context={"request": request})


# ── Data & Analytics Section ─────────────────────────────────────────────────

@app.get("/analytics", response_class=HTMLResponse)
async def analytics_landing_page(request: Request):
    """Data & Analytics section landing page"""
    return templates.TemplateResponse(name="analytics_landing.html", context={"request": request})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=True)
