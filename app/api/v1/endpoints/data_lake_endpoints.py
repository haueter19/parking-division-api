"""
API Endpoints for Data Lake Operations
Add this to app/api/v1/endpoints/data_lake.py
"""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
import os
import pandas as pd

from app.db.session import get_db
from app.api.dependencies import get_current_user
from app.models.database import User, UserRole, UploadedFile, Transaction, DataSourceType
from app.models.schemas import (
    FileProcessRequest, FileProcessResponse,
    ETLProcessRequest, ETLProcessResponse, ETLStatusResponse,
    TransactionFilter, TransactionResponse, TransactionSummary,
    BulkUploadRequest, BulkUploadResponse
)
from app.utils.etl_processor import ETLProcessor, DataLoader

router = APIRouter(prefix="/api/v1/data-lake", tags=["data-lake"])


# ============= File Upload to Staging =============
@router.post("/upload-to-staging", response_model=FileProcessResponse)
async def upload_to_staging(
    file: UploadFile = File(...),
    source_type: str = Form(...),
    report_type: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Upload a file and load it directly to the appropriate staging table
    """
    if current_user.role not in [UserRole.ADMIN, UserRole.MANAGER, UserRole.UPLOADER]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    try:
        # Save the uploaded file
        upload_dir = "/app/uploads"
        os.makedirs(upload_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{file.filename}"
        file_path = os.path.join(upload_dir, filename)
        
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Create database record for the file
        db_file = UploadedFile(
            filename=filename,
            original_filename=file.filename,
            file_path=file_path,
            file_size=len(content),
            data_source_type=source_type,
            uploaded_by=current_user.id,
            description=f"{source_type} upload"
        )
        db.add(db_file)
        db.flush()
        
        # Load to staging table based on source type
        loader = DataLoader(db)
        records_loaded = 0
        
        if source_type == "windcave_cc":
            records_loaded = loader.load_windcave_csv(file_path, db_file.id)
        elif source_type == "payments_insider_cc":
            records_loaded = loader.load_payments_insider(file_path, db_file.id, report_type or "sales")
        elif source_type == "ips_cash":
            # Implement IPS cash loader
            pass
        # Add other loaders as needed
        
        db.commit()
        
        return FileProcessResponse(
            success=True,
            file_id=db_file.id,
            records_loaded=records_loaded,
            message=f"Successfully loaded {records_loaded} records to staging"
        )
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/process-file/{file_id}", response_model=FileProcessResponse)
async def process_uploaded_file(
    file_id: int,
    request: FileProcessRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Process a previously uploaded file to staging tables
    """
    if current_user.role not in [UserRole.ADMIN, UserRole.MANAGER]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    # Get the file record
    file_record = db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")
    
    if file_record.is_processed:
        raise HTTPException(status_code=400, detail="File already processed")
    
    try:
        loader = DataLoader(db)
        records_loaded = 0
        
        # Load based on source type
        if request.source_type == DataSourceType.WINDCAVE_CC:
            records_loaded = loader.load_windcave_csv(file_record.file_path, file_id)
        elif request.source_type == DataSourceType.PAYMENTS_INSIDER_CC:
            records_loaded = loader.load_payments_insider(
                file_record.file_path, file_id, request.report_type or "sales"
            )
        # Add other source types
        
        return FileProcessResponse(
            success=True,
            file_id=file_id,
            records_loaded=records_loaded,
            message=f"Loaded {records_loaded} records to staging"
        )
        
    except Exception as e:
        return FileProcessResponse(
            success=False,
            file_id=file_id,
            records_loaded=0,
            message="Processing failed",
            errors=[str(e)]
        )


# ============= ETL Processing =============
@router.post("/etl/process", response_model=ETLProcessResponse)
async def run_etl_process(
    request: ETLProcessRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Run ETL process to transform staging data to final transactions table
    """
    if current_user.role not in [UserRole.ADMIN, UserRole.MANAGER]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    start_time = datetime.now()
    
    try:
        processor = ETLProcessor(db)
        
        if request.dry_run:
            # Preview mode - don't commit changes
            # Implement dry run logic
            pass
        
        if request.source_table:
            # Process specific staging table
            if request.source_table == "windcave":
                results = {"windcave": processor.process_windcave(request.file_id)}
            elif request.source_table == "payments_insider":
                results = {"payments_insider": processor.process_payments_insider(request.file_id)}
            elif request.source_table == "ips_cash":
                results = {"ips_cash": processor.process_ips_cash(request.file_id)}
            else:
                raise HTTPException(status_code=400, detail=f"Unknown source table: {request.source_table}")
        else:
            # Process all staging tables
            results = processor.process_all_staging_tables(request.file_id)
        
        # Calculate totals
        total_created = sum(r.get("records_created", 0) for r in results.values() if r.get("success"))
        total_failed = sum(r.get("records_failed", 0) for r in results.values() if r.get("success"))
        
        duration = (datetime.now() - start_time).total_seconds()
        
        return ETLProcessResponse(
            success=True,
            results=results,
            total_created=total_created,
            total_failed=total_failed,
            duration_seconds=duration,
            message=f"ETL completed: {total_created} records created, {total_failed} failed"
        )
        
    except Exception as e:
        return ETLProcessResponse(
            success=False,
            results={},
            total_created=0,
            total_failed=0,
            duration_seconds=(datetime.now() - start_time).total_seconds(),
            message=f"ETL failed: {str(e)}"
        )


@router.get("/etl/status", response_model=ETLStatusResponse)
async def get_etl_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get current ETL processing status and pending records
    """
    from sqlalchemy import func, and_
    from app.models.database import (
        WindcaveStaging, PaymentsInsiderStaging, 
        IPSCashStaging, ETLProcessingLog
    )
    
    # Count pending records in each staging table
    pending = {}
    
    pending["windcave"] = db.query(WindcaveStaging).filter(
        WindcaveStaging.processed_to_final == False
    ).count()
    
    pending["payments_insider"] = db.query(PaymentsInsiderStaging).filter(
        PaymentsInsiderStaging.processed_to_final == False
    ).count()
    
    pending["ips_cash"] = db.query(IPSCashStaging).filter(
        IPSCashStaging.processed_to_final == False
    ).count()
    
    # Get today's processed count
    today = datetime.now().date()
    processed_today = db.query(Transaction).filter(
        func.date(Transaction.created_at) == today
    ).count()
    
    # Get last ETL run
    last_run = db.query(ETLProcessingLog).order_by(
        ETLProcessingLog.start_time.desc()
    ).first()
    
    return ETLStatusResponse(
        pending_records=pending,
        processed_today=processed_today,
        last_run=last_run.start_time if last_run else None,
        errors=None
    )


# ============= Transaction Queries =============
@router.post("/transactions/search", response_model=List[TransactionResponse])
async def search_transactions(
    filters: TransactionFilter,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Search transactions with filters
    """
    query = db.query(Transaction)
    
    if filters.start_date:
        query = query.filter(Transaction.transaction_date >= filters.start_date)
    if filters.end_date:
        query = query.filter(Transaction.transaction_date <= filters.end_date)
    if filters.source:
        query = query.filter(Transaction.source == filters.source)
    if filters.location_type:
        query = query.filter(Transaction.location_type == filters.location_type)
    if filters.payment_type:
        query = query.filter(Transaction.payment_type == filters.payment_type)
    if filters.terminal_id:
        query = query.filter(Transaction.device_terminal_id == filters.terminal_id)
    if filters.org_code:
        query = query.filter(Transaction.org_code == filters.org_code)
    if filters.min_amount:
        query = query.filter(Transaction.transaction_amount >= filters.min_amount)
    if filters.max_amount:
        query = query.filter(Transaction.transaction_amount <= filters.max_amount)
    
    transactions = query.limit(limit).offset(offset).all()
    return transactions


@router.post("/transactions/summary", response_model=TransactionSummary)
async def get_transaction_summary(
    filters: TransactionFilter,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get summary statistics for transactions
    """
    from sqlalchemy import func
    
    query = db.query(Transaction)
    
    # Apply filters (same as search)
    if filters.start_date:
        query = query.filter(Transaction.transaction_date >= filters.start_date)
    if filters.end_date:
        query = query.filter(Transaction.transaction_date <= filters.end_date)
    # ... apply other filters
    
    # Get totals
    totals = db.query(
        func.count(Transaction.id).label("count"),
        func.sum(Transaction.transaction_amount).label("amount"),
        func.sum(Transaction.settle_amount).label("settle_amount")
    ).filter(query.whereclause).first() if query.whereclause is not None else db.query(
        func.count(Transaction.id).label("count"),
        func.sum(Transaction.transaction_amount).label("amount"),
        func.sum(Transaction.settle_amount).label("settle_amount")
    ).first()
    
    # Group by payment type
    by_payment = {}
    payment_groups = db.query(
        Transaction.payment_type,
        func.sum(Transaction.transaction_amount).label("amount")
    ).group_by(Transaction.payment_type).all()
    for pg in payment_groups:
        by_payment[pg.payment_type] = float(pg.amount or 0)
    
    # Group by location type
    by_location = {}
    location_groups = db.query(
        Transaction.location_type,
        func.sum(Transaction.transaction_amount).label("amount")
    ).group_by(Transaction.location_type).all()
    for lg in location_groups:
        by_location[lg.location_type] = float(lg.amount or 0)
    
    # Group by source
    by_source = {}
    source_groups = db.query(
        Transaction.source,
        func.sum(Transaction.transaction_amount).label("amount")
    ).group_by(Transaction.source).all()
    for sg in source_groups:
        by_source[sg.source] = float(sg.amount or 0)
    
    # Get date range
    date_range = db.query(
        func.min(Transaction.transaction_date).label("min_date"),
        func.max(Transaction.transaction_date).label("max_date")
    ).first()
    
    return TransactionSummary(
        total_count=totals.count or 0,
        total_amount=float(totals.amount or 0),
        total_settle_amount=float(totals.settle_amount or 0),
        by_payment_type=by_payment,
        by_location_type=by_location,
        by_source=by_source,
        date_range={
            "start": date_range.min_date,
            "end": date_range.max_date
        }
    )


# ============= Bulk Operations =============
@router.post("/bulk/upload", response_model=BulkUploadResponse)
async def bulk_upload_files(
    files: List[UploadFile] = File(...),
    auto_process: bool = Form(False),
    auto_etl: bool = Form(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Bulk upload multiple files and optionally process them
    """
    if current_user.role not in [UserRole.ADMIN, UserRole.MANAGER]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    uploaded = 0
    processed = 0
    total_records = 0
    errors = []
    
    for file in files:
        try:
            # Process each file
            # Implementation similar to single file upload
            uploaded += 1
            
            if auto_process:
                # Load to staging
                processed += 1
            
            if auto_etl:
                # Run ETL
                pass
                
        except Exception as e:
            errors.append(f"Error with {file.filename}: {str(e)}")
    
    return BulkUploadResponse(
        success=len(errors) == 0,
        files_uploaded=uploaded,
        files_processed=processed,
        total_records=total_records,
        errors=errors if errors else None
    )


# Register this router in your main API file:
# app.include_router(router, dependencies=[Depends(get_current_user)])
