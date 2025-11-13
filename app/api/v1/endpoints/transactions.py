"""
Endpoint for processing Payments Insider transaction files
Integrates with existing parking-division-api upload system
"""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Optional
import pandas as pd
import logging
from datetime import datetime

from app.db.session import get_db
from app.api.dependencies import get_current_user
from app.models.database import User, UploadedFile
from app.models.schemas import ProcessTransactionRequest, ProcessTransactionResponse
from app.utils.transaction_processor import TransactionProcessor

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/transactions", tags=["transactions"])


@router.post("/process/{file_id}", response_model=ProcessTransactionResponse)
async def process_transaction_file(
    file_id: int,
    sheet_name: Optional[str] = None,
    if_exists: str = "append",
    background_tasks: BackgroundTasks = None,
    process_async: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Process a Payments Insider transaction file that was previously uploaded
    
    Args:
        file_id: ID of the uploaded file from uploaded_files table
        sheet_name: Optional Excel sheet name
        if_exists: 'append', 'replace', or 'fail'
        process_async: If True, process in background
        db: Database session
        current_user: Authenticated user
    
    Returns:
        Processing status and record count
    """
    # Validate if_exists parameter
    if if_exists not in ['append', 'replace', 'fail']:
        raise HTTPException(
            status_code=400,
            detail="if_exists must be one of: 'append', 'replace', 'fail'"
        )
    
    # Get the uploaded file record
    uploaded_file = db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
    
    if not uploaded_file:
        raise HTTPException(
            status_code=404,
            detail=f"File with ID {file_id} not found"
        )
    
    # Verify this is a Payments Insider file
    if uploaded_file.data_source_type != "payments_insider_cc":
        raise HTTPException(
            status_code=400,
            detail=f"File is not a Payments Insider file. Type: {uploaded_file.data_source_type}"
        )
    
    # Check if already processed
    if uploaded_file.processing_status == "processing":
        raise HTTPException(
            status_code=409,
            detail="File is currently being processed"
        )
    
    # Initialize processor
    processor = TransactionProcessor(db)
    
    if process_async:
        # Process in background
        background_tasks.add_task(
            process_file_background,
            file_id=file_id,
            file_path=uploaded_file.file_path,
            sheet_name=sheet_name,
            if_exists=if_exists,
            db=db
        )
        
        # Update status to processing
        uploaded_file.processing_status = "processing"
        uploaded_file.processed_at = datetime.utcnow()
        db.commit()
        
        return ProcessTransactionResponse(
            success=True,
            file_id=file_id,
            filename=uploaded_file.original_filename,
            status="processing",
            message=f"Processing started for file '{uploaded_file.original_filename}'. Check status later."
        )
    
    else:
        # Process synchronously
        try:
            uploaded_file.processing_status = "processing"
            uploaded_file.processed_at = datetime.utcnow()
            db.commit()
            
            # Process the file
            records_inserted = processor.process_payments_insider_file(
                file_path=uploaded_file.file_path,
                sheet_name=sheet_name,
                if_exists=if_exists
            )
            
            # Update file record
            uploaded_file.processing_status = "completed"
            uploaded_file.records_processed = records_inserted
            db.commit()
            
            return ProcessTransactionResponse(
                success=True,
                file_id=file_id,
                filename=uploaded_file.original_filename,
                status="completed",
                records_inserted=records_inserted,
                message=f"Successfully processed {records_inserted} transaction records"
            )
            
        except ValueError as e:
            # Validation errors
            uploaded_file.processing_status = "failed"
            uploaded_file.processing_error = str(e)
            db.commit()
            
            return ProcessTransactionResponse(
                success=False,
                file_id=file_id,
                filename=uploaded_file.original_filename,
                status="failed",
                message="Data validation failed",
                errors=[str(e)]
            )
            
        except Exception as e:
            # Other errors
            logger.error(f"Error processing file {file_id}: {str(e)}")
            uploaded_file.processing_status = "failed"
            uploaded_file.processing_error = str(e)
            db.commit()
            
            raise HTTPException(
                status_code=500,
                detail=f"Error processing file: {str(e)}"
            )


@router.get("/status/{file_id}")
async def get_processing_status(
    file_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get the processing status of a transaction file
    
    Args:
        file_id: ID of the uploaded file
        db: Database session
        current_user: Authenticated user
    
    Returns:
        Processing status information
    """
    uploaded_file = db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
    
    if not uploaded_file:
        raise HTTPException(
            status_code=404,
            detail=f"File with ID {file_id} not found"
        )
    
    return {
        "file_id": uploaded_file.id,
        "filename": uploaded_file.original_filename,
        "data_source_type": uploaded_file.data_source_type,
        "uploaded_at": uploaded_file.uploaded_at.isoformat(),
        "uploaded_by": uploaded_file.uploaded_by_username,
        "processing_status": uploaded_file.processing_status,
        "processed_at": uploaded_file.processed_at.isoformat() if uploaded_file.processed_at else None,
        "records_processed": uploaded_file.records_processed,
        "processing_error": uploaded_file.processing_error
    }


@router.get("/stats")
async def get_transaction_stats(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get statistics about processed transactions
    
    Returns:
        Transaction statistics
    """
    from sqlalchemy import text
    
    query = text("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT mid) as unique_merchants,
        MIN(transaction_date) as earliest_transaction,
        MAX(transaction_date) as latest_transaction,
        SUM(settled_amount) as total_settled_amount,
        AVG(settled_amount) as avg_settled_amount
    FROM transactions
    """)
    
    result = db.execute(query).fetchone()
    
    if result and result[0] > 0:
        return {
            "total_records": result[0],
            "unique_merchants": result[1],
            "earliest_transaction": result[2].isoformat() if result[2] else None,
            "latest_transaction": result[3].isoformat() if result[3] else None,
            "total_settled_amount": float(result[4]) if result[4] else 0.0,
            "avg_settled_amount": float(result[5]) if result[5] else 0.0
        }
    else:
        return {
            "message": "No transaction data available",
            "total_records": 0
        }


def process_file_background(
    file_id: int,
    file_path: str,
    sheet_name: Optional[str],
    if_exists: str,
    db: Session
):
    """
    Background task for processing transaction files
    
    Args:
        file_id: File ID
        file_path: Path to uploaded file
        sheet_name: Optional Excel sheet name
        if_exists: How to handle existing data
        db: Database session
    """
    processor = TransactionProcessor(db)
    uploaded_file = db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
    
    try:
        records = processor.process_payments_insider_file(
            file_path=file_path,
            sheet_name=sheet_name,
            if_exists=if_exists
        )
        
        uploaded_file.processing_status = "completed"
        uploaded_file.records_processed = records
        db.commit()
        
        logger.info(f"Background processing completed for file {file_id}: {records} records")
        
    except Exception as e:
        logger.error(f"Background processing failed for file {file_id}: {str(e)}")
        uploaded_file.processing_status = "failed"
        uploaded_file.processing_error = str(e)
        db.commit()