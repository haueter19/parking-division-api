from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import text, func, case
from typing import Optional, List
from datetime import datetime

from app.db.session import get_db
from app.api.dependencies import get_current_active_user
from app.models.database import User, UserRole, UploadedFile, ETLProcessingLog, DataSourceType
from app.models.schemas import (
    FileStatusResponse, 
    FileStatusListResponse,
    ProcessETLRequest,
    ProcessETLResponse
)
from app.utils.etl_processor import ETLProcessor, DataLoader
from app.utils import etl_cache

router = APIRouter()


@router.get("/status", response_model=FileStatusListResponse)
async def get_files_status(
    skip: int = 0,
    limit: int = 100,
    data_source_type: Optional[DataSourceType] = None,
    status_filter: Optional[str] = None,  # 'complete', 'not_complete', 'failed', 'not_started'
    sort_by: str = "id",  # Column to sort by
    sort_order: str = "asc",  # 'asc' or 'desc'
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Get list of uploaded files with their ETL processing status
    
    Combines uploaded_files table with etl_processing_log to show:
    - Files uploaded but not yet loaded to staging
    - Files in staging but not yet in final transactions table
    - Completed files
    - Failed processing attempts
    """
    
    # Build filter clauses first
    data_source_filter = ""
    if data_source_type:
        data_source_filter = f"AND uf.data_source_type = '{data_source_type.value}'"
    
    status_filter_clause = ""
    if status_filter:
        if status_filter == 'complete':
            status_filter_clause = "AND MAX(CASE WHEN etl.status = 'completed' THEN 1 ELSE 0 END) = 1"
        elif status_filter == 'not_complete':
            status_filter_clause = """
            AND uf.records_processed IS NOT NULL 
            AND MAX(CASE WHEN etl.status = 'completed' THEN 1 ELSE 0 END) = 0
            """
        elif status_filter == 'failed':
            status_filter_clause = "AND MAX(CASE WHEN etl.status = 'failed' THEN 1 ELSE 0 END) = 1"
        elif status_filter == 'not_started':
            status_filter_clause = "AND uf.records_processed IS NULL"
    
    # Build the complex query with formatted filter clauses
    query_string = """
    SELECT
        uf.id,
        uf.original_filename,
        uf.file_size,
        uf.data_source_type,
        uf.upload_date,
        uf.processed_at,
        uf.records_processed,
        uf.description,
        uf.uploaded_by,
        CASE 
            WHEN uf.records_processed IS NULL THEN 'not_started'
            WHEN MAX(CASE WHEN etl.status = 'completed' THEN 1 ELSE 0 END) = 1 THEN 'complete'
            WHEN MAX(CASE WHEN etl.status = 'running' THEN 1 ELSE 0 END) = 1 THEN 'in_progress'
            WHEN MAX(CASE WHEN etl.status = 'failed' THEN 1 ELSE 0 END) = 1 THEN 'failed'
            ELSE 'not_complete'
        END AS status,
        MAX(etl.records_created) AS records_created,
        MAX(etl.records_failed) AS records_failed,
        MAX(etl.error_message) AS error_message,
        CASE 
            WHEN uf.records_processed > 0 AND SUM(etl.records_created) > 0 
            THEN (CAST(SUM(etl.records_created) AS FLOAT) / CAST(uf.records_processed AS FLOAT)) * 100.0
            ELSE NULL
        END AS percent_complete,
        -- Computed flags
        CASE 
            WHEN uf.records_processed > 0 AND COALESCE(MAX(etl.records_created), 0) < uf.records_processed 
            THEN 1 ELSE 0 
        END AS needs_etl,
        CASE
            WHEN uf.records_processed > 0 AND MAX(CASE WHEN etl.status = 'running' THEN 1 ELSE 0 END) = 0
            THEN 1 ELSE 0
        END AS can_process
    FROM app.uploaded_files uf
    LEFT JOIN app.etl_processing_log etl ON (uf.id = etl.source_file_id)
    WHERE 1=1
        {data_source_filter}
        {status_filter}
    GROUP BY 
        uf.id, uf.original_filename, uf.file_size, uf.data_source_type,
        uf.upload_date, uf.processed_at, uf.records_processed, uf.description, uf.uploaded_by
    ORDER BY uf.{sort_col} {sort_dir}
    OFFSET :skip ROWS
    FETCH NEXT :limit ROWS ONLY
    """.format(
        data_source_filter=data_source_filter,
        status_filter=status_filter_clause,
        sort_col=sort_by,
        sort_dir=sort_order.upper()
    )
    
    # Wrap the formatted query with text()
    query = text(query_string)
    
    # Execute query
    result = db.execute(
        query,
        {"skip": skip, "limit": limit}
    )
    
    rows = result.fetchall()
    
    # Get total count for pagination
    count_query_string = """
    SELECT COUNT(DISTINCT uf.id)
    FROM app.uploaded_files uf
    LEFT JOIN app.etl_processing_log etl ON (uf.id = etl.source_file_id)
    WHERE 1=1
        {data_source_filter}
        {status_filter}
    """.format(
        data_source_filter=data_source_filter,
        status_filter=status_filter_clause
    )

    count_query = text(count_query_string)
    total = db.execute(count_query).scalar()
    
    # Convert rows to response objects
    items = []
    for row in rows:
        # Normalize data_source_type: DB may return enum NAME (e.g. 'PAYMENTS_INSIDER_PAYMENTS')
        # or the enum value (e.g. 'pi_payments'). Pydantic expects the DataSourceType value.
        dst = row.data_source_type
        try:
            if dst is None:
                normalized_dst = None
            elif isinstance(dst, DataSourceType):
                normalized_dst = dst
            elif isinstance(dst, str):
                # If DB returned the enum member name, map via __members__
                if dst in DataSourceType.__members__:
                    normalized_dst = DataSourceType[dst]
                else:
                    # Otherwise assume it's the enum value
                    normalized_dst = DataSourceType(dst)
            else:
                normalized_dst = DataSourceType(str(dst))
        except Exception:
            # Fallback: pass raw value (Pydantic will raise if invalid)
            normalized_dst = dst

        items.append(FileStatusResponse(
            id=row.id,
            original_filename=row.original_filename,
            file_size=row.file_size,
            data_source_type=normalized_dst,
            upload_date=row.upload_date,
            description=row.description,
            processed_at=row.processed_at,
            records_processed=row.records_processed,
            status=row.status,
            records_created=row.records_created,
            records_failed=row.records_failed,
            error_message=row.error_message,
            percent_complete=row.percent_complete,
            needs_etl=bool(row.needs_etl),
            can_process=bool(row.can_process)
        ))
    
    total_pages = (total + limit - 1) // limit if limit > 0 else 0
    
    return FileStatusListResponse(
        total=total,
        items=items,
        page=skip // limit + 1 if limit > 0 else 1,
        page_size=limit,
        total_pages=total_pages
    )


@router.post("/{file_id}/load-to-staging", response_model=ProcessETLResponse)
async def load_file_to_staging(
    file_id: int,
    background_tasks: BackgroundTasks,
    process_async: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Load an uploaded file to its staging table
    This is the first step: file -> staging table
    
    Requires: UPLOADER, MANAGER, or ADMIN role
    """
    if current_user.role not in [UserRole.UPLOADER, UserRole.MANAGER, UserRole.ADMIN]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions"
        )
    
    # Get file record
    file_record = db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
    if not file_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File {file_id} not found"
        )
    
    # Check if already processed to staging
    if file_record.is_processed:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File has already been loaded to staging"
        )
    
    try:
        loader = DataLoader(db, file_record.data_source_type)
        
        # Get the appropriate loader function
        load_function = loader.mapping.get(file_record.data_source_type)
        if not load_function:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"No loader available for data source type: {file_record.data_source_type}"
            )
        
        # Load to staging
        records_loaded = load_function(file_record.file_path, file_id)
        
        return ProcessETLResponse(
            success=True,
            file_id=file_id,
            message=f"Successfully loaded {records_loaded} records to staging",
            records_created=records_loaded,
            records_failed=0,
            errors=None
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error loading file to staging: {str(e)}"
        )


@router.post("/{file_id}/process-etl", response_model=ProcessETLResponse)
async def process_file_to_final(
    file_id: int,
    request: ProcessETLRequest = ProcessETLRequest(),
    background_tasks: BackgroundTasks = None,
    process_async: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Process staging records to final transactions table
    This is the second step: staging -> transactions table
    
    Requires: MANAGER or ADMIN role
    """
    if current_user.role not in [UserRole.MANAGER, UserRole.ADMIN]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions"
        )
    
    # Get file record
    file_record = db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
    if not file_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File {file_id} not found"
        )
    
    # Check if file has been loaded to staging
    if not file_record.is_processed:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be loaded to staging before ETL processing"
        )
    
    # Check if already running
    active_log = db.query(ETLProcessingLog).filter(
        ETLProcessingLog.source_file_id == file_id,
        ETLProcessingLog.status == 'running'
    ).first()
    
    if active_log:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="ETL processing is already running for this file"
        )
    
    try:
        # Get cached lookups initialized at startup
        org_code_cache = etl_cache.get_org_code_cache()
        location_cache = etl_cache.get_location_cache()
        
        processor = ETLProcessor(
            db,
            org_code_cache=org_code_cache,
            location_from_charge_code=location_cache
        )
        
        # Determine which staging table to process based on data source type
        source_table_map = {
            DataSourceType.WINDCAVE: "windcave",
            DataSourceType.PAYMENTS_INSIDER_SALES: "payments_insider",
            DataSourceType.PAYMENTS_INSIDER_PAYMENTS: "payments_insider",
            DataSourceType.IPS_CC: "ips_cc",
            DataSourceType.IPS_MOBILE: "ips_mobile",
            DataSourceType.IPS_CASH: "ips_cash"
        }
        
        source_table = source_table_map.get(file_record.data_source_type)
        if not source_table:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"No ETL processor available for: {file_record.data_source_type}"
            )
        
        # Process based on source type
        if source_table == "windcave":
            result = processor.process_windcave(file_id)
        elif source_table == "payments_insider":
            result = processor.process_payments_insider(file_id)
        elif source_table == "ips_cc":
            result = processor.process_ips_cc(file_id)
        elif source_table == "ips_mobile":
            result = processor.process_ips_mobile(file_id)
        elif source_table == "ips_cash":
            result = processor.process_ips_cash(file_id)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown source table: {source_table}"
            )
        
        return ProcessETLResponse(
            success=result.get("success", False),
            file_id=file_id,
            message=f"ETL processing completed for {file_record.original_filename}",
            records_created=result.get("records_created"),
            records_failed=result.get("records_failed"),
            errors=None
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during ETL processing: {str(e)}"
        )


@router.get("/{file_id}/status", response_model=FileStatusResponse)
async def get_file_status(
    file_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Get detailed status for a single file
    """
    query = text("""
    SELECT
        uf.id,
        uf.original_filename,
        uf.file_size,
        uf.data_source_type,
        uf.upload_date,
        uf.processed_at,
        uf.records_processed,
        uf.description,
        CASE 
            WHEN uf.records_processed IS NULL THEN 'not_started'
            WHEN MAX(CASE WHEN etl.status = 'completed' THEN 1 ELSE 0 END) = 1 THEN 'complete'
            WHEN MAX(CASE WHEN etl.status = 'running' THEN 1 ELSE 0 END) = 1 THEN 'in_progress'
            WHEN MAX(CASE WHEN etl.status = 'failed' THEN 1 ELSE 0 END) = 1 THEN 'failed'
            ELSE 'not_complete'
        END AS status,
        MAX(etl.records_created) AS records_created,
        MAX(etl.records_failed) AS records_failed,
        MAX(etl.error_message) AS error_message,
        CASE 
            WHEN uf.records_processed > 0 AND SUM(etl.records_created) > 0 
            THEN (CAST(SUM(etl.records_created) AS FLOAT) / CAST(uf.records_processed AS FLOAT)) * 100.0
            ELSE NULL
        END AS percent_complete,
        CASE 
            WHEN uf.records_processed > 0 AND COALESCE(MAX(etl.records_created), 0) < uf.records_processed 
            THEN 1 ELSE 0 
        END AS needs_etl,
        CASE
            WHEN uf.records_processed > 0 AND MAX(CASE WHEN etl.status = 'running' THEN 1 ELSE 0 END) = 0
            THEN 1 ELSE 0
        END AS can_process
    FROM app.uploaded_files uf
    LEFT JOIN app.etl_processing_log etl ON (uf.id = etl.source_file_id)
    WHERE uf.id = :file_id
    GROUP BY 
        uf.id, uf.original_filename, uf.file_size, uf.data_source_type,
        uf.upload_date, uf.processed_at, uf.records_processed, uf.description
    """)
    
    result = db.execute(query, {"file_id": file_id})
    row = result.fetchone()
    
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File {file_id} not found"
        )
    
    # Normalize data_source_type for Pydantic enum validation (handle enum NAME vs value)
    dst = row.data_source_type
    try:
        if dst is None:
            normalized_dst = None
        elif isinstance(dst, DataSourceType):
            normalized_dst = dst
        elif isinstance(dst, str):
            if dst in DataSourceType.__members__:
                normalized_dst = DataSourceType[dst]
            else:
                normalized_dst = DataSourceType(dst)
        else:
            normalized_dst = DataSourceType(str(dst))
    except Exception:
        normalized_dst = dst

    return FileStatusResponse(
        id=row.id,
        original_filename=row.original_filename,
        file_size=row.file_size,
        data_source_type=normalized_dst,
        upload_date=row.upload_date,
        description=row.description,
        processed_at=row.processed_at,
        records_processed=row.records_processed,
        status=row.status,
        records_created=row.records_created,
        records_failed=row.records_failed,
        error_message=row.error_message,
        percent_complete=row.percent_complete,
        needs_etl=bool(row.needs_etl),
        can_process=bool(row.can_process)
    )


