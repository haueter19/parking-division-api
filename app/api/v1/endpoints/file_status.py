from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Request
from sqlalchemy.orm import Session
from sqlalchemy import text, func, case
from typing import Optional, List
from datetime import datetime
import threading
import queue
import asyncio
import json
from starlette.responses import StreamingResponse

from app.db.session import get_db, get_traffic_db
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
from db_manager import ConnectionManager

router = APIRouter()


@router.get("/{file_id}/process-etl/stream")
async def stream_process_etl(
    request: Request,
    file_id: int,
    db: Session = Depends(get_db),
    traffic_db: Session = Depends(get_traffic_db),
    current_user: User = Depends(get_current_active_user)
):
    """Run ETL for a file and stream progress updates as server-sent events.

    Uses a background thread to run the synchronous ETLProcessor while
    streaming JSON payloads as SSE to the client.
    """
    if current_user.role not in [UserRole.MANAGER, UserRole.ADMIN]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not enough permissions")

    # Validate file exists and has been loaded to staging
    file_record = db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
    if not file_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"File {file_id} not found")

    if not file_record.is_processed:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File must be loaded to staging before ETL processing")

    # Ensure not already running
    active_log = db.query(ETLProcessingLog).filter(
        ETLProcessingLog.source_file_id == file_id,
        ETLProcessingLog.status == 'running'
    ).first()
    if active_log:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="ETL processing is already running for this file")

    # Use startup cache
    #org_code_cache = etl_cache.get_org_code_cache()
    #location_cache = etl_cache.get_location_from_charge_code()

    q = queue.Queue()

    def progress_cb(payload: dict):
        try:
            q.put_nowait(payload)
        except Exception:
            pass

    def run_etl():
        try:
            processor = ETLProcessor(db, traffic_db=traffic_db, progress_callback=progress_cb)
            if file_record.data_source_type == DataSourceType.COIN_COLLECTION:
                result = processor.process_coin_collector(file_id)
                q.put({"event": "done", "result": result})
            else:
                source_key, staging_table = processor._get_source_key_and_staging_table(file_record.data_source_type)
                result = processor.process_file(file_id, source_key, staging_table)
                q.put({"event": "done", "result": result})

        except Exception as e:
            try:
                q.put({"event": "error", "message": str(e)})
            except Exception:
                pass

    thread = threading.Thread(target=run_etl, daemon=True)
    thread.start()

    async def event_generator():
        # Stream queue items as SSE
        while thread.is_alive() or not q.empty():
            try:
                item = q.get(timeout=0.25)
            except queue.Empty:
                # If client disconnects, stop
                if await request.is_disconnected():
                    break
                await asyncio.sleep(0.1)
                continue

            # Format as SSE data event (JSON payload)
            try:
                data = json.dumps(item)
            except Exception:
                data = json.dumps({"event": "error", "message": "Failed to serialize progress payload"})

            yield f"data: {data}\n\n"

        # Ensure final event
        yield f"data: {json.dumps({"event": "complete"})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.get("/status", response_model=FileStatusListResponse)
async def get_files_status(
    skip: int = 0,
    limit: int = 100,
    # Accept string here so UI can send either enum value (e.g. 'pi_sales') or enum NAME (e.g. 'PAYMENTS_INSIDER_SALES')
    data_source_type: Optional[str] = None,
    status_filter: Optional[str] = None,  # 'complete', 'incomplete', 'failed', 'not_started'
    sort_by: str = "id",  # Column to sort by
    sort_order: str = "desc",  # 'asc' or 'desc'
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
        src = data_source_type.strip()

        # Validate and map to known enum member if possible
        matched = None
        # First, check if matches enum NAME
        if src in DataSourceType.__members__:
            matched = DataSourceType[src]
        else:
            # Then check if matches enum value
            for member in DataSourceType:
                if member.value == src:
                    matched = member
                    break

        if not matched:
            # Reject unknown data source to avoid malformed SQL
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unknown data_source_type: {data_source_type}")

        # Build SQL that matches either stored enum NAME or VALUE in DB to handle both storage formats
        data_source_filter = f"AND (uf.data_source_type = '{matched.value}' OR uf.data_source_type = '{matched.name}')"
    
    # Separate WHERE (non-aggregate) and HAVING (aggregate) clauses
    where_clauses = []
    having_clauses = []
    if status_filter:
        if status_filter == 'complete':
            # aggregate check belongs in HAVING
            having_clauses.append("MAX(CASE WHEN etl.status = 'completed' THEN 1 ELSE 0 END) = 1")
        elif status_filter == 'not_complete':
            # Needs both non-aggregate (records_processed not null) and aggregate (not 100% complete)
            where_clauses.append("uf.records_processed IS NOT NULL")
            # A file is 'not_complete' if records_created < records_processed (or no etl records yet)
            having_clauses.append("COALESCE(SUM(etl.records_created), 0) < MAX(uf.records_processed)")
        elif status_filter == 'failed':
            having_clauses.append("MAX(CASE WHEN etl.status = 'failed' THEN 1 ELSE 0 END) = 1")
        elif status_filter == 'not_started':
            where_clauses.append("uf.records_processed IS NULL")
    
    # Build the complex query with formatted filter clauses
    # build WHERE and HAVING clauses into query string
    where_fragment = "\n        " + "\n        ".join([data_source_filter] + [f"AND {c}" for c in where_clauses]) if (data_source_filter or where_clauses) else ""
    having_fragment = "\n    HAVING " + (" AND ".join(having_clauses)) if having_clauses else ""

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
            WHEN MAX(CAST(uf.is_processed As INT)) = 1 AND max(uf.records_processed) = 0 THEN 'complete'
            WHEN uf.records_processed IS NULL THEN 'not_started'
            WHEN MAX(CASE WHEN etl.status IN ('complete', 'completed') THEN 1 ELSE 0 END) = 1 THEN 'complete'
            WHEN MAX(CASE WHEN etl.status = 'running' THEN 1 ELSE 0 END) = 1 THEN 'in_progress'
            WHEN MAX(CASE WHEN etl.status = 'failed' THEN 1 ELSE 0 END) = 1 THEN 'failed'
            ELSE 'not_complete'
        END AS status,
        SUM(etl.records_created) AS records_created,
        SUM(etl.records_failed) AS records_failed,
        CASE
            WHEN SUM(etl.records_created) = sum(etl.records_processed) THEN ''
            ELSE MAX(etl.error_message) 
        END As error_message,
        CASE 
            WHEN uf.records_processed > 0 AND SUM(etl.records_created) > 0 
            THEN (CAST(SUM(etl.records_created) AS FLOAT) / CAST(uf.records_processed AS FLOAT)) * 100.0
            ELSE NULL
        END AS percent_complete,
        -- Computed flags
        CASE 
            WHEN uf.records_processed > 0 AND COALESCE(SUM(etl.records_created), 0) < uf.records_processed 
            THEN 1 ELSE 0 
        END AS needs_etl,
        CASE
            WHEN uf.records_processed > 0 AND MAX(CASE WHEN etl.status = 'running' THEN 1 ELSE 0 END) = 0
            THEN 1 ELSE 0
        END AS can_process
    FROM app.uploaded_files uf
    LEFT JOIN app.etl_processing_log etl ON (uf.id = etl.source_file_id)
    WHERE 1=1
        {where_fragment}
    GROUP BY 
        uf.id, uf.original_filename, uf.file_size, uf.data_source_type,
        uf.upload_date, uf.processed_at, uf.records_processed, uf.description, uf.uploaded_by
    {having_fragment}
    ORDER BY uf.{sort_col} {sort_dir}
    OFFSET :skip ROWS
    FETCH NEXT :limit ROWS ONLY
    """.format(
        where_fragment=where_fragment,
        having_fragment=having_fragment,
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
    if having_clauses:
        # Need to count rows after grouping+having; use a subquery
        having_expr = ' AND '.join(having_clauses)
        count_query_string = """
        SELECT COUNT(*) FROM (
            SELECT uf.id
            FROM app.uploaded_files uf
            LEFT JOIN app.etl_processing_log etl ON (uf.id = etl.source_file_id)
            WHERE 1=1
                {where_fragment}
            GROUP BY uf.id
            HAVING {having}
        ) AS t
        """.format(where_fragment=where_fragment, having=having_expr)
    else:
        count_query_string = """
        SELECT 
            COUNT(DISTINCT uf.id)
        FROM app.uploaded_files uf
        LEFT JOIN app.etl_processing_log etl ON (uf.id = etl.source_file_id)
        WHERE 1=1
            {where_fragment}
        """.format(where_fragment=where_fragment)

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
    traffic_db: Session = Depends(get_traffic_db),
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
    traffic_db: Session = Depends(get_traffic_db),
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
        processor = ETLProcessor(
            db,
            traffic_db=traffic_db,
            #org_code_cache=org_code_cache,
            #location_from_charge_code=location_cache
        )
        
        if file_record.data_source_type == DataSourceType.COIN_COLLECTION:
            result = processor.process_coin_collector(file_id)
        else:
            source_key, staging_table = processor._get_source_key_and_staging_table(file_record.data_source_type)

            if not staging_table:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"No ETL processor available for: {file_record.data_source_type}"
                )
            
            result = processor.process_file(file_id, source_key, staging_table)
        
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
            WHEN MAX(CAST(uf.is_processed As INT)) = 1 AND max(uf.records_processed) = 0 THEN 'complete'
            WHEN uf.records_processed IS NULL THEN 'not_started'
            WHEN MAX(CASE WHEN etl.status IN ('complete', 'completed') THEN 1 ELSE 0 END) = 1 THEN 'complete'
            WHEN MAX(CASE WHEN etl.status = 'running' THEN 1 ELSE 0 END) = 1 THEN 'in_progress'
            WHEN MAX(CASE WHEN etl.status = 'failed' THEN 1 ELSE 0 END) = 1 THEN 'failed'
            ELSE 'not_complete'
        END AS status,
        SUM(etl.records_created) AS records_created,
        SUM(etl.records_failed) AS records_failed,
        CASE
            WHEN SUM(etl.records_created) = sum(etl.records_processed) THEN ''
            ELSE MAX(etl.error_message) 
        END As error_message,
        CASE 
            WHEN uf.records_processed > 0 AND SUM(etl.records_created) > 0 
            THEN (CAST(SUM(etl.records_created) AS FLOAT) / CAST(uf.records_processed AS FLOAT)) * 100.0
            ELSE NULL
        END AS percent_complete,
        CASE 
            WHEN uf.records_processed > 0 AND COALESCE(SUM(etl.records_created), 0) < uf.records_processed 
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


