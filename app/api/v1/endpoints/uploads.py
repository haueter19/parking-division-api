from fastapi import APIRouter, Depends, File, UploadFile, HTTPException, status, Form
from sqlalchemy.orm import Session
from typing import Optional
import os
import shutil
from datetime import datetime
from app.db.session import get_db
from app.models.database import User, UploadedFile, UserRole, DataSourceType
from app.models.schemas import UploadedFileCreate, UploadedFileResponse, UploadedFileWithUser
from app.api.dependencies import get_current_active_user, require_role
from app.config import settings
from app.utils.file_inference import infer_data_source_type

router = APIRouter()


@router.post("/upload", response_model=UploadedFileResponse, status_code=status.HTTP_201_CREATED)
async def upload_file(
    file: UploadFile = File(...),
    description: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Upload a file with metadata tracking.
    
    The data source type is automatically inferred from the filename using
    pattern matching. No manual selection is required.
    
    Requires: UPLOADER, MANAGER, or ADMIN role
    """
    # Check permissions
    if current_user.role not in [UserRole.UPLOADER, UserRole.MANAGER, UserRole.ADMIN]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions to upload files"
        )
    
    # Check file size
    file_size = 0
    temp_file = None
    
    try:
        # Save to temporary location to check size
        temp_file = f"assets/temp/{file.filename}"
        with open(temp_file, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        file_size = os.path.getsize(temp_file)
        
        # Check size limit
        max_size = settings.max_upload_size_mb * 1024 * 1024  # Convert MB to bytes
        if file_size > max_size:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File size exceeds maximum allowed size of {settings.max_upload_size_mb}MB"
            )
        
        # Generate unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_extension = os.path.splitext(file.filename)[1]
        unique_filename = f"{timestamp}_{current_user.username}_{file.filename}"
        
        # Infer data source type from filename using pattern matching
        data_source_type = infer_data_source_type(file.filename)
        
        # Create subdirectory for data source type
        source_dir = os.path.join(settings.upload_dir, data_source_type.value)
        os.makedirs(source_dir, exist_ok=True)
        
        # Final file path
        file_path = os.path.join(source_dir, unique_filename)
        
        # Move file to final location
        shutil.move(temp_file, file_path)
        temp_file = None  # Mark as moved
        
        # Create database record
        uploaded_file_record = UploadedFile(
            filename=unique_filename,
            original_filename=file.filename,
            file_path=file_path,
            file_size=file_size,
            data_source_type=data_source_type,
            uploaded_by=current_user.id,
            description=description
        )
        
        db.add(uploaded_file_record)
        db.commit()
        db.refresh(uploaded_file_record)
        
        return uploaded_file_record
        
    except HTTPException:
        # Re-raise HTTP exceptions
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)
        raise
    except Exception as e:
        # Clean up temporary file on error
        print(str(e))
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error uploading file: {str(e)}"
        )


@router.get("/uploads", response_model=list[UploadedFileWithUser])
async def list_uploaded_files(
    skip: int = 0,
    limit: int = 100,
    data_source_type: Optional[DataSourceType] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    List uploaded files with optional filtering by data source type
    """
    query = db.query(UploadedFile)
    
    # Filter by data source type if provided
    if data_source_type:
        query = query.filter(UploadedFile.data_source_type == data_source_type)
    
    # Order by most recent first
    query = query.order_by(UploadedFile.upload_date.desc())
    
    files = query.offset(skip).limit(limit).all()
    return files


@router.get("/uploads/{file_id}", response_model=UploadedFileWithUser)
async def get_uploaded_file(
    file_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Get details of a specific uploaded file
    """
    file_record = db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
    
    if not file_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )
    
    return file_record


@router.delete("/uploads/{file_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_uploaded_file(
    file_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN, UserRole.MANAGER]))
):
    """
    Delete an uploaded file (Admin/Manager only)
    """
    file_record = db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
    
    if not file_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )
    
    # Delete physical file
    try:
        if os.path.exists(file_record.file_path):
            os.remove(file_record.file_path)
    except Exception as e:
        # Log error but continue with database deletion
        print(f"Error deleting physical file: {e}")
    
    # Delete database record
    db.delete(file_record)
    db.commit()
    
    return None
