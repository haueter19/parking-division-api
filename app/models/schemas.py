from pydantic import BaseModel, EmailStr, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
from app.models.database import UserRole, DataSourceType, LocationType, PaymentType


# ============= User Schemas =============

class UserBase(BaseModel):
    """Base user schema"""
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    full_name: Optional[str] = None


class UserCreate(UserBase):
    """Schema for creating a new user"""
    password: str = Field(..., min_length=8)
    role: UserRole = UserRole.VIEWER


class UserUpdate(BaseModel):
    """Schema for updating user information"""
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None


class UserResponse(UserBase):
    """Schema for user response"""
    id: int
    role: UserRole
    is_active: bool
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)


class UserLogin(BaseModel):
    """Schema for user login"""
    username: str
    password: str


class Token(BaseModel):
    """Schema for authentication token"""
    access_token: str
    token_type: str


class TokenData(BaseModel):
    """Schema for token data"""
    username: Optional[str] = None

# ============= Enums (matching database enums) =============



class UploadedFileBase(BaseModel):
    """Base uploaded file schema"""
    data_source_type: DataSourceType
    description: Optional[str] = None


class UploadedFileCreate(UploadedFileBase):
    """Schema for creating an uploaded file record"""
    filename: str
    original_filename: str
    file_path: str
    file_size: int
    uploaded_by: int


class UploadedFileResponse(UploadedFileBase):
    """Schema for uploaded file response"""
    id: int
    filename: str
    original_filename: str
    file_size: int
    uploaded_by: int
    upload_date: datetime
    is_processed: bool
    processed_at: Optional[datetime] = None
    records_processed: Optional[int] = None
    
    model_config = ConfigDict(from_attributes=True)


class UploadedFileWithUser(UploadedFileResponse):
    """Schema for uploaded file with user information"""
    uploaded_by_user: UserResponse
    
    model_config = ConfigDict(from_attributes=True)


class ProcessTransactionRequest(BaseModel):
    """Request model for processing transaction files"""
    sheet_name: Optional[str] = None
    if_exists: str = "append"
    process_async: bool = False


class ProcessTransactionResponse(BaseModel):
    """Response model for transaction processing"""
    success: bool
    file_id: int
    filename: str
    status: str  # "processing", "completed", "failed"
    records_inserted: Optional[int] = None
    message: str
    errors: Optional[List[str]] = None
    
    model_config = ConfigDict(from_attributes=True)

# ============= File Upload Processing =============
class FileProcessRequest(BaseModel):
    """Request to process an uploaded file to staging"""
    file_id: int
    source_type: DataSourceType
    report_type: Optional[str] = None  # For PI: 'sales' or 'payments'
    sheet_name: Optional[str] = None  # For Excel files
    skip_rows: Optional[int] = 0
    
    
class FileProcessResponse(BaseModel):
    """Response from file processing"""
    success: bool
    file_id: int
    records_loaded: int
    message: str
    errors: Optional[List[str]] = None


# ============= Staging Table Schemas =============
class WindcaveStagingBase(BaseModel):
    """Base schema for Windcave staging records"""
    transaction_date: datetime
    card_number_masked: Optional[str]
    amount: float
    settlement_date: Optional[datetime]
    settlement_amount: Optional[float]
    terminal_id: str
    reference: Optional[str]
    card_type: Optional[str]
    merchant_id: Optional[str]


class WindcaveStagingCreate(WindcaveStagingBase):
    """Create schema for Windcave staging"""
    source_file_id: int


class WindcaveStagingResponse(WindcaveStagingBase):
    """Response schema for Windcave staging"""
    id: int
    loaded_at: datetime
    processed_to_final: bool
    transaction_id: Optional[int]
    
    model_config = ConfigDict(from_attributes=True)


class PaymentsInsiderStagingBase(BaseModel):
    """Base schema for Payments Insider staging"""
    report_type: str  # 'sales' or 'payments'
    transaction_date: Optional[datetime]
    payment_date: Optional[datetime]
    amount: float
    card_type: Optional[str]
    terminal_id: str
    location: Optional[str]
    reference_number: str
    batch_number: Optional[str]


class PaymentsInsiderStagingCreate(PaymentsInsiderStagingBase):
    """Create schema for PI staging"""
    source_file_id: int


class PaymentsInsiderStagingResponse(PaymentsInsiderStagingBase):
    """Response schema for PI staging"""
    id: int
    loaded_at: datetime
    processed_to_final: bool
    transaction_id: Optional[int]
    matching_report_id: Optional[int]
    
    model_config = ConfigDict(from_attributes=True)


# ============= Transaction Schemas (Final Table) =============
class TransactionBase(BaseModel):
    """Base schema for normalized transactions"""
    transaction_date: datetime
    transaction_amount: float
    settle_date: Optional[datetime]
    settle_amount: Optional[float]
    source: DataSourceType
    location_type: LocationType
    location_name: Optional[str]
    device_terminal_id: str
    payment_type: PaymentType
    reference_number: Optional[str]
    org_code: Optional[str]


class TransactionCreate(TransactionBase):
    """Schema for creating transactions"""
    staging_table: Optional[str]
    staging_record_id: Optional[int]


class TransactionResponse(TransactionBase):
    """Response schema for transactions"""
    id: int
    created_at: datetime
    updated_at: Optional[datetime]
    
    model_config = ConfigDict(from_attributes=True)


class TransactionFilter(BaseModel):
    """Filter parameters for transaction queries"""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    source: Optional[DataSourceType] = None
    location_type: Optional[LocationType] = None
    payment_type: Optional[PaymentType] = None
    terminal_id: Optional[str] = None
    org_code: Optional[str] = None
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None


class TransactionSummary(BaseModel):
    """Summary statistics for transactions"""
    total_count: int
    total_amount: float
    total_settle_amount: float
    by_payment_type: Dict[str, float]
    by_location_type: Dict[str, float]
    by_source: Dict[str, float]
    date_range: Dict[str, datetime]


# ============= ETL Processing =============
class ETLProcessRequest(BaseModel):
    """Request to process staging data to final transactions"""
    source_table: Optional[str] = None  # Process specific table or all
    file_id: Optional[int] = None  # Process specific file or all
    dry_run: bool = False  # Preview without committing


class ETLProcessResponse(BaseModel):
    """Response from ETL processing"""
    success: bool
    results: Dict[str, Any]  # Results by staging table
    total_created: int
    total_failed: int
    duration_seconds: float
    message: str


class ETLStatusResponse(BaseModel):
    """Status of ETL processing"""
    pending_records: Dict[str, int]  # Count by staging table
    processed_today: int
    last_run: Optional[datetime]
    errors: Optional[List[str]]


# ============= File Status Schemas =============
class FileStatusResponse(BaseModel):
    """Combined response showing uploaded file with ETL processing status"""
    # File information
    id: int
    original_filename: str
    file_size: int
    data_source_type: DataSourceType
    upload_date: datetime
    description: Optional[str]
    
    # Processing status
    processed_at: Optional[datetime]
    records_processed: Optional[int]  # Records loaded to staging
    
    # ETL status
    status: str  # 'not_started', 'in_progress', 'complete', 'failed'
    records_created: Optional[int]  # Records created in final transactions table
    records_failed: Optional[int]
    error_message: Optional[str]
    percent_complete: Optional[float]  # Percentage of staging records processed to final
    
    # Computed fields
    needs_etl: bool  # True if loaded to staging but not completed in ETL
    can_process: bool  # True if file can be processed
    
    model_config = ConfigDict(from_attributes=True)


class FileStatusListResponse(BaseModel):
    """Paginated list of file statuses"""
    total: int
    items: list[FileStatusResponse]
    page: int
    page_size: int
    total_pages: int


class ProcessETLRequest(BaseModel):
    """Request to process file through ETL pipeline"""
    source_table: Optional[str] = None  # Specific staging table or all
    force_reprocess: bool = False  # Reprocess already-processed records
    dry_run: bool = False  # Preview without committing


class ProcessETLResponse(BaseModel):
    """Response from ETL processing request"""
    success: bool
    file_id: int
    message: str
    records_created: Optional[int]
    records_failed: Optional[int]
    errors: Optional[list[str]]


# ============= Bulk Upload =============
class BulkUploadRequest(BaseModel):
    """Request for bulk file upload and processing"""
    files: List[Dict[str, Any]]  # File info
    auto_process: bool = False  # Automatically process to staging
    auto_etl: bool = False  # Automatically run ETL to final


class BulkUploadResponse(BaseModel):
    """Response from bulk upload"""
    success: bool
    files_uploaded: int
    files_processed: int
    total_records: int
    errors: Optional[List[str]]



# ============= Analytics Schemas =============

class DailySummary(BaseModel):
    """Schema for daily revenue summary"""
    date: datetime
    total_revenue: float
    transaction_count: int
    by_source: dict[str, float]
    by_payment_method: dict[str, float]


class MonthlySummary(BaseModel):
    """Schema for monthly revenue summary"""
    year: int
    month: int
    total_revenue: float
    transaction_count: int
    by_source: dict[str, float]
    by_org_code: dict[str, float]