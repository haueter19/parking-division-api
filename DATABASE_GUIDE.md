# Database Models & Schemas Guide

## Understanding the Data Model

This guide explains the database structure and how to work with SQLAlchemy models and Pydantic schemas.

## Core Database Tables

### 1. Users Table
Stores user accounts with role-based access control.

**Fields:**
- `id` - Primary key
- `username` - Unique username
- `email` - Unique email address
- `hashed_password` - Bcrypt hashed password
- `full_name` - User's full name
- `role` - UserRole enum (ADMIN, MANAGER, UPLOADER, VIEWER)
- `is_active` - Boolean flag for account status
- `created_at` - Timestamp when user was created
- `updated_at` - Timestamp of last update

**Relationships:**
- `uploaded_files` - One-to-many with UploadedFile

### 2. UploadedFile Table
Tracks all uploaded files with metadata.

**Fields:**
- `id` - Primary key
- `filename` - Unique generated filename
- `original_filename` - Original filename from upload
- `file_path` - Full path to stored file
- `file_size` - Size in bytes
- `data_source_type` - DataSourceType enum (see below)
- `uploaded_by` - Foreign key to Users
- `upload_date` - Timestamp of upload
- `description` - Optional description
- `is_processed` - Boolean flag if file has been processed
- `processed_at` - Timestamp when processing completed

**Relationships:**
- `uploaded_by_user` - Many-to-one with User
- `transactions` - One-to-many with Transaction

### 3. Transaction Table
Individual revenue transactions extracted from files.

**Fields:**
- `id` - Primary key
- `transaction_date` - Date/time of transaction
- `settle_date` - Settlement date (for credit cards)
- `amount` - Transaction amount (Numeric 10,2)
- `payment_method` - Payment type (credit_card, cash, etc.)
- `data_source_type` - Source of this transaction
- `terminal_id` - Terminal/location identifier
- `transaction_reference` - External reference number
- `source_file_id` - Foreign key to UploadedFile
- `org_code_id` - Foreign key to OrgCode (optional)
- `location_id` - Foreign key to Location (optional)
- `notes` - Additional notes
- `created_at` - When record was created

**Relationships:**
- `source_file` - Many-to-one with UploadedFile
- `org_code` - Many-to-one with OrgCode
- `location` - Many-to-one with Location

### 4. Location Table
Physical locations (meters, garages, parking lots).

**Fields:**
- `id` - Primary key
- `location_code` - Unique location code
- `location_name` - Descriptive name
- `location_type` - Type (meter, garage, lot)
- `terminal_id` - Credit card terminal ID
- `address` - Physical address
- `is_active` - Active status
- `created_at` - Creation timestamp

**Relationships:**
- `transactions` - One-to-many with Transaction

### 5. OrgCode Table
Organization codes for revenue categorization.

**Fields:**
- `id` - Primary key
- `code` - Unique org code
- `description` - Description of org code
- `is_active` - Active status
- `created_at` - Creation timestamp

**Relationships:**
- `transactions` - One-to-many with Transaction

## Enumerations

### UserRole
```python
class UserRole(str, enum.Enum):
    ADMIN = "admin"          # Full access
    MANAGER = "manager"      # Manage operations
    VIEWER = "viewer"        # Read-only access
    UPLOADER = "uploader"    # Upload files
```

### DataSourceType
```python
class DataSourceType(str, enum.Enum):
    WINDCAVE_CC = "windcave_cc"                    # Windcave credit card
    PAYMENTS_INSIDER_CC = "payments_insider_cc"    # Payments Insider
    IPS_CC = "ips_cc"                              # IPS credit card
    CASH_COLLECTION = "cash_collection"            # Cash PDFs
    RP3_PERMITS = "rp3_permits"                    # Residential permits
    MONTHLY_PERMITS = "monthly_permits"            # Monthly permits
    GARAGE_TRANSACTIONS = "garage_transactions"    # Garage visits
    PARKING_TICKETS = "parking_tickets"            # AIMS tickets
    TOWED_VEHICLES = "towed_vehicles"              # Towed vehicles
    OOPH_CHARGES = "ooph_charges"                  # Out of hours
    METER_USAGE = "meter_usage"                    # IPS meters
    PCI_INSPECTIONS = "pci_inspections"            # PCI compliance
    OTHER = "other"                                # Other sources
```

## Pydantic Schemas

Schemas validate API requests and responses. They're separate from database models.

### Pattern: Base, Create, Update, Response

Each model typically has:
1. **Base** - Common fields
2. **Create** - Fields needed for creation
3. **Update** - Optional fields for updates
4. **Response** - Fields returned to client

Example for User:
```python
class UserBase(BaseModel):
    username: str
    email: EmailStr
    full_name: Optional[str] = None

class UserCreate(UserBase):
    password: str  # Not in base
    role: UserRole = UserRole.VIEWER

class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None

class UserResponse(UserBase):
    id: int
    role: UserRole
    is_active: bool
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)
```

## Adding New Fields

### Example: Add "phone_number" to User

**Step 1: Update Database Model**
```python
# In app/models/database.py
class User(Base):
    # ... existing fields ...
    phone_number = Column(String(20))
```

**Step 2: Update Pydantic Schemas**
```python
# In app/models/schemas.py
class UserBase(BaseModel):
    username: str
    email: EmailStr
    full_name: Optional[str] = None
    phone_number: Optional[str] = None  # Add this

class UserCreate(UserBase):
    password: str
    role: UserRole = UserRole.VIEWER
    # phone_number inherited from UserBase

class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    phone_number: Optional[str] = None  # Add this
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None

class UserResponse(UserBase):
    id: int
    role: UserRole
    is_active: bool
    created_at: datetime
    # phone_number inherited from UserBase
```

**Step 3: Update Database**
```python
# Option 1: Recreate database (development only)
from app.db.session import init_db
init_db()

# Option 2: Use Alembic for migrations (production)
alembic revision --autogenerate -m "Add phone_number to users"
alembic upgrade head
```

## Adding New Tables

### Example: Add "ParkingZone" table

**Step 1: Create Database Model**
```python
# In app/models/database.py
class ParkingZone(Base):
    __tablename__ = "parking_zones"
    
    id = Column(Integer, primary_key=True, index=True)
    zone_code = Column(String(50), unique=True, nullable=False, index=True)
    zone_name = Column(String(255), nullable=False)
    hourly_rate = Column(Numeric(10, 2))
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    locations = relationship("Location", back_populates="zone")
```

**Step 2: Update Related Models**
```python
# Add to Location model
class Location(Base):
    # ... existing fields ...
    zone_id = Column(Integer, ForeignKey("parking_zones.id"))
    
    # Add relationship
    zone = relationship("ParkingZone", back_populates="locations")
```

**Step 3: Create Pydantic Schemas**
```python
# In app/models/schemas.py
class ParkingZoneBase(BaseModel):
    zone_code: str
    zone_name: str
    hourly_rate: Optional[float] = None

class ParkingZoneCreate(ParkingZoneBase):
    pass

class ParkingZoneResponse(ParkingZoneBase):
    id: int
    is_active: bool
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)
```

**Step 4: Create API Endpoints**
```python
# In app/api/v1/endpoints/zones.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.models.database import ParkingZone
from app.models.schemas import ParkingZoneCreate, ParkingZoneResponse

router = APIRouter()

@router.post("/zones", response_model=ParkingZoneResponse)
async def create_zone(
    zone_data: ParkingZoneCreate,
    db: Session = Depends(get_db)
):
    zone = ParkingZone(**zone_data.model_dump())
    db.add(zone)
    db.commit()
    db.refresh(zone)
    return zone

@router.get("/zones", response_model=list[ParkingZoneResponse])
async def list_zones(db: Session = Depends(get_db)):
    return db.query(ParkingZone).all()
```

**Step 5: Register Router**
```python
# In app/api/v1/api.py
from app.api.v1.endpoints import zones

api_router.include_router(zones.router, prefix="/zones", tags=["zones"])
```

## Querying Data

### Basic Queries
```python
from app.models.database import User, UploadedFile, Transaction

# Get all users
users = db.query(User).all()

# Filter users
active_users = db.query(User).filter(User.is_active == True).all()

# Get by ID
user = db.query(User).filter(User.id == 1).first()

# Get with relationships
uploads = db.query(UploadedFile).filter(
    UploadedFile.uploaded_by == user_id
).all()
```

### Joining Tables
```python
# Get transactions with location info
results = db.query(Transaction, Location).join(
    Location, Transaction.location_id == Location.id
).filter(
    Transaction.transaction_date >= start_date
).all()

# Get uploads with user info (using relationship)
uploads = db.query(UploadedFile).join(
    User, UploadedFile.uploaded_by == User.id
).filter(
    User.role == UserRole.UPLOADER
).all()
```

### Aggregations
```python
from sqlalchemy import func

# Count transactions by source
counts = db.query(
    Transaction.data_source_type,
    func.count(Transaction.id)
).group_by(
    Transaction.data_source_type
).all()

# Sum revenue by org code
revenue = db.query(
    OrgCode.code,
    func.sum(Transaction.amount)
).join(
    Transaction, OrgCode.id == Transaction.org_code_id
).group_by(
    OrgCode.code
).all()
```

## Best Practices

1. **Always use Pydantic schemas** for API input/output
2. **Never expose passwords** in response schemas
3. **Use relationships** instead of manual joins when possible
4. **Index frequently queried fields**
5. **Use transactions** for multiple related operations
6. **Validate at schema level** before database operations
7. **Use enums** for fixed value sets
8. **Add created_at/updated_at** to all tables
9. **Soft delete** with is_active instead of hard deletes
10. **Use foreign keys** to maintain referential integrity

## Common Patterns

### Creating Related Records
```python
# Create user and initial upload in one transaction
user = User(username="newuser", ...)
db.add(user)
db.flush()  # Get user.id without committing

upload = UploadedFile(uploaded_by=user.id, ...)
db.add(upload)
db.commit()
```

### Eager Loading Relationships
```python
from sqlalchemy.orm import joinedload

# Load uploads with user info in one query
uploads = db.query(UploadedFile).options(
    joinedload(UploadedFile.uploaded_by_user)
).all()
```

### Pagination
```python
page = 1
page_size = 20
skip = (page - 1) * page_size

results = db.query(Transaction).offset(skip).limit(page_size).all()
```

This should give you a solid foundation for working with the database models!
