# Parking Division Operations & Revenue Tracking System

A FastAPI-based web application for tracking parking division operations and revenue data from multiple sources including credit card settlements, permits, garage transactions, and parking tickets.

## Features

- ✅ **File Upload System** - Track uploaded files with metadata (uploader, date, source type)
- ✅ **User Authentication** - Role-based access control (Admin, Manager, Uploader, Viewer)
- ✅ **Multiple Data Sources** - Support for 10+ data source types
- ✅ **SQL Server Database** - SQLAlchemy ORM with comprehensive data models
- ✅ **Modern Web Interface** - Clean, responsive UI with drag-and-drop file upload
- ✅ **RESTful API** - Well-documented FastAPI endpoints
- ✅ **Revenue Tracking** - Fine-grained revenue tracking with org codes and locations

## Data Sources Supported

1. **Credit Card Settlements** - Windcave, Payments Insider, IPS
2. **Cash Collections** - Finance department PDFs
3. **RP3 Permits** - Residential Parking Permit Program
4. **Monthly Permits** - Garage/lot monthly parking permits
5. **Garage Transactions** - Visit and occupancy data
6. **Parking Tickets** - AIMS vendor data
7. **Towed Vehicles** - Towed/abandoned vehicle records
8. **OOPH Charges** - Out of allowed parking hours
9. **Meter Usage** - IPS meter usage and revenue
10. **PCI Inspections** - PCI compliance inspection records

## Tech Stack

- **Backend**: FastAPI, SQLAlchemy, Pydantic
- **Database**: Microsoft SQL Server
- **Authentication**: JWT tokens with bcrypt password hashing
- **Data Processing**: Pandas for analytics
- **Frontend**: Modern HTML/CSS/JavaScript (no framework dependencies)

## Project Structure

```
parking-division-api/
├── app/
│   ├── main.py                    # FastAPI application entry point
│   ├── config.py                  # Configuration settings
│   ├── api/
│   │   ├── dependencies.py        # Auth dependencies
│   │   └── v1/
│   │       ├── api.py            # API router aggregation
│   │       └── endpoints/
│   │           ├── auth.py       # Authentication endpoints
│   │           ├── uploads.py    # File upload endpoints
│   │           └── health.py     # Health check endpoints
│   ├── models/
│   │   ├── database.py           # SQLAlchemy models
│   │   └── schemas.py            # Pydantic schemas
│   ├── db/
│   │   └── session.py            # Database session management
│   ├── utils/
│   │   └── auth.py               # Authentication utilities
│   └── static/
│       ├── index.html            # Login page
│       └── upload.html           # File upload interface
├── scripts/
│   └── seed_data.py              # Database seeding script
├── requirements.txt
├── .env.example
└── README.md
```

## Database Models

### Core Models

- **User** - User accounts with role-based access
- **UploadedFile** - Track all uploaded files with metadata
- **Transaction** - Individual revenue transactions
- **Location** - Physical locations (meters, garages, lots)
- **OrgCode** - Organization codes for revenue tracking

### Enumerations

- **UserRole**: ADMIN, MANAGER, UPLOADER, VIEWER
- **DataSourceType**: 13 different data source categories

## Installation & Setup

### Prerequisites

- Python 3.9+
- Microsoft SQL Server (or SQL Server Express)
- ODBC Driver 17 for SQL Server

### 1. Clone and Setup

```bash
# Navigate to project directory
cd parking-division-api

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Database Configuration

Create a `.env` file based on `.env.example`:

```bash
cp .env.example .env
```

Edit `.env` with your SQL Server credentials:

```env
DB_SERVER=localhost
DB_NAME=parking_division
DB_USER=your_username
DB_PASSWORD=your_password
DB_DRIVER=ODBC Driver 17 for SQL Server

SECRET_KEY=your-secret-key-here-change-in-production
UPLOAD_DIR=./uploads
```

### 3. Initialize Database

```bash
# Create database tables and seed initial data
python scripts/seed_data.py
```

This creates:
- Database tables
- Admin user (username: `admin`, password: `admin123`)
- Sample users for each role
- Sample organization codes

### 4. Run the Application

```bash
# Development mode with auto-reload
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The application will be available at:
- **Web Interface**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs
- **API Alternative Docs**: http://localhost:8000/redoc

## Usage

### Web Interface

1. **Login** - Navigate to http://localhost:8000 and login with credentials
2. **Upload Files** - Use the drag-and-drop interface to upload files
3. **Select Data Source** - Choose the appropriate data source type
4. **Add Description** - Optional description for context
5. **View Recent Uploads** - See recently uploaded files

### API Endpoints

#### Authentication
- `POST /api/v1/auth/login` - Login and get access token
- `GET /api/v1/auth/me` - Get current user info
- `POST /api/v1/auth/register` - Register new user (Admin only)
- `GET /api/v1/auth/users` - List all users (Admin/Manager)

#### File Uploads
- `POST /api/v1/files/upload` - Upload a file
- `GET /api/v1/files/uploads` - List uploaded files
- `GET /api/v1/files/uploads/{id}` - Get specific file details
- `DELETE /api/v1/files/uploads/{id}` - Delete file (Admin/Manager)

#### Health Checks
- `GET /api/v1/health` - Basic health check
- `GET /api/v1/health/db` - Database connectivity check

### Example API Usage

```python
import requests

# Login
response = requests.post(
    "http://localhost:8000/api/v1/auth/login",
    data={"username": "admin", "password": "admin123"}
)
token = response.json()["access_token"]

# Upload file
headers = {"Authorization": f"Bearer {token}"}
files = {"file": open("data.csv", "rb")}
data = {
    "data_source_type": "windcave_cc",
    "description": "March 2024 credit card settlements"
}
response = requests.post(
    "http://localhost:8000/api/v1/files/upload",
    headers=headers,
    files=files,
    data=data
)
```

## User Roles & Permissions

| Role | Upload Files | View Files | Manage Users | Delete Files |
|------|--------------|------------|--------------|--------------|
| **Admin** | ✅ | ✅ | ✅ | ✅ |
| **Manager** | ✅ | ✅ | View Only | ✅ |
| **Uploader** | ✅ | ✅ | ❌ | ❌ |
| **Viewer** | ❌ | ✅ | ❌ | ❌ |

## Default Credentials

**⚠️ IMPORTANT: Change these passwords immediately after setup!**

- **Admin**: `admin` / `admin123`
- **Manager**: `manager1` / `manager123`
- **Uploader**: `uploader1` / `upload123`
- **Viewer**: `viewer1` / `viewer123`

## Next Steps / TODO

1. **Data Processing Pipeline** - Implement parsers for each data source type
2. **Transaction Import** - Parse uploaded files and create transaction records
3. **Analytics Dashboard** - Build daily/monthly summary views
4. **Revenue Reports** - Generate reports by org code, location, date range
5. **Location Management** - UI for managing locations and terminal IDs
6. **Org Code Management** - UI for managing organization codes
7. **Data Validation** - Add validation rules for different file types
8. **Export Functionality** - Export processed data to Excel/CSV
9. **Audit Logging** - Track all data modifications
10. **Advanced Filtering** - Filter transactions by multiple criteria

## Development

### Adding New Endpoints

1. Create endpoint file in `app/api/v1/endpoints/`
2. Add router to `app/api/v1/api.py`
3. Use dependencies from `app/api/dependencies.py` for auth

### Adding New Models

1. Add SQLAlchemy model to `app/models/database.py`
2. Add Pydantic schemas to `app/models/schemas.py`
3. Run migrations or recreate database

### Security Considerations

- Always use HTTPS in production
- Change default SECRET_KEY
- Change all default passwords
- Configure CORS appropriately
- Implement rate limiting
- Regular security audits
- Keep dependencies updated

## Troubleshooting

### Database Connection Issues

```bash
# Test SQL Server connectivity
sqlcmd -S localhost -U sa -P your_password -Q "SELECT @@VERSION"
```

### ODBC Driver Issues

```bash
# List available ODBC drivers
# Windows:
odbcad32.exe

# Linux:
odbcinst -q -d
```

Download ODBC Driver 17: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

## License

Internal use only - Parking Division

## Support

For questions or issues, contact the development team.
