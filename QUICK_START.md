# 🚀 QUICK START GUIDE

## Parking Division API - Getting Started in 5 Minutes

### Step 1: Prerequisites
- Python 3.9 or higher installed
- Microsoft SQL Server installed and running
- ODBC Driver 17 for SQL Server installed

### Step 2: Database Setup

1. Create a database named `parking_division` in SQL Server:
```sql
CREATE DATABASE parking_division;
```

### Step 3: Configure Environment

1. Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

2. Edit `.env` with your SQL Server credentials:
```env
DB_SERVER=localhost
DB_NAME=parking_division
DB_USER=your_username
DB_PASSWORD=your_password
DB_DRIVER=ODBC Driver 17 for SQL Server
SECRET_KEY=change-this-to-a-random-secret-key
```

### Step 4: Install Dependencies

```bash
# Create virtual environment
python -m venv venv

# Activate it
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install packages
pip install -r requirements.txt
```

### Step 5: Initialize Database

```bash
python scripts/seed_data.py
```

This creates:
- All database tables
- Default admin user (username: `admin`, password: `admin123`)
- Sample users and org codes

### Step 6: Start the Server

```bash
uvicorn app.main:app --reload
```

Or use the startup script:
```bash
./start.sh
```

### Step 7: Access the Application

Open your browser and go to:
- **Login Page**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs

### Step 8: Login

Use these default credentials:
- **Username**: `admin`
- **Password**: `admin123`

⚠️ **IMPORTANT**: Change the admin password immediately after first login!

---

## What You Can Do Now

### Upload Files
1. Login to the web interface
2. Navigate to the upload page (automatic after login)
3. Drag and drop a file or click to browse
4. Select the data source type (Windcave CC, IPS, etc.)
5. Add an optional description
6. Click "Upload File"

### Use the API
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
files = {"file": open("your-file.csv", "rb")}
data = {
    "data_source_type": "windcave_cc",
    "description": "March settlements"
}
response = requests.post(
    "http://localhost:8000/api/v1/files/upload",
    headers=headers,
    files=files,
    data=data
)
print(response.json())
```

---

## Troubleshooting

### Database Connection Error
- Verify SQL Server is running
- Check credentials in `.env`
- Test connection: `sqlcmd -S localhost -U sa -P your_password`

### Import Errors
- Make sure virtual environment is activated
- Run `pip install -r requirements.txt` again

### Port Already in Use
- Change port: `uvicorn app.main:app --reload --port 8001`

---

## Next Steps

1. **Change Default Passwords** - Very important for security!
2. **Add Your Users** - Use the admin account to create real users
3. **Configure Org Codes** - Add your organization's specific codes
4. **Upload Test Files** - Try uploading some sample data
5. **Build Data Processors** - Implement parsers for your specific file formats

For detailed documentation, see README.md

---

## Project Structure at a Glance

```
parking-division-api/
├── app/
│   ├── main.py              # Application entry point
│   ├── config.py            # Settings
│   ├── api/                 # API routes
│   │   └── v1/endpoints/    # Version 1 endpoints
│   ├── models/              # Database & Pydantic models
│   ├── db/                  # Database setup
│   ├── utils/               # Helper functions
│   └── static/              # Web interface (HTML)
├── scripts/
│   └── seed_data.py         # Database initialization
├── requirements.txt         # Python dependencies
└── .env                     # Your configuration (create this!)
```

---

## Support

Read the full README.md for comprehensive documentation!
