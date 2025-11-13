# 🎯 START HERE - Parking Division API

Welcome! This is your complete FastAPI application for tracking parking division operations and revenue data.

## 📚 Documentation Guide

Start with the document that matches your needs:

### 🚀 For First-Time Setup
**Read:** [QUICK_START.md](QUICK_START.md)
- 5-minute setup guide
- Step-by-step instructions
- Get running immediately

### 📖 For Complete Understanding
**Read:** [README.md](README.md)
- Comprehensive documentation
- All features explained
- API usage examples
- Production deployment

### 🗄️ For Database Work
**Read:** [DATABASE_GUIDE.md](DATABASE_GUIDE.md)
- SQLAlchemy models explained
- Pydantic schemas guide
- How to add fields/tables
- Query examples

### 🎨 For Project Overview
**Read:** [OVERVIEW.md](OVERVIEW.md)
- Visual project structure
- Quick reference
- All features at a glance
- Tech stack details

### 📊 For Project Summary
**Read:** [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md)
- What has been built
- Design decisions
- Next steps
- Key highlights

---

## ⚡ Quick Start (TL;DR)

```bash
# 1. Configure database
cp .env.example .env
# Edit .env with your SQL Server credentials

# 2. Setup
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 3. Initialize
python scripts/seed_data.py

# 4. Run
uvicorn app.main:app --reload

# 5. Open browser
# http://localhost:8000
# Login: admin / admin123
```

---

## 📁 What's Inside

```
parking-division-api/
├── 📘 START_HERE.md          ← You are here
├── 📘 QUICK_START.md         ← Setup in 5 minutes
├── 📘 README.md              ← Full documentation
├── 📘 OVERVIEW.md            ← Visual reference
├── 📘 DATABASE_GUIDE.md      ← Database help
├── 📘 PROJECT_SUMMARY.md     ← What's built
│
├── app/                      ← Application code
│   ├── main.py              ← FastAPI app
│   ├── config.py            ← Settings
│   ├── api/                 ← API endpoints
│   ├── models/              ← Database models
│   ├── db/                  ← Database setup
│   ├── utils/               ← Utilities
│   └── static/              ← Web UI (HTML)
│
├── scripts/                  ← Utility scripts
│   └── seed_data.py         ← Initialize database
│
├── requirements.txt          ← Dependencies
├── .env.example             ← Config template
└── start.sh                 ← Startup script
```

---

## 🎯 What Does This Do?

This application helps you:

✅ **Upload Files** - Track all parking revenue data files
✅ **Manage Users** - Role-based access control
✅ **Track Sources** - 13 different data source types
✅ **Store Metadata** - Who uploaded what, when
✅ **Organize Files** - Automatic organization by type
✅ **Secure Access** - JWT authentication

### 13 Data Source Types
1. Windcave Credit Card
2. Payments Insider CC
3. IPS Credit Card
4. Cash Collections
5. RP3 Permits
6. Monthly Permits
7. Garage Transactions
8. Parking Tickets (AIMS)
9. Towed Vehicles
10. Out of Hours Charges
11. IPS Meter Usage
12. PCI Inspections
13. Other

---

## 🗄️ Database Tables

- **users** - User accounts with roles
- **uploaded_files** - File tracking
- **transactions** - Revenue transactions
- **locations** - Parking locations
- **org_codes** - Organization codes

All tables have relationships and proper foreign keys.

---

## 🔐 Default Users

After running `seed_data.py`, you'll have:

| User | Password | Role |
|------|----------|------|
| admin | admin123 | Admin |
| manager1 | manager123 | Manager |
| uploader1 | upload123 | Uploader |
| viewer1 | viewer123 | Viewer |

**⚠️ Change these passwords immediately!**

---

## 🌐 URLs After Starting

- **Web App:** http://localhost:8000
- **API Docs:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc
- **Health:** http://localhost:8000/api/v1/health

---

## 🛠️ Technology

- **Backend:** FastAPI + SQLAlchemy
- **Database:** SQL Server
- **Auth:** JWT + bcrypt
- **Frontend:** HTML/CSS/JavaScript
- **Data:** Pandas (ready for processing)

---

## 🆘 Need Help?

### Setup Issues?
👉 Read [QUICK_START.md](QUICK_START.md)

### Database Questions?
👉 Read [DATABASE_GUIDE.md](DATABASE_GUIDE.md)

### Want Full Details?
👉 Read [README.md](README.md)

### Just Want Overview?
👉 Read [OVERVIEW.md](OVERVIEW.md)

---

## ✅ Checklist

Before you start:
- [ ] SQL Server installed and running
- [ ] Python 3.9+ installed
- [ ] ODBC Driver 17 for SQL Server installed

After setup:
- [ ] `.env` file configured
- [ ] Virtual environment created
- [ ] Dependencies installed
- [ ] Database initialized (seed_data.py)
- [ ] Server running
- [ ] Default passwords changed

---

## 🚀 Next Steps

1. **Read QUICK_START.md** to get running
2. **Login to web interface** and test uploads
3. **Explore API docs** at `/docs`
4. **Read DATABASE_GUIDE.md** to understand models
5. **Start building** data processors for your files

---

## 📝 What's NOT Built Yet

These are intentionally left for you to implement based on your specific needs:

- File parsers (each data source has unique format)
- Transaction extraction from files
- Analytics dashboards
- Daily/monthly summaries
- Location management UI
- Org code management UI
- Advanced filtering

The foundation is complete - now add your business logic!

---

## 💡 Pro Tips

1. **Start Small** - Upload a test file first
2. **Check Logs** - Watch the console for errors
3. **Use API Docs** - `/docs` is interactive
4. **Read Comments** - Code is well-documented
5. **Test Locally** - Before deploying to production

---

**Ready? Start with [QUICK_START.md](QUICK_START.md)! 🎉**
