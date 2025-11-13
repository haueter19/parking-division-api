"""
Script to create initial admin user and seed database with basic data
"""
from app.db.session import SessionLocal, init_db
from app.models.database import User, UserRole, OrgCode
from app.utils.auth import get_password_hash


def create_admin_user():
    """Create initial admin user"""
    db = SessionLocal()
    
    try:
        # Check if admin already exists
        admin = db.query(User).filter(User.username == "admin").first()
        
        if not admin:
            admin = User(
                username="admin",
                email="dhaueter@cityofmadison.com",
                full_name="System Administrator",
                role=UserRole.ADMIN,
                hashed_password=get_password_hash("admin123"),  # Change this password!
                is_active=True
            )
            db.add(admin)
            db.commit()
            print("✅ Admin user created successfully!")
            print("   Username: admin")
            print("   Password: admin123")
            print("   ⚠️  IMPORTANT: Change this password immediately!")
        else:
            print("ℹ️  Admin user already exists")
        
        # Create sample users
        users_to_create = [
            {
                "username": "uploader1",
                "email": "uploader1@cityofmadison.com",
                "full_name": "File Uploader",
                "role": UserRole.UPLOADER,
                "password": "upload123"
            },
            {
                "username": "manager1",
                "email": "manager1@cityofmadison.com",
                "full_name": "Department Manager",
                "role": UserRole.MANAGER,
                "password": "manager123"
            },
            {
                "username": "viewer1",
                "email": "viewer1@cityofmadison.com",
                "full_name": "Report Viewer",
                "role": UserRole.VIEWER,
                "password": "viewer123"
            }
        ]
        
        for user_data in users_to_create:
            existing = db.query(User).filter(User.username == user_data["username"]).first()
            if not existing:
                user = User(
                    username=user_data["username"],
                    email=user_data["email"],
                    full_name=user_data["full_name"],
                    role=user_data["role"],
                    hashed_password=get_password_hash(user_data["password"]),
                    is_active=True
                )
                db.add(user)
                print(f"✅ Created {user_data['role'].value} user: {user_data['username']}")
        
        db.commit()
        
        # Create sample org codes
        sample_org_codes = [
            {"code": "PKG-001", "description": "Downtown Parking Operations"},
            {"code": "PKG-002", "description": "Garage Revenue"},
            {"code": "PKG-003", "description": "Street Meter Operations"},
            {"code": "PKG-004", "description": "Permit Program"},
            {"code": "PKG-005", "description": "Enforcement Revenue"},
        ]
        
        for org_data in sample_org_codes:
            existing = db.query(OrgCode).filter(OrgCode.code == org_data["code"]).first()
            if not existing:
                org_code = OrgCode(
                    code=org_data["code"],
                    description=org_data["description"],
                    is_active=True
                )
                db.add(org_code)
                print(f"✅ Created org code: {org_data['code']}")
        
        db.commit()
        print("\n✅ Database seeded successfully!")
        
    except Exception as e:
        print(f"❌ Error seeding database: {e}")
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    print("Initializing database...")
    init_db()
    print("✅ Database tables created")
    
    print("\nSeeding database with initial data...")
    create_admin_user()
    
    print("\n" + "="*60)
    print("SETUP COMPLETE!")
    print("="*60)
    print("\nYou can now start the application with:")
    print("  uvicorn app.main:app --reload")
    print("\nDefault login credentials:")
    print("  Admin    - Username: admin     Password: admin123")
    print("  Uploader - Username: uploader1 Password: upload123")
    print("  Manager  - Username: manager1  Password: manager123")
    print("  Viewer   - Username: viewer1   Password: viewer123")
    print("\n⚠️  IMPORTANT: Change all default passwords in production!")
