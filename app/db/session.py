from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator

# Import your existing db_manager
from db_manager import ConnectionManager
cnxn = ConnectionManager()

# Get the engine using your existing connection system
engine = cnxn.get_engine('PUReporting')

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for declarative models
metadata = MetaData(schema="app")
Base = declarative_base(metadata=metadata)


def get_db() -> Generator[Session, None, None]:
    """
    Dependency that provides a database session.
    
    Usage in FastAPI endpoints:
        @app.get("/items/")
        def read_items(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database - create all tables"""
    Base.metadata.create_all(bind=engine)