from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator

# Import your existing db_manager
from db_manager import ConnectionManager
cnxn = ConnectionManager()

# Get the engine using your existing connection system
engine = cnxn.get_engine('PUReporting')

# Additional engine for external/secondary data sources (Traffic)
# Use ConnectionManager to obtain the 'Traffic' engine. This allows
# parts of the app (ETL, lookups) to open sessions against the Traffic DB.
engine_traffic = cnxn.get_engine('Traffic')


# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
SessionLocalTraffic = sessionmaker(autocommit=False, autoflush=False, bind=engine_traffic)

# Base class for declarative models
metadata = MetaData(schema="app")
Base = declarative_base(metadata=metadata)

metadata_Traffic = MetaData(schema="data_admin8")
Base_Traffic = declarative_base(metadata=metadata_Traffic)

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


def get_traffic_db() -> Generator[Session, None, None]:
    """
    Dependency that provides a database session bound to the Traffic engine.

    Use in endpoints or background tasks that need to query the Traffic DB:

        def handler(db: Session = Depends(get_traffic_db)):
            ...
    """
    db = SessionLocalTraffic()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database - create all tables"""
    Base.metadata.create_all(bind=engine)