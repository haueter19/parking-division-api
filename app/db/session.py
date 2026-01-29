#from http import server
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker, Session
#from sqlalchemy.engine import URL
from typing import Generator
#from app.models import database

# Import your existing db_manager
#from db_manager import ConnectionManager
#cnxn = ConnectionManager()


# Get the engine using your existing connection system
#engine = cnxn.get_engine('PUReporting')
server1 = 'pubworksdbprd'
database1 = 'PUReporting'
connection_string = (
    f"mssql+pyodbc://@{server1}/{database1}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)
engine = create_engine(connection_string)


# Additional engine for external/secondary data sources (Traffic)
# Use ConnectionManager to obtain the 'Traffic' engine. This allows
# parts of the app (ETL, lookups) to open sessions against the Traffic DB.
server2 = 'arcsde'
database2 = 'Traffic'
connection_string = (
    f"mssql+pyodbc://@{server2}/{database2}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)
traffic_engine = create_engine(connection_string)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
SessionLocalTraffic = sessionmaker(autocommit=False, autoflush=False, bind=traffic_engine)

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