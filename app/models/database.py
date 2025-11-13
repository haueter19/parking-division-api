from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Enum, Text, Numeric, JSON, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.db.session import Base
import enum



class UserRole(str, enum.Enum):
    """User role enumeration"""
    ADMIN = "admin"
    MANAGER = "manager"
    VIEWER = "viewer"
    UPLOADER = "uploader"

class LocationType(str, enum.Enum):
    """Location type for transactions"""
    GARAGE = "garage"
    LOT = "lot"
    METER = "meter"
    OTHER = "other"

class PaymentType(str, enum.Enum):
    """Payment type enumeration"""
    VISA = "visa"
    MASTERCARD = "mastercard"
    AMEX = "amex"
    DISCOVER = "discover"
    CASH = "cash"
    MOBILE = "mobile"
    OTHER = "other"  

class DataSourceType(str, enum.Enum):
    """Data source type enumeration for categorizing uploaded files"""
    WINDCAVE = "windcave"  # Windcave credit card settlements
    PAYMENTS_INSIDER = "payments_insider"  # Payments Insider credit card
    IPS_CC = "ips_cc"  # IPS credit card
    IPS_PBP = "ips_pbp"  # IPS credit card
    IPS_Cash = "ips_cash" 
    CASH_COLLECTION = "cash_collection"  # Cash collection PDFs
    RP3_PERMITS = "rp3_permits"  # Residential Parking Permit Program
    MONTHLY_PERMITS = "monthly_permits"  # Monthly parking permits
    GARAGE_TRANSACTIONS = "garage_transactions"  # Garage visit transactions
    PARKING_TICKETS = "parking_tickets"  # AIMS parking tickets
    TOWED_VEHICLES = "towed_vehicles"  # Towed/abandoned vehicles
    OOPH_CHARGES = "ooph_charges"  # Out of hours parking charges
    METER_USAGE = "meter_usage"  # IPS meter usage and revenue
    PCI_INSPECTIONS = "pci_inspections"  # PCI compliance inspections
    OTHER = "other"


class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), unique=True, nullable=False, index=True)
    full_name = Column(String(100))
    hashed_password = Column(String(255), nullable=False)
    role = Column(Enum(UserRole), nullable=False, default=UserRole.VIEWER)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    uploaded_files = relationship("UploadedFile", back_populates="uploader")


class UploadedFile(Base):
    __tablename__ = "uploaded_files"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), nullable=False)
    original_filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    file_size = Column(Integer, nullable=False)
    data_source_type = Column(Enum(DataSourceType), nullable=False, index=True)
    uploaded_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    description = Column(Text)
    upload_date = Column(DateTime(timezone=True), server_default=func.now())
    is_processed = Column(Boolean, default=False, nullable=False)
    processed_at = Column(DateTime(timezone=True))
    records_processed = Column(Integer)
    processing_errors = Column(JSON)  # Store any errors encountered
    
    # Relationships
    uploader = relationship("User", back_populates="uploaded_files")
    
    @property
    def uploaded_by_user(self):
        """Alias property for API/serialization compatibility.

        Some parts of the codebase (Pydantic schemas and templates) expect
        an attribute named `uploaded_by_user`. The actual relationship is
        named `uploader`. Expose this property so Pydantic's
        `from_attributes=True` can find the nested User object.
        """
        return self.uploader
    # Link to staging records for audit trail
    windcave_records = relationship("WindcaveStaging", back_populates="source_file")
    payments_insider_records = relationship("PaymentsInsiderStaging", back_populates="source_file")
    ips_cc_records = relationship("IPSCreditCardStaging", back_populates="source_file")
    ips_mobile_records = relationship("IPSMobileStaging", back_populates="source_file")
    ips_cash_records = relationship("IPSCashStaging", back_populates="source_file")


# ============= Staging Tables =============

class WindcaveStaging(Base):
    """Staging table for Windcave credit card transactions"""
    __tablename__ = "windcave_staging"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, ForeignKey("uploaded_files.id"), nullable=False)
    
    # Raw fields from Windcave CSV - adjust these based on actual columns
    time = Column(DateTime)
    settlement_date = Column(String(20))
    group_account = Column(String(24))
    type = Column(String(5))
    authorized = Column(Integer)
    reference = Column(Integer)
    auth_code = Column(String(12))
    cur = Column(String(5))
    amount = Column(Numeric(10, 2))
    card_num = Column(String(12))
    card_type = Column(String(20))
    card_holder_name = Column(String(50))
    dpstxnref = Column(String(20))
    txnref = Column(String(32))
    reco = Column(Integer)
    responsetext = Column(String(24))
    billingid = Column(Integer)
    dpsbillingid = Column(Integer)
    txndata1 = Column(String(20))
    txndata2 = Column(String(20))
    txndata3 = Column(String(20))
    username = Column(String(20))
    caid = Column(Integer)
    catid = Column(Integer)
    merch_corp_ref = Column(Integer)
    order_number = Column(Integer)
    device_id = Column(String(20))
    voided = Column(Integer)
    cardnumber2 = Column(Integer)

    # Processing metadata
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_to_final = Column(Boolean, default=False)
    transaction_id = Column(Integer, ForeignKey("transactions.id"))  # Link to final transaction
    
    # Relationships
    source_file = relationship("UploadedFile", back_populates="windcave_records")
    final_transaction = relationship("Transaction", back_populates="windcave_source")


class PaymentsInsiderStaging(Base):
    """Staging table for Payments Insider credit card transactions"""
    __tablename__ = "payments_insider_staging"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, ForeignKey("uploaded_files.id"), nullable=False)
    report_type = Column(String(20))  # 'sales' or 'payments'
    
    # Raw fields from PI reports - adjust based on actual columns
    transaction_date = Column(DateTime)
    payment_date = Column(DateTime)
    amount = Column(Numeric(10, 2))
    card_type = Column(String(50))
    terminal_id = Column(String(100))
    location = Column(String(255))
    reference_number = Column(String(255))
    batch_number = Column(String(100))
    
    # Processing metadata
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_to_final = Column(Boolean, default=False)
    transaction_id = Column(Integer, ForeignKey("transactions.id"))
    matching_report_id = Column(Integer)  # ID of matching sales/payments report
    
    # Relationships
    source_file = relationship("UploadedFile", back_populates="payments_insider_records")
    final_transaction = relationship("Transaction", back_populates="pi_source")


class IPSCreditCardStaging(Base):
    """Staging table for IPS credit card transactions"""
    __tablename__ = "ips_cc_staging"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, ForeignKey("uploaded_files.id"), nullable=False)
    
    # Raw fields from IPS CC - adjust based on actual columns
    transaction_date = Column(DateTime)
    amount = Column(Numeric(10, 2))
    terminal_id = Column(String(100))
    location = Column(String(255))
    card_type = Column(String(50))
    reference = Column(String(255))
    
    # Processing metadata
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_to_final = Column(Boolean, default=False)
    transaction_id = Column(Integer, ForeignKey("transactions.id"))
    
    # Relationships
    source_file = relationship("UploadedFile", back_populates="ips_cc_records")
    final_transaction = relationship("Transaction", back_populates="ips_cc_source")


class IPSMobileStaging(Base):
    """Staging table for IPS mobile payment transactions"""
    __tablename__ = "ips_mobile_staging"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, ForeignKey("uploaded_files.id"), nullable=False)
    
    # Raw fields from IPS Mobile - adjust based on actual columns
    transaction_date = Column(DateTime)
    amount = Column(Numeric(10, 2))
    phone_number = Column(String(20))  # Masked/partial
    location = Column(String(255))
    meter_id = Column(String(100))
    payment_method = Column(String(50))  # SMS or App
    
    # Processing metadata
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_to_final = Column(Boolean, default=False)
    transaction_id = Column(Integer, ForeignKey("transactions.id"))
    
    # Relationships
    source_file = relationship("UploadedFile", back_populates="ips_mobile_records")
    final_transaction = relationship("Transaction", back_populates="ips_mobile_source")


class IPSCashStaging(Base):
    """Staging table for IPS cash transactions (coins in meters)"""
    __tablename__ = "ips_cash_staging"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, ForeignKey("uploaded_files.id"), nullable=False)
    
    # Raw fields from IPS Cash reports
    collection_date = Column(DateTime)
    amount = Column(Numeric(10, 2))
    meter_id = Column(String(100))
    location = Column(String(255))
    collector_id = Column(String(100))
    
    # Processing metadata
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_to_final = Column(Boolean, default=False)
    transaction_id = Column(Integer, ForeignKey("transactions.id"))
    
    # Relationships
    source_file = relationship("UploadedFile", back_populates="ips_cash_records")
    final_transaction = relationship("Transaction", back_populates="ips_cash_source")


class SQLCashStaging(Base):
    """Staging table for cash transactions from SQL queries"""
    __tablename__ = "sql_cash_staging"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Fields from your SQL query results
    transaction_date = Column(DateTime)
    amount = Column(Numeric(10, 2))
    location = Column(String(255))
    terminal_id = Column(String(100))
    reference = Column(String(255))
    
    # Processing metadata
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_to_final = Column(Boolean, default=False)
    transaction_id = Column(Integer, ForeignKey("transactions.id"))
    
    # Relationships
    final_transaction = relationship("Transaction", back_populates="sql_cash_source")


# ============= UPDATED/NEW Transaction Model (Normalized Final Table) =============
class Transaction(Base):
    """
    Normalized transactions table - the final destination for all payment data
    This replaces your existing Transaction model
    """
    __tablename__ = "transactions"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Core transaction fields (required)
    transaction_date = Column(DateTime, nullable=False, index=True)
    transaction_amount = Column(Numeric(10, 2), nullable=False)
    
    # Settlement fields (nullable for cash transactions)
    settle_date = Column(DateTime, index=True)  # NULL for cash, same as transaction_date
    settle_amount = Column(Numeric(10, 2))  # May differ from transaction_amount due to fees
    
    # Source and location information
    source = Column(Enum(DataSourceType), nullable=False, index=True)
    location_type = Column(Enum(LocationType), nullable=False)
    location_name = Column(String(255))
    device_terminal_id = Column(String(100), index=True)
    
    # Payment information
    payment_type = Column(Enum(PaymentType), nullable=False)
    
    # Additional fields for tracking
    reference_number = Column(String(255))  # Original transaction reference
    org_code = Column(String(50), index=True)  # Retrieved from terminal_id lookup
    
    # Audit trail - which staging record(s) created this transaction
    staging_table = Column(String(50))  # Which staging table this came from
    staging_record_id = Column(Integer)  # ID in that staging table
    
    # Processing metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships back to staging tables (for audit trail)
    windcave_source = relationship("WindcaveStaging", back_populates="final_transaction", uselist=False)
    pi_source = relationship("PaymentsInsiderStaging", back_populates="final_transaction", uselist=False)
    ips_cc_source = relationship("IPSCreditCardStaging", back_populates="final_transaction", uselist=False)
    ips_mobile_source = relationship("IPSMobileStaging", back_populates="final_transaction", uselist=False)
    ips_cash_source = relationship("IPSCashStaging", back_populates="final_transaction", uselist=False)
    sql_cash_source = relationship("SQLCashStaging", back_populates="final_transaction", uselist=False)


# ============= Optional: ETL Processing Log =============
class ETLProcessingLog(Base):
    """Track ETL processing runs"""
    __tablename__ = "etl_processing_log"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    source_table = Column(String(50), nullable=False)
    source_file_id = Column(Integer, ForeignKey("uploaded_files.id"))
    start_time = Column(DateTime(timezone=True), server_default=func.now())
    end_time = Column(DateTime(timezone=True))
    records_processed = Column(Integer)
    records_created = Column(Integer)
    records_updated = Column(Integer)
    records_failed = Column(Integer)
    status = Column(String(20))  # 'running', 'completed', 'failed'
    error_message = Column(Text)
    
    # Relationship
    source_file = relationship("UploadedFile")