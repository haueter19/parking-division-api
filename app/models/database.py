from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Enum, Text, Numeric, JSON, Float
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import func
from app.db.session import Base
import enum
from datetime import datetime



def parse_time_string(t):
    if not t:
        return None
    
    t = t.strip()

    # Supported formats (add more if needed)
    formats = [
        "%I:%M:%S %p",  # 9:05:32 AM
        "%I:%M %p",     # 9:05 AM
        "%H:%M:%S",     # 14:37:55
        "%H:%M",        # 14:37
        "%H%M%S",       # 143755
        "%H%M",         # 1437
    ]

    for fmt in formats:
        try:
            return datetime.strptime(t, fmt).time()
        except ValueError:
            continue

    # Fallback: can't parse
    return None

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
    #METER = "meter"
    SINGLE_SPACE_METER = "single_space_meter"
    MULTI_SPACE_METER = "multi_space_meter"
    SMART_METER = "smart_meter"
    OTHER = "other"

class PaymentType(str, enum.Enum):
    """Payment type enumeration"""
    VISA = "visa"
    MASTERCARD = "mastercard"
    MC = 'mastercard'
    AMEX = "amex"
    DISCOVER = "discover"
    CASH = "cash"
    MOBILE = "mobile"
    PARK_SMARTER = 'park_smarter'
    TEXT_TO_PAY = 'text_to_pay'
    TEXT = 'text'
    OTHER = "other"  

class DataSourceType(str, enum.Enum):
    """Data source type enumeration for categorizing uploaded files"""
    WINDCAVE = "windcave"  # Windcave credit card settlements
    PAYMENTS_INSIDER_PAYMENTS = "pi_payments"  # Payments Insider credit card
    PAYMENTS_INSIDER_SALES = "pi_sales"  # Payments Insider sales report
    IPS_CC = "ips_cc"  # IPS credit card
    IPS_MOBILE = "ips_mobile"  # IPS credit card
    IPS_CASH = "ips_cash" 
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
    file_hash = Column(String(64), unique=True, index=True)
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
    payments_insider_sales_records = relationship("PaymentsInsiderSalesStaging", back_populates="source_file")
    payments_insider_payments_records = relationship("PaymentsInsiderPaymentsStaging", back_populates="source_file")
    ips_cc_records = relationship("IPSCreditCardStaging", back_populates="source_file")
    ips_mobile_records = relationship("IPSMobileStaging", back_populates="source_file")
    ips_cash_records = relationship("IPSCashStaging", back_populates="source_file")


# ============= Staging Tables =============

class WindcaveStaging(Base):
    __tablename__ = "windcave_staging"
    __table_args__ = {"schema": "app"}

    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, ForeignKey("uploaded_files.id"), nullable=False)

    time = Column(DateTime)
    settlement_date = Column(DateTime)
    group_account = Column(String(24))
    type = Column(String(5))
    authorized = Column(Integer)
    reference = Column(String(20))
    auth_code = Column(String(12))
    cur = Column(String(5))
    amount = Column(Float)
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
    caid = Column(String(24))
    catid = Column(Integer)
    merch_corp_ref = Column(Integer)
    order_number = Column(Integer)
    device_id = Column(String(20))
    voided = Column(Integer)
    cardnumber2 = Column(String(32))
    
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_to_final = Column(Boolean, default=False)
    transaction_id = Column(Integer, ForeignKey("transactions.id"))

    source_file = relationship("UploadedFile", back_populates="windcave_records")
    final_transaction = relationship("Transaction", back_populates="windcave_source")



class PaymentsInsiderSalesStaging(Base):
    """Staging table for Payments Insider credit card transactions"""
    __tablename__ = "payments_insider_sales_staging"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, ForeignKey("uploaded_files.id"), nullable=False)
    
    # Raw fields from PI reports - adjust based on actual columns
    business_name = Column(String(30))
    mid = Column(String(15))
    store_number = Column(Integer)
    card_brand = Column(String(20))
    card_number = Column(String(20))
    transaction_type = Column(String(20))
    void_ind = Column(String(3))
    settled_amount = Column(Numeric(10,2))
    settled_currency = Column(String(5))
    settled_date = Column(DateTime)
    transaction_amount = Column(Numeric(10,2))
    transaction_currency = Column(String(5))
    transaction_date = Column(DateTime)
    transaction_time = Column(String(8))
    authorization_code = Column(String(12))
    gbok__batch_id = Column(String(12))
    terminal_id = Column(String(24))
    exchange_type = Column(String(12))
    durbin_regulated = Column(String(1))
    roc_text = Column(String(10))
    invoice = Column(String(50))
    ticket_number = Column(String(20))
    order_number = Column(String(50))
    check_number = Column(String(20))
    custom_data_1 = Column(String(20))
    card_swipe_indicator = Column(String(16))
    pos_entry = Column(String(3))
    bnpl_product_code = Column(String(12))
    
    # Processing metadata
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_to_final = Column(Boolean, default=False)
    transaction_id = Column(Integer, ForeignKey("transactions.id"))
    matching_report_id = Column(Integer)  # ID of matching sales/payments report
    
    # Relationships
    source_file = relationship("UploadedFile", back_populates="payments_insider_sales_records")
    final_transaction = relationship("Transaction", back_populates="pi_sales_source")

    # Calculate datetime from date and time fields
    @hybrid_property
    def transaction_datetime(self):
        from datetime import datetime
        
        time_value = parse_time_string(self.transaction_time)
        if self.transaction_date and time_value:
            return datetime.combine(self.transaction_date, time_value)

class PaymentsInsiderPaymentsStaging(Base):
    """Staging table for Payments Insider credit card transaction payments"""
    __tablename__ = "payments_insider_payments_staging"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, ForeignKey("uploaded_files.id"), nullable=False)
    
    # Raw fields from PI reports - adjust based on actual columns
    payment_amount = Column(Numeric(10,2))
    currency = Column(String(3))
    transaction_amount = Column(Numeric(10,2))
    payment_no = Column(String(20))
    payment_date = Column(DateTime)
    fund_source = Column(String(3))
    account = Column(String(12))
    merchant_id = Column(String(10))
    business_name = Column(String(30))
    payment_type = Column(String(12))
    adjustment_description = Column(String(50))
    batch_amount = Column(Numeric(10,2))
    paid_to_others = Column(Numeric(10,2))
    paid_to_others_reason = Column(String(50))
    net_fund_batch_amount = Column(Numeric(10,2))
    gbok__batch_id = Column(String(12))
    terminal_id = Column(String(24))
    roc_text = Column(Integer)
    card_brand = Column(String(12))
    interchange_description = Column(String(40))
    card_number = Column(String(20))
    transaction_date = Column(DateTime)
    authorization_code = Column(String(8))
    settlement_date = Column(DateTime)
    case_id = Column(Integer)
    chargeback_code = Column(String(16))
    chargeback_description = Column(String(50))
    arn_number = Column(String(25))
    purchase_id_number = Column(String(30))
    airline_ticket_number = Column(String(30))
    store_numbe = Column(Integer)
        
    # Processing metadata
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_to_final = Column(Boolean, default=False)
    transaction_id = Column(Integer, ForeignKey("transactions.id"))
    matching_report_id = Column(Integer)  # ID of matching sales/payments report
    
    # Relationships
    source_file = relationship("UploadedFile", back_populates="payments_insider_payments_records")
    final_transaction = relationship("Transaction", back_populates="pi_payments_source")


class IPSCreditCardStaging(Base):
    """Staging table for IPS credit card transactions"""
    __tablename__ = "ips_cc_staging"
    __table_args__ = {"schema": "app"}
    
    id = Column(Integer, primary_key=True, index=True)
    source_file_id = Column(Integer, ForeignKey("uploaded_files.id"), nullable=False)
    
    # Raw fields from IPS CC - adjust based on actual columns
    settlement_date_time = Column(DateTime)
    transaction_reference = Column(String(15))
    transaction_date_time = Column(DateTime)
    zone = Column(String(24))
    area = Column(String(50))
    sub_area = Column(String(50))
    pole = Column(String(24))
    terminal = Column(String(12))
    batch_number = Column(Integer)
    authorization_code = Column(String(8))
    card_type = Column(String(12))
    card_number = Column(String(15))
    expiry = Column(String(8))
    amount = Column(Numeric(10,2))
    
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
    received_Date_time = Column(DateTime)
    zone = Column(String(24))
    area = Column(String(50))
    sub_area = Column(String(50))
    pole = Column(String(24))
    meter_type = Column(String(12))
    space_name = Column(Integer)
    license_plate = Column(String(10))
    prid = Column(Integer)
    paid = Column(Numeric(10,2))
    convenience_fee = Column(Numeric(10,2))
    time_purchased = Column(String(8))
    session_start_date_time = Column(DateTime)
    session_end_date_time = Column(DateTime)
    sms__ble = Column(String(8))
    sms__ble_received = Column(String(3))
    partner_name = Column(String(12))
    
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
    collection_time = Column(String(11))
    zone = Column(String(24))
    area = Column(String(50))
    sub_area = Column(String(50))
    pole_ser_no = Column(String(24))
    terminal = Column(String(12))
    meter_type = Column(String(12))
    pennies = Column(Numeric(10,2))
    nickels = Column(Numeric(10,2))
    dimes = Column(Numeric(10,2))
    quarters = Column(Numeric(10,2))
    dollars = Column(Numeric(10,2))
    coin_total = Column(Integer)
    coin_revenue = Column(Numeric(10,2))
    unrecognized_coins = Column(Integer)
    invalid_coin_revenue = Column(Numeric(10,2))
    coin_reversal_count = Column(Integer)
    
    # Processing metadata
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_to_final = Column(Boolean, default=False)
    transaction_id = Column(Integer, ForeignKey("transactions.id"))
    
    # Relationships
    source_file = relationship("UploadedFile", back_populates="ips_cash_records")
    final_transaction = relationship("Transaction", back_populates="ips_cash_source")

    # Calculate datetime from date and time fields
    @hybrid_property
    def transaction_datetime(self):
        from datetime import datetime
        
        time_value = parse_time_string(self.collection_time)
        if self.collection_date and time_value:
            return datetime.combine(self.collection_date, time_value)


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
    org_code = Column(Integer, index=True)  # Retrieved from terminal_id lookup
    
    # Audit trail - which staging record(s) created this transaction
    staging_table = Column(String(50))  # Which staging table this came from
    staging_record_id = Column(Integer)  # ID in that staging table
    
    # Processing metadata
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
    
    # Relationships back to staging tables (for audit trail)
    windcave_source = relationship("WindcaveStaging", back_populates="final_transaction", uselist=False)
    pi_sales_source = relationship("PaymentsInsiderSalesStaging", back_populates="final_transaction", uselist=False)
    pi_payments_source = relationship("PaymentsInsiderPaymentsStaging", back_populates="final_transaction", uselist=False)
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
    start_time = Column(DateTime, server_default=func.now())
    end_time = Column(DateTime)
    records_processed = Column(Integer)
    records_created = Column(Integer)
    records_updated = Column(Integer)
    records_failed = Column(Integer)
    status = Column(String(20))  # 'running', 'completed', 'failed'
    error_message = Column(Text)
    
    # Relationship
    source_file = relationship("UploadedFile")