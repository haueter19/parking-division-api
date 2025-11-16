"""
ETL Processing Functions for Data Lake
Transforms data from staging tables to normalized transactions table
"""

from datetime import datetime
from typing import Optional, Dict, List, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from sqlalchemy import Table, MetaData, select, insert, text
from sqlalchemy.ext.hybrid import hybrid_property
import numpy as np
import pandas as pd
from app.models.database import (
    Transaction, DataSourceType, LocationType, PaymentType,
    WindcaveStaging, PaymentsInsiderPaymentsStaging, PaymentsInsiderSalesStaging, 
    IPSCreditCardStaging, IPSMobileStaging, IPSCashStaging, SQLCashStaging,
    ETLProcessingLog, UploadedFile, PU_PARCS_UCD, PU_REVENUE_CREDITCARDTERMINALS
)
#from db_manager import ConnectionManager
#cnxn = ConnectionManager()

class ETLProcessor:
    """Main ETL processor for transforming staging data to final transactions"""
    
    def __init__(self, db: Session, traffic_db: Optional[Session] = None):
        """
        ETLProcessor can accept two session objects:
        - db: primary application DB session (PUReporting)
        - traffic_db: optional session bound to the Traffic engine

        If `traffic_db` is provided, `get_org_code` will query the Traffic
        database tables (`PU_PARCS_UCD` and `PU_REVENUE_CREDITCARDTERMINALS`) and
        return the matching org_code. If not provided, the method falls back to
        the default behavior (placeholder/None).
        """
        self.db = db
        self.traffic_db = traffic_db
        self.charge_code_from_housing_id = None
        self.charge_code_from_terminal_id = None
        self.garage_from_station = None
        
    def get_org_code(self) -> Optional[pd.DataFrame]:
        """
        Get org code for a terminal ID
        This should connect to your existing terminal/org_code tables
        """

        # If a Traffic DB session was provided, query the Traffic DB tables.
        # We'll use SQLAlchemy Core (select + union) with table reflection so
        # we don't need ORM model classes for those legacy tables.

        try:
            org_lookup_tbl = pd.read_sql("""
                with ucds as (
                    SELECT * FROM [Traffic].[data_admin8].[PU_PARCS_UCD] WHERE HousingID IS NOT NULL AND ChargeCode IS NOT NULL
                ), cc_terminals as (
                    SELECT * FROM [Traffic].[data_admin8].[PU_REVENUE_CREDITCARDTERMINALS] WHERE ChargeCode IS NOT NULL
                )
                SELECT 
                    ucds.HousingID, CONCAT('0010050008016090',CAST(ucds.TerminalID As varchar)) TerminalID, ucds.ChargeCode, 'Windcave' as Brand
                FROM ucds 
                UNION
                SELECT
                    NULL, cc_terminals.TerminalID, cc_terminals.ChargeCode, Brand
                FROM cc_terminals
                ORDER BY TerminalID
                """, self.traffic_db.get_bind())

            
            # Get station to garage mapping
            garage_and_station_records = pd.read_sql(text("""
                    SELECT 
                        Location.TxnT2StationAdddress, pa.ParkingName Garage
                    FROM Location
                    INNER JOIN ParkingAdmin pa On (Location.Id_Parking=pa.Id_Parking)
                   """), self.db.get_bind())
            
            # Turn DataFrame into dicts
            charge_code_from_housing_id = {a:b for a,b in zip(org_lookup_tbl['HousingID'], org_lookup_tbl['ChargeCode']) if a != None}
            charge_code_from_terminal_id = {a:b for a,b in zip(org_lookup_tbl['TerminalID'], org_lookup_tbl['ChargeCode']) if a != None}
            garage_from_station = {a:b for a,b in zip(garage_and_station_records['TxnT2StationAdddress'], garage_and_station_records['Garage']) if a != None}

            # Additional hardcoded mappings -- remove after updating Traffic DB
            charge_code_from_terminal_id['0010050008031494050786'] = 82088
            charge_code_from_terminal_id['0010050008031494050908'] = 82074

            # Save dicts to the class
            self.charge_code_from_housing_id = charge_code_from_housing_id
            self.charge_code_from_terminal_id = charge_code_from_terminal_id
            self.garage_from_station = garage_from_station

        except Exception as e:
            # On error, log/print and fall back to None
            print(f"Error querying Traffic DB for org_codes: {e}")
            
            self.org_code_cache = org_lookup_tbl
            return org_lookup_tbl

        self.org_code_cache = org_lookup_tbl
        return org_lookup_tbl
    
    def determine_location_type(self, location_name: str, terminal_id: str = None) -> LocationType:
        """Determine location type from location name or terminal ID"""
        location_lower = (location_name or "").lower()
        
        if "garage" in location_lower or "parking structure" in location_lower:
            return LocationType.GARAGE
        elif "lot" in location_lower or "surface" in location_lower:
            return LocationType.LOT
        elif "meter" in location_lower or terminal_id and terminal_id.startswith("M"):
            return LocationType.SINGLE_SPACE_METER
        else:
            return LocationType.OTHER
    
    def map_payment_type(self, card_type: str = None, source: DataSourceType = None) -> PaymentType:
        """Map card type string to PaymentType enum"""
        if not card_type and source in [DataSourceType.IPS_CASH, DataSourceType.SQL_CASH_QUERY]:
            return PaymentType.CASH
        elif not card_type and source == DataSourceType.IPS_MOBILE:
            return PaymentType.MOBILE
        
        card_lower = (card_type or "").lower()
        if "visa" in card_lower:
            return PaymentType.VISA
        elif "mastercard" in card_lower or "master" in card_lower:
            return PaymentType.MASTERCARD
        elif "amex" in card_lower or "american express" in card_lower:
            return PaymentType.AMEX
        elif "discover" in card_lower:
            return PaymentType.DISCOVER
        else:
            return PaymentType.OTHER
    
        

    def process_windcave(self, file_id: Optional[int] = None) -> Dict[str, Any]:
        """Process Windcave staging records to final transactions"""
        log_entry = self._start_log("windcave_staging", file_id)
        
        if self.org_code_cache is None:
            self.get_org_code()
            
        try:
            # Query unprocessed records
            query = self.db.query(WindcaveStaging).filter(
                WindcaveStaging.processed_to_final == False,
                WindcaveStaging.voided == 0
            )
            #qry = text("SELECT * FROM app.windcave_staging WHERE processed_to_final = :processed_to_final AND voided = 0")
            #records = pd.read_sql(qry, self.db.get_bind(), params={'processed_to_final':0})
            
            if file_id:
                query = query.filter(WindcaveStaging.source_file_id == file_id)
                #records = records[records['source_file_id']==file_id]
            
            # Query to get the records
            records = query.all()

            # For any device_id with len > 3, use the first part of txnref
            for record in records:
                if len(record.device_id) > 3:
                    record.device_id = record.txnref.split('-')[0]
            #records['txnref'] = records['txnref'].apply(lambda x: x.split('-')[0])

            # Merge on device_id to HousingID to get ChargeCode
            #records.merge(self.org_lookup_tbl[['HousingID', 'ChargeCode']], left_on='device_id', right_on='HousingID', how='left')

            # Sometimes device_id has a weird value. In this case, use txnref
            #records.merge(self.org_lookup_tbl[['HousingID', 'ChargeCode']], left_on='txnref', right_on='HousingID', how='left', suffixes=['','_y'])
            
            # Fill missing ChargeCode from second merge
            #records.fillna({'ChargeCode':records['ChargeCode_y']},inplace=True)

            created_count = 0
            failed_count = 0
            
            for _, record in records.iterrows():
                try:
                    transaction = Transaction(
                        transaction_date=record['time'],
                        transaction_amount=record['amount'],
                        settle_date=record['settlement_date'],
                        settle_amount=record['amount'],
                        source=DataSourceType.WINDCAVE,
                        location_type=LocationType.GARAGE,
                        location_name=self.garage_from_station(record['device_id']), 
                        device_terminal_id=record['device_id'], # May need to look at this. Do I care about the weird ones going to final?
                        payment_type=self.map_payment_type(record['card_type']),
                        reference_number=record['dpstxnref'], # Do I need reference number? Is this the best choice?
                        org_code=self.charge_code_from_housing_id.get(record['device_id']) or self.charge_code_from_housing_id.get(record['txn']), # Try using device_id, fail to txn
                        staging_table="windcave_staging",
                        staging_record_id=record['id']
                    )
                    
                    self.db.add(transaction)
                    self.db.flush()
                    
                    # Update staging record
                    record['processed_to_final'] = True
                    record['transaction_id'] = transaction.id
                    created_count += 1
                    
                except Exception as e:
                    failed_count += 1
                    print(f"Error processing Windcave record {record.id}: {e}")
            
            self.db.commit()
            self._complete_log(log_entry, len(records), created_count, 0, failed_count)
            
            return {
                "success": True,
                "records_processed": len(records),
                "records_created": created_count,
                "records_failed": failed_count
            }
            
        except Exception as e:
            self.db.rollback()
            self._fail_log(log_entry, str(e))
            raise
    
    def process_payments_insider(self, file_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Process Payments Insider staging records to final transactions
        Note: PI requires matching Sales and Payments reports
        """
        log_entry = self._start_log("payments_insider_sales_staging", file_id)
        
        try:
            # Query sales LEFT JOIN payments (match SQL behavior).
            # Select sales rows (even when there's no matching payment) where
            # the sale hasn't been processed and is not voided.
            query = self.db.query(
                PaymentsInsiderSalesStaging,
                PaymentsInsiderPaymentsStaging
            ).outerjoin(
                PaymentsInsiderPaymentsStaging,
                PaymentsInsiderSalesStaging.invoice == PaymentsInsiderPaymentsStaging.purchase_id_number
            ).filter(
                and_(
                    PaymentsInsiderSalesStaging.processed_to_final == False,
                    PaymentsInsiderSalesStaging.void_ind == 'N'
                )
            )
            
            if file_id:
                query = query.filter(PaymentsInsiderSalesStaging.source_file_id == file_id)
            
            records = query.all()
            created_count = 0
            failed_count = 0
            
            for sales_record, payment_record in records:
                try:
                    
                    transaction = Transaction(
                        transaction_date=sales_record.transaction_datetime,
                        transaction_amount=sales_record.transaction_amount,
                        settle_date=payment_record.payment_date if payment_record else None,
                        settle_amount=payment_record.payment_amount if payment_record else None,
                        source=DataSourceType.PAYMENTS_INSIDER_SALES,
                        location_type=self.determine_location_type(sales_record.location),
                        location_name=sales_record.location,
                        device_terminal_id=sales_record.terminal_id,
                        payment_type=self.map_payment_type(sales_record.card_brand),
                        reference_number=sales_record.invoice,
                        org_code=self.get_org_code(sales_record.terminal_id),
                        staging_table="payments_insider_sales_staging",
                        staging_record_id=sales_record.id
                    )
                    
                    self.db.add(transaction)
                    self.db.flush()
                    
                    # Update both staging records
                    sales_record.processed_to_final = True
                    sales_record.transaction_id = transaction.id
                    if payment_record:
                        payment_record.processed_to_final = True
                        payment_record.transaction_id = transaction.id
                    created_count += 1
                    
                except Exception as e:
                    failed_count += 1
                    print(f"Error processing PI record {sales_record.id}: {e}")
            
            self.db.commit()
            self._complete_log(log_entry, len(records), created_count, 0, failed_count)
            
            return {
                "success": True,
                "records_processed": len(records),
                "records_created": created_count,
                "records_failed": failed_count
            }
            
        except Exception as e:
            self.db.rollback()
            self._fail_log(log_entry, str(e))
            raise
    
    
    def process_ips_cash(self, file_id: Optional[int] = None) -> Dict[str, Any]:
        """Process IPS Cash staging records to final transactions"""
        log_entry = self._start_log("ips_cash_staging", file_id)
        
        try:
            query = self.db.query(IPSCashStaging).filter(
                IPSCashStaging.processed_to_final == False
            )
            if file_id:
                query = query.filter(IPSCashStaging.source_file_id == file_id)
            
            records = query.all()
            created_count = 0
            failed_count = 0
            
            for record in records:
                try:
                    # For cash, settle date = transaction date
                    transaction = Transaction(
                        transaction_date=record.collection_date,
                        transaction_amount=record.coin_revenue,
                        settle_date=record.collection_date,  # Same as transaction date for cash
                        settle_amount=record.coin_revenue,
                        source=DataSourceType.IPS_CASH,
                        location_type=LocationType.METER,  # NEED TO FIND Single Space or Multi-Space
                        location_name=record.pole_ser_no,
                        device_terminal_id=record.terminal,
                        payment_type=PaymentType.CASH,
                        reference_number=record.id,
                        org_code=self.get_org_code(record.meter_id), # 82088 for single, 82074 for multi
                        staging_table="ips_cash_staging",
                        staging_record_id=record.id
                    )
                    
                    self.db.add(transaction)
                    self.db.flush()
                    
                    record.processed_to_final = True
                    record.transaction_id = transaction.id
                    created_count += 1
                    
                except Exception as e:
                    failed_count += 1
                    print(f"Error processing IPS Cash record {record.id}: {e}")
            
            self.db.commit()
            self._complete_log(log_entry, len(records), created_count, 0, failed_count)
            
            return {
                "success": True,
                "records_processed": len(records),
                "records_created": created_count,
                "records_failed": failed_count
            }
            
        except Exception as e:
            self.db.rollback()
            self._fail_log(log_entry, str(e))
            raise
    
    def process_all_staging_tables(self, file_id: Optional[int] = None) -> Dict[str, Any]:
        """Process all staging tables to final transactions"""
        results = {}
        
        # Process each staging table
        processors = [
            ("windcave", self.process_windcave),
            ("payments_insider", self.process_payments_insider),
            ("ips_cash", self.process_ips_cash),
            
            # Add other processors as needed
        ]
        
        for name, processor in processors:
            try:
                results[name] = processor(file_id)
            except Exception as e:
                results[name] = {
                    "success": False,
                    "error": str(e)
                }
        
        return results
    
    def _parse_time_string(self, t):
        if not t:
            return None
        
        t = t.strip()

        # Adjust these formats depending on real data
        formats = ["%H:%M:%S", "%H:%M", "%H%M%S", "%H%M"]

        for fmt in formats:
            try:
                return datetime.strptime(t, fmt).time()
            except ValueError:
                continue

        # Fallback: can't parse
        return None

    
    # Logging helper methods
    def _start_log(self, source_table: str, file_id: Optional[int]) -> ETLProcessingLog:
        """Start a processing log entry"""
        log_entry = ETLProcessingLog(
            source_table=source_table,
            source_file_id=file_id,
            status="running"
        )
        self.db.add(log_entry)
        self.db.flush()
        return log_entry
    
    def _complete_log(self, log_entry: ETLProcessingLog, processed: int, 
                     created: int, updated: int, failed: int):
        """Complete a processing log entry"""
        log_entry.end_time = datetime.now()
        log_entry.records_processed = processed
        log_entry.records_created = created
        log_entry.records_updated = updated
        log_entry.records_failed = failed
        log_entry.status = "completed"
        self.db.commit()
    
    def _fail_log(self, log_entry: ETLProcessingLog, error_message: str):
        """Mark a processing log entry as failed"""
        log_entry.end_time = datetime.now()
        log_entry.status = "failed"
        log_entry.error_message = error_message
        self.db.commit()


class DataLoader:
    """Load data from files to staging tables"""
    
    def __init__(self, db: Session, data_source_type: DataSourceType):
        self.db = db
        self.data_source_type = data_source_type
        self.mapping = {
            DataSourceType.WINDCAVE: self.load_windcave_csv,
            DataSourceType.PAYMENTS_INSIDER_PAYMENTS: self.load_payments_insider,
            DataSourceType.PAYMENTS_INSIDER_SALES: self.load_payments_insider,
            DataSourceType.IPS_CC: self.load_ips_credit,
            DataSourceType.IPS_MOBILE: self.load_ips_mobile,
            DataSourceType.IPS_CASH: self.load_ips_cash,
            #DataSourceType.SQL_CASH_QUERY: self.load_sql_cash_query
            # Add other mappings as needed
            }
    
    def load_windcave_csv(self, file_path: str, file_id: int) -> int:
        """Load Windcave CSV to staging table"""
        
        if file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
        
        # --- Normalize column names ---
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("/","").str.replace('\n','').str.replace('.','')
    
        # --- Add metadata columns ---
        df["source_file_id"] = file_id
        df["processed_to_final"] = False

        # --- Convert datetimes where possible ---
        for col in df.columns:
            if "date" in col or "time" in col:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception:
                    pass

        # --- Convert large integers to string ---
        df['caid'] = df['caid'].astype(str)
        df['cardnumber2'] = df['cardnumber2'].astype(str)

        # --- Handle integer columns - replace NaN with None ---
        int_columns = ['authorized', 'reco', 'billingid', 'dpsbillingid', 
                   'catid', 'merch_corp_ref', 'order_number', 'voided']
        
        for col in int_columns:
            if col in df.columns:
                # Convert to nullable integer type or replace NaN with None
                df[col] = df[col].replace({pd.NA: None, np.nan: None})
                # Convert to int where not None
                df.loc[df[col].notna(), col] = df[col].loc[df[col].notna()].astype(int)
        
        # --- Convert pandas NaN to None for SQL ---
        df = df.replace({pd.NA: None, np.nan: None, pd.NaT: None})
        
        # --- Convert to list of dictionaries ---
        records = df.to_dict(orient="records")

        # --- Bulk insert using SQLAlchemy ---
        self.db.execute(insert(WindcaveStaging), records)
        self.db.commit()
        
        # Update file as processed
        file_record = self.db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
        if file_record:
            file_record.is_processed = True
            file_record.processed_at = datetime.now()
            file_record.records_processed = len(records)
            self.db.commit()
        
        return len(records)
    
    def load_payments_insider(self, file_path: str, file_id: int, report_type: Optional[str] = None) -> int:
        """Load Payments Insider report to staging table"""

        # Determine Sales or Payments from filename
        
        if not report_type:
            if 'sales' in self.data_source_type.value.lower():
                report_type = 'Sales'
            if 'payments' in self.data_source_type.value.lower():
                report_type = 'Payments'
        
        # Establish dtypes
        set_dtypes = {'MID':str, 'Merchant ID':str, 'Terminal ID':str, 'GBOK / Batch ID':str, 'Payment No.':str}
        
        # Determine if Excel or CSV
        if file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path, sheet_name=report_type, skiprows=1, dtype=set_dtypes)
        else:
            df = pd.read_csv(file_path, dtype=set_dtypes)

        # --- Normalize column names ---
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("/","").str.replace('\n','').str.replace('.','')

        # --- Add metadata columns ---
        df["source_file_id"] = file_id
        df["processed_to_final"] = False
        
        # --- Convert datetimes where possible ---
        for col in df.columns:
            if "date" in col:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception:
                    pass

        # --- Handle integer columns - replace NaN with None ---
        int_columns = ['store_number', 'store_numbe', 'pos_entry', 'roc_text', 'case_id']
        
        for col in int_columns:
            if col in df.columns:
                # Convert to nullable integer type or replace NaN with None
                df[col] = df[col].replace({pd.NA: None, np.nan: None})
                # Convert to int where not None
                df.loc[df[col].notna(), col] = df[col].loc[df[col].notna()].astype(int)
        
        # --- Convert pandas NaN to None for SQL ---
        df = df.replace({pd.NA: None, np.nan: None, pd.NaT: None})

        # --- Convert to list of dictionaries ---
        records = df.to_dict(orient="records")

        # --- Bulk insert using SQLAlchemy ---
        if report_type == 'Sales':
            self.db.execute(insert(PaymentsInsiderSalesStaging), records)
        else:
            self.db.execute(insert(PaymentsInsiderPaymentsStaging), records)
        self.db.commit()

        # Update file as processed
        file_record = self.db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
        if file_record:
            file_record.is_processed = True
            file_record.processed_at = datetime.now()
            file_record.records_processed = len(records)
            self.db.commit()
        
        return len(records)

    def load_ips_credit(self, file_path: str, file_id: int, convenience_fee: float = 0.45) -> int:
        """Load IPS data to staging table"""
        
        # Determine if Excel or CSV
        if file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)

        # --- Check for a sum or total at the bottom of the report and remove it ---
        df = df[df['Transaction Date Time'].notna()]
        
        # --- Normalize column names ---
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("/","").str.replace('\n','').str.replace('.','')
        df.rename(columns=({'Amount ($)':'amount', '$ Paid':'paid', '$0.01':'pennies', '$0.05':'nickels', '$0.10':'dimes', '$0.25':'quarters', '$1.00':'dollars'}), inplace=True)
        
        # --- Add metadata columns ---
        df["source_file_id"] = file_id
        df["processed_to_final"] = False
        
        # --- Convert datetimes where possible ---
        for col in df.columns:
            if "date" in col:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception:
                    pass
                    
        # --- Handle integer columns - replace NaN with None ---
        int_columns = ['batch_number']
        
        for col in int_columns:
            if col in df.columns:
                # Convert to nullable integer type or replace NaN with None
                df[col] = df[col].replace({pd.NA: None, np.nan: None})
                # Convert to int where not None
                df.loc[df[col].notna(), col] = df[col].loc[df[col].notna()].astype(int)
                
        # --- Convert pandas NaN to None for SQL ---
        df = df.replace({pd.NA: None, np.nan: None, pd.NaT: None})
        
        # --- Convert to list of dictionaries ---
        records = df.to_dict(orient="records")
        
        # --- Bulk insert using SQLAlchemy ---
        self.db.execute(insert(IPSCreditCardStaging), records)
        self.db.commit()

        # Update file as processed
        file_record = self.db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
        if file_record:
            file_record.is_processed = True
            file_record.processed_at = datetime.now()
            file_record.records_processed = len(records)
            self.db.commit()
        
        return len(records)

    def load_ips_mobile(self, file_path: str, file_id: int, convenience_fee: float = 0.45) -> int:
        """Load IPS data to staging table"""
        
        # Determine if Excel or CSV
        if file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)

        # --- Check for a sum or total at the bottom of the report and remove it ---
        df = df[df['Received Date Time'].notna()]
        df['convenience_fee'] = convenience_fee
        
        # --- Normalize column names ---
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("/","").str.replace('\n','').str.replace('.','')
        df.rename(columns=({'Amount ($)':'amount', '$ Paid':'paid', '$0.01':'pennies', '$0.05':'nickels', '$0.10':'dimes', '$0.25':'quarters', '$1.00':'dollars'}), inplace=True)
        
        # --- Add metadata columns ---
        df["source_file_id"] = file_id
        df["processed_to_final"] = False

        # --- Convert datetimes where possible ---
        for col in df.columns:
            if "date" in col:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception:
                    pass

        # --- Handle integer columns - replace NaN with None ---
        int_columns = ['space_name', 'prid']
        
        for col in int_columns:
            if col in df.columns:
                # Convert to nullable integer type or replace NaN with None
                df[col] = df[col].replace({pd.NA: None, np.nan: None})
                # Convert to int where not None
                df.loc[df[col].notna(), col] = df[col].loc[df[col].notna()].astype(int)
                
        # --- Convert pandas NaN to None for SQL ---
        df = df.replace({pd.NA: None, np.nan: None, pd.NaT: None})
        
        # --- Convert to list of dictionaries ---
        records = df.to_dict(orient="records")
        
        # --- Bulk insert using SQLAlchemy ---
        self.db.execute(insert(IPSMobileStaging), records)
        self.db.commit()

        # Update file as processed
        file_record = self.db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
        if file_record:
            file_record.is_processed = True
            file_record.processed_at = datetime.now()
            file_record.records_processed = len(records)
            self.db.commit()
            
        return len(records)

    def load_ips_cash(self, file_path: str, file_id: int) -> int:
        """Load IPS data to staging table"""
        
        # Determine if Excel or CSV
        if file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path, dtype={'Terminal':'str'})
        else:
            df = pd.read_csv(file_path, dtype={'Terminal':'str'})

        # --- Check for a sum or total at the bottom of the report and remove it ---
        df = df[df['Collection Date'].notna()]
        
        # --- Normalize column names ---
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("/","").str.replace('\n','').str.replace('.','')
        df.rename(columns=({'Amount ($)':'amount', '$ Paid':'paid', '$0.01':'pennies', '$0.05':'nickels', '$0.10':'dimes', '$0.25':'quarters', '$1.00':'dollars'}), inplace=True)
        
        # --- Add metadata columns ---
        df["source_file_id"] = file_id
        df["processed_to_final"] = False

        # --- Convert datetimes where possible ---
        for col in df.columns:
            if "date" in col:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception:
                    pass
                    
        # --- Handle integer columns - replace NaN with None ---
        int_columns = ['coin_total', 'unrecognized_coins', 'coin_reversal_count']
        
        for col in int_columns:
            if col in df.columns:
                # Convert to nullable integer type or replace NaN with None
                df[col] = df[col].replace({pd.NA: None, np.nan: None})
                # Convert to int where not None
                df.loc[df[col].notna(), col] = df[col].loc[df[col].notna()].astype(int)
                
        # --- Convert pandas NaN to None for SQL ---
        df = df.replace({pd.NA: None, np.nan: None, pd.NaT: None})
        
        # --- Convert to list of dictionaries ---
        records = df.to_dict(orient="records")
        
        # --- Bulk insert using SQLAlchemy ---
        self.db.execute(insert(IPSCashStaging), records)
        self.db.commit()

        # --- Update file as processed ---
        file_record = self.db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
        if file_record:
            file_record.is_processed = True
            file_record.processed_at = datetime.now()
            file_record.records_processed = len(records)
            self.db.commit()
        
        return len(records)