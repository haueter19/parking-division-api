"""
ETL Processing Functions for Data Lake
Transforms data from staging tables to normalized transactions table
"""

from datetime import datetime
from typing import Optional, Dict, List, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
import pandas as pd
from app.models.database import (
    Transaction, DataSourceType, LocationType, PaymentType,
    WindcaveStaging, PaymentsInsiderStaging, IPSCreditCardStaging,
    IPSMobileStaging, IPSCashStaging, SQLCashStaging,
    ETLProcessingLog, UploadedFile
)


class ETLProcessor:
    """Main ETL processor for transforming staging data to final transactions"""
    
    def __init__(self, db: Session):
        self.db = db
        self.org_code_cache = {}  # Cache for terminal_id -> org_code lookups
        
    def get_org_code(self, terminal_id: str) -> Optional[str]:
        """
        Get org code for a terminal ID
        This should connect to your existing terminal/org_code tables
        """
        if terminal_id in self.org_code_cache:
            return self.org_code_cache[terminal_id]
        
        # TODO: Replace with actual query to your terminal/org_code tables
        # Example:
        # result = self.db.query(TerminalOrgCode).filter(
        #     TerminalOrgCode.terminal_id == terminal_id
        # ).first()
        # org_code = result.org_code if result else None
        
        org_code = None  # Placeholder
        self.org_code_cache[terminal_id] = org_code
        return org_code
    
    def determine_location_type(self, location_name: str, terminal_id: str = None) -> LocationType:
        """Determine location type from location name or terminal ID"""
        location_lower = (location_name or "").lower()
        
        if "garage" in location_lower or "parking structure" in location_lower:
            return LocationType.GARAGE
        elif "lot" in location_lower or "surface" in location_lower:
            return LocationType.LOT
        elif "meter" in location_lower or terminal_id and terminal_id.startswith("M"):
            return LocationType.METER
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
        
        try:
            # Query unprocessed records
            query = self.db.query(WindcaveStaging).filter(
                WindcaveStaging.processed_to_final == False
            )
            if file_id:
                query = query.filter(WindcaveStaging.source_file_id == file_id)
            
            records = query.all()
            created_count = 0
            failed_count = 0
            
            for record in records:
                try:
                    transaction = Transaction(
                        transaction_date=record.transaction_date,
                        transaction_amount=record.amount,
                        settle_date=record.settlement_date,
                        settle_amount=record.settlement_amount,
                        source=DataSourceType.WINDCAVE_CC,
                        location_type=self.determine_location_type("", record.terminal_id),
                        location_name=None,  # Windcave might not have location names
                        device_terminal_id=record.terminal_id,
                        payment_type=self.map_payment_type(record.card_type),
                        reference_number=record.reference,
                        org_code=self.get_org_code(record.terminal_id),
                        staging_table="windcave_staging",
                        staging_record_id=record.id
                    )
                    
                    self.db.add(transaction)
                    self.db.flush()
                    
                    # Update staging record
                    record.processed_to_final = True
                    record.transaction_id = transaction.id
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
        log_entry = self._start_log("payments_insider_staging", file_id)
        
        try:
            # First, match sales and payments reports by reference numbers
            self._match_pi_reports()
            
            # Query matched records that haven't been processed
            query = self.db.query(PaymentsInsiderStaging).filter(
                and_(
                    PaymentsInsiderStaging.processed_to_final == False,
                    PaymentsInsiderStaging.matching_report_id != None
                )
            )
            if file_id:
                query = query.filter(PaymentsInsiderStaging.source_file_id == file_id)
            
            sales_records = query.filter(PaymentsInsiderStaging.report_type == "sales").all()
            created_count = 0
            failed_count = 0
            
            for sales_record in sales_records:
                try:
                    # Get matching payment record
                    payment_record = self.db.query(PaymentsInsiderStaging).filter(
                        PaymentsInsiderStaging.id == sales_record.matching_report_id
                    ).first()
                    
                    if not payment_record:
                        continue
                    
                    transaction = Transaction(
                        transaction_date=sales_record.transaction_date,
                        transaction_amount=sales_record.amount,
                        settle_date=payment_record.payment_date,
                        settle_amount=payment_record.amount,
                        source=DataSourceType.PAYMENTS_INSIDER_CC,
                        location_type=self.determine_location_type(sales_record.location),
                        location_name=sales_record.location,
                        device_terminal_id=sales_record.terminal_id,
                        payment_type=self.map_payment_type(sales_record.card_type),
                        reference_number=sales_record.reference_number,
                        org_code=self.get_org_code(sales_record.terminal_id),
                        staging_table="payments_insider_staging",
                        staging_record_id=sales_record.id
                    )
                    
                    self.db.add(transaction)
                    self.db.flush()
                    
                    # Update both staging records
                    sales_record.processed_to_final = True
                    sales_record.transaction_id = transaction.id
                    payment_record.processed_to_final = True
                    payment_record.transaction_id = transaction.id
                    created_count += 1
                    
                except Exception as e:
                    failed_count += 1
                    print(f"Error processing PI record {sales_record.id}: {e}")
            
            self.db.commit()
            self._complete_log(log_entry, len(sales_records), created_count, 0, failed_count)
            
            return {
                "success": True,
                "records_processed": len(sales_records),
                "records_created": created_count,
                "records_failed": failed_count
            }
            
        except Exception as e:
            self.db.rollback()
            self._fail_log(log_entry, str(e))
            raise
    
    def _match_pi_reports(self):
        """Match Payments Insider sales and payments reports"""
        # Get unmatched sales reports
        unmatched_sales = self.db.query(PaymentsInsiderStaging).filter(
            and_(
                PaymentsInsiderStaging.report_type == "sales",
                PaymentsInsiderStaging.matching_report_id == None
            )
        ).all()
        
        for sales in unmatched_sales:
            # Try to find matching payment by reference number and amount
            payment = self.db.query(PaymentsInsiderStaging).filter(
                and_(
                    PaymentsInsiderStaging.report_type == "payments",
                    PaymentsInsiderStaging.reference_number == sales.reference_number,
                    PaymentsInsiderStaging.amount == sales.amount,
                    PaymentsInsiderStaging.matching_report_id == None
                )
            ).first()
            
            if payment:
                sales.matching_report_id = payment.id
                payment.matching_report_id = sales.id
        
        self.db.commit()
    
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
                        transaction_amount=record.amount,
                        settle_date=record.collection_date,  # Same as transaction date for cash
                        settle_amount=record.amount,
                        source=DataSourceType.IPS_CASH,
                        location_type=LocationType.METER,  # IPS Cash is always meters
                        location_name=record.location,
                        device_terminal_id=record.meter_id,
                        payment_type=PaymentType.CASH,
                        reference_number=record.collector_id,
                        org_code=self.get_org_code(record.meter_id),
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
        log_entry.end_time = datetime.utcnow()
        log_entry.records_processed = processed
        log_entry.records_created = created
        log_entry.records_updated = updated
        log_entry.records_failed = failed
        log_entry.status = "completed"
        self.db.commit()
    
    def _fail_log(self, log_entry: ETLProcessingLog, error_message: str):
        """Mark a processing log entry as failed"""
        log_entry.end_time = datetime.utcnow()
        log_entry.status = "failed"
        log_entry.error_message = error_message
        self.db.commit()


class DataLoader:
    """Load data from files to staging tables"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def load_windcave_csv(self, file_path: str, file_id: int) -> int:
        """Load Windcave CSV to staging table"""
        df = pd.read_csv(file_path)
        
        # Map CSV columns to staging table columns
        # Adjust these mappings based on actual Windcave CSV structure
        records = []
        for _, row in df.iterrows():
            record = WindcaveStaging(
                source_file_id=file_id,
                transaction_date=pd.to_datetime(row.get('TransactionDate')),
                card_number_masked=row.get('CardNumber'),
                amount=row.get('Amount'),
                settlement_date=pd.to_datetime(row.get('SettlementDate')),
                settlement_amount=row.get('SettlementAmount'),
                terminal_id=row.get('TerminalID'),
                reference=row.get('Reference'),
                card_type=row.get('CardType'),
                merchant_id=row.get('MerchantID')
            )
            records.append(record)
        
        self.db.bulk_save_objects(records)
        self.db.commit()
        
        # Update file as processed
        file_record = self.db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
        if file_record:
            file_record.is_processed = True
            file_record.processed_at = datetime.utcnow()
            file_record.records_processed = len(records)
            self.db.commit()
        
        return len(records)
    
    def load_payments_insider(self, file_path: str, file_id: int, report_type: str) -> int:
        """Load Payments Insider report to staging table"""
        # Determine if Excel or CSV
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
        
        records = []
        for _, row in df.iterrows():
            record = PaymentsInsiderStaging(
                source_file_id=file_id,
                report_type=report_type,  # 'sales' or 'payments'
                transaction_date=pd.to_datetime(row.get('TransactionDate')) if report_type == 'sales' else None,
                payment_date=pd.to_datetime(row.get('PaymentDate')) if report_type == 'payments' else None,
                amount=row.get('Amount'),
                card_type=row.get('CardType'),
                terminal_id=row.get('TerminalID'),
                location=row.get('Location'),
                reference_number=row.get('ReferenceNumber'),
                batch_number=row.get('BatchNumber')
            )
            records.append(record)
        
        self.db.bulk_save_objects(records)
        self.db.commit()
        
        return len(records)
