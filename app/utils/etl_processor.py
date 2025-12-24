"""
ETL Processing Functions for Data Lake
Transforms data from staging tables to normalized transactions table
"""

from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Callable
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from sqlalchemy import Table, MetaData, select, insert, text
from sqlalchemy.ext.hybrid import hybrid_property
import numpy as np
import pandas as pd
from app.utils import etl_cache
from app.models.database import (
    Transaction, DataSourceType, LocationType, PaymentType,
    WindcaveStaging, PaymentsInsiderPaymentsStaging, PaymentsInsiderSalesStaging, 
    IPSCreditCardStaging, IPSMobileStaging, IPSCashStaging, SQLCashStaging,
    ETLProcessingLog, UploadedFile
)
#from db_manager import ConnectionManager
#cnxn = ConnectionManager()

class ETLProcessor:
    """Main ETL processor for transforming staging data to final transactions"""
    
    def __init__(self, db: Session, traffic_db: Optional[Session] = None, 
                 org_code_cache: Optional[pd.DataFrame] = None,
                 location_from_charge_code: Optional[Dict] = None,
                 progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None):
        """
        ETLProcessor can accept two session objects:
        - db: primary application DB session (PUReporting)
        - traffic_db: optional session bound to the Traffic engine

        If `traffic_db` is provided, `get_org_code` will query the Traffic
        database tables (`PU_PARCS_UCD` and `PU_REVENUE_CREDITCARDTERMINALS`) and
        return the matching org_code. If not provided, the method falls back to
        the default behavior (placeholder/None).
        
        Optional pre-initialized caches can be passed in:
        - org_code_cache: DataFrame from cache with org code lookup data
        - location_from_charge_code: Dict mapping charge codes to location names
        """
        self.db = db
        self.traffic_db = traffic_db
        self.BATCH_SIZE = 500
        # initialize lookup dicts to empty dicts so .get() is always safe
        self.charge_code_from_housing_id = {}
        self.charge_code_from_terminal_id = {}
        self.garage_from_station = {}
        self.org_code_from_area = {
                "SSCO": 82007,
                "OC": 82002,
                "CSN": 82001,
                "SLS": 82162,
                "BLAIR": 82055,
                "WINGRA": 82057,
                "BUCKEYE": 82224,
                "EVERGREEN": 82225,
                "Spares": 82935,
                "LAKE": 82005,
                "FRANCES": 82005,
                "TEST": 82935,
                "WILSON": 82004
            }
        
        # Use pre-initialized caches if provided, otherwise will initialize on-demand
        self.org_code_cache = org_code_cache
        self.location_from_charge_code = location_from_charge_code

        # If org_code_cache was provided, derive the dict lookups used throughout the processor
        if self.org_code_cache is not None:
            print('Using provided org_code_cache for ETLProcessor')
            #try:
                #self.charge_code_from_housing_id = {a: b for a, b in zip(self.org_code_cache['Device_ID'], self.org_code_cache['ChargeCode']) if a is not None}
            #except Exception:
                #self.charge_code_from_housing_id = {}
            self.charge_code_from_housing_id = etl_cache.get_charge_code_from_housing_id()
            #try:
                #self.charge_code_from_terminal_id = {a: b for a, b in zip(self.org_code_cache['TerminalID'], self.org_code_cache['ChargeCode']) if a is not None}
            #except Exception:
                #self.charge_code_from_terminal_id = {}
            self.charge_code_from_terminal_id = etl_cache.get_charge_code_from_terminal_id()
            # Fill location_from_charge_code from org_code_cache if not provided explicitly
            self.location_from_charge_code = etl_cache.get_location_from_charge_code()
            self.garage_from_station = etl_cache.get_garage_from_station()
            if not self.location_from_charge_code:
                print('location_from_charge_code not provided')
                try:
                    if 'Location' in self.org_code_cache.columns:
                        self.location_from_charge_code = {a: b for a, b in zip(self.org_code_cache['ChargeCode'], self.org_code_cache['Location']) if a is not None}
                except Exception:
                    self.location_from_charge_code = {}

        # If garage map missing, attempt to read from global etl_cache if available
        if not self.garage_from_station:
            try:
                from app.utils import etl_cache as _etl_cache_mod
                self.garage_from_station = _etl_cache_mod.get_garage_from_station() or {}
            except Exception:
                # keep empty dict as fallback
                self.garage_from_station = {}
        # Optional callback to report progress updates. Callable should accept a dict payload.
        self.progress_callback = progress_callback

    def _report_progress(self, payload: Dict[str, Any]):
        """Invoke progress callback if provided. Swallow any exceptions from callback."""
        if not self.progress_callback:
            return
        try:
            self.progress_callback(payload)
        except Exception:
            # Don't let progress reporting break processing
            pass
        
    def get_org_code(self) -> Optional[pd.DataFrame]:
        """
        Get org code for a terminal ID
        This should connect to your existing terminal/org_code tables
        
        If org_code_cache is already initialized (from startup cache), returns immediately.
        Otherwise, queries the Traffic DB.
        """
        
        # If caches are already initialized (from app startup), return immediately
        if self.org_code_cache is not None and self.location_from_charge_code is not None:
            return self.org_code_cache

        # If a Traffic DB session was provided, query the Traffic DB tables.
        # We'll use SQLAlchemy Core (select + union) with table reflection so
        # we don't need ORM model classes for those legacy tables.

        try:
            org_lookup_tbl = pd.read_sql("""
                with ucds as (
                    SELECT
                        'EMV Reader' source, Device_ID, a.TerminalID, b.ChargeCode, a.Facility_Name_Abr, a.Facility_Name_Full, a.DateRemoved
                    FROM data_admin8.PU_PARCS_EQUIP a
                    INNER JOIN data_admin8.PU_PARCS_UCD b On (b.HousingID=a.Device_ID)
                    WHERE 
                        a.TerminalID IS NOT NULL 
                        AND a.DateREmoved IS NULL
                        AND b.ChargeCode IS NOT NULL
                ), cc_terminals as (
                    SELECT 'CC Terminal' source, DeviceID, TerminalID, DateAssigned, DateRemoved, ChargeCode FROM [Traffic].[data_admin8].PU_CC_TERMINAL_HISTORY WHERE ChargeCode IS NOT NULL --AND DateRemoved IS NULL
                )
                SELECT 
                    source, ucds.Device_ID, CONCAT('0010050008016090',CAST(ucds.TerminalID As varchar)) TerminalID, NULL DateAssigned, DateRemoved, ucds.ChargeCode,
                    CASE
                        WHEN Device_ID = 'E164' THEN 'Capitol Square North'
                        WHEN Device_ID LIKE '_1_' THEN 'Overture Center'
                        WHEN Device_ID LIKE '_2_' THEN 'State Street Capitol'
                        WHEN Device_ID LIKE '_4_' THEN 'Lake/Frances'
                        WHEN Device_ID LIKE '_5_' THEN 'Lake/Frances'
                        WHEN Device_ID LIKE '_6_' THEN 'Capitol Square North'
                        WHEN Device_ID LIKE '_7_' THEN 'Wilson Street'
                        WHEN Device_ID LIKE '_8_' THEN 'Livingston'
                        WHEN Device_ID LIKE '_9_' THEN 'Shop'
                        ELSE NULL
                    END As Location
                FROM ucds 
                UNION
                SELECT
                    source, NULL, cc_terminals.TerminalID, COALESCE(cc_terminals.DateAssigned, '1900-01-01') DateAssigned, COALESCE(cc_terminals.DateRemoved, '2050-01-01') DateRemoved, cc_terminals.ChargeCode,
                    CASE
                        WHEN cc_terminals.ChargeCode IN (82001, 82044) THEN 'Capitol Square North'
                        WHEN cc_terminals.ChargeCode IN (82002, 82045) THEN 'Overture Center'
                        WHEN cc_terminals.ChargeCode IN (82004, 82047) THEN 'Wilson Street'
                        WHEN cc_terminals.ChargeCode IN (82005, 82048) THEN 'Lake/Frances'
                        WHEN cc_terminals.ChargeCode IN (82007, 82050) THEN 'State Street Capitol'
                        WHEN cc_terminals.ChargeCode IN (82162, 82164) THEN 'Livingston'
                        WHEN cc_terminals.ChargeCode IN (82172) THEN 'Shop'
                        ELSE NULL
                    END As Location
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
            charge_code_from_housing_id = {a:b for a,b in zip(org_lookup_tbl['Device_ID'], org_lookup_tbl['ChargeCode']) if a != None}
            charge_code_from_terminal_id = {a:b for a,b in zip(org_lookup_tbl['TerminalID'], org_lookup_tbl['ChargeCode']) if a != None}
            location_from_charge_code = {a:b for a,b in zip(org_lookup_tbl['ChargeCode'], org_lookup_tbl['Location']) if a != None}
            garage_from_station = {a:b for a,b in zip(garage_and_station_records['TxnT2StationAdddress'], garage_and_station_records['Garage']) if a != None}
            location_from_charge_code = {a:b for a,b in zip(org_lookup_tbl['ChargeCode'], org_lookup_tbl['Location']) if a != None}
            #location_from_charge_code[82044] = 'Capitol Square North'
            #location_from_charge_code[82045] = 'Overture Center'
            #location_from_charge_code[82047] = 'Wilson Street'
            #location_from_charge_code[82048] = 'Lake/Frances'
            #location_from_charge_code[82050] = 'State Street Capitol'
            #location_from_charge_code[82164] = 'Livingston'
            #location_from_charge_code[82172] = 'Over/Short/Helpline'
            location_from_charge_code[82055] = 'Blair Lot'
            location_from_charge_code[82057] = 'Wingra Lot'
            location_from_charge_code[82074] = 'Multi-Space Meters'
            location_from_charge_code[82088] = 'Single Space Meters'
            location_from_charge_code[82224] = 'Buckeye Lot'
            location_from_charge_code[82225] = 'Evergreen Lot'
            location_from_charge_code[82935] = 'Meter Over/Short'

            # Additional hardcoded mappings -- remove after updating Traffic DB
            charge_code_from_terminal_id['0010050008031494050786'] = 82088
            charge_code_from_terminal_id['0010050008031494050908'] = 82074

            # Save dicts to the class
            self.charge_code_from_housing_id = charge_code_from_housing_id
            self.charge_code_from_terminal_id = charge_code_from_terminal_id
            self.location_from_charge_code = location_from_charge_code
            self.garage_from_station = garage_from_station

        except Exception as e:
            # On error, log/print and fall back to empty lookups to avoid NoneType .get errors
            print(f"Error querying Traffic DB for org_codes: {e}")
            # Ensure lookup dicts exist as empty dicts
            self.charge_code_from_housing_id = {}
            self.charge_code_from_terminal_id = {}
            self.location_from_charge_code = {}
            self.garage_from_station = {}
            try:
                self.org_code_cache = org_lookup_tbl
            except NameError:
                self.org_code_cache = None
            return self.org_code_cache

        self.org_code_cache = org_lookup_tbl
        return org_lookup_tbl
    
    def determine_location_type(self, terminal_id: str = None) -> LocationType:
        """Determine location type from location name or terminal ID"""
        #location_lower = (location_name or "").lower()
        
        if terminal_id == '0010050008031494050786':
            return LocationType.SINGLE_SPACE_METER
        elif terminal_id == '0010050008031494050908':
            return LocationType.MULTI_SPACE_METER
        elif terminal_id in self.charge_code_from_housing_id.keys():
            return LocationType.GARAGE
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
        elif "mastercard" in card_lower or "master" in card_lower or 'mc' in card_lower:
            return PaymentType.MASTERCARD
        elif "amex" in card_lower or "american express" in card_lower:
            return PaymentType.AMEX
        elif "discover" in card_lower:
            return PaymentType.DISCOVER
        elif 'park smarter' in card_lower:
            return PaymentType.PARK_SMARTER
        elif 'text to pay' in card_lower:
            return PaymentType.TEXT_TO_PAY
        else:
            return PaymentType.OTHER
    
        
    def process_windcave(self, file_id: int) -> Dict[str, Any]:
        """Process Windcave staging records to final transactions"""
        log = self._start_log("windcave_staging", file_id)
        
        try:
            # Query unprocessed records
            self.db.execute(
                text("""
                    INSERT INTO app.fact_transaction (
                        transaction_date,
                        transaction_amount,
                        settle_date,
                        settle_amount,
                        staging_table,
                        source_file_id,
                        staging_record_id,
                        payment_method_id,
                        device_id,
                        settlement_system_id,
                        location_id,
                        program_id,
                        charge_code_id,
                        reference_number
                    )
                    SELECT
                        s.time,
                        s.amount,
                        s.settlement_date,
                        s.amount,
                        'windcave_staging',
                        s.source_file_id,
                        s.id,
                        pm.payment_method_id,
                        d.device_id,
                        2, -- settlement_system_id hardcoded to 2 for Windcave transactions
                        da.location_id,
                        1, -- program_id hardcoded to 1 for Windcave transactions
                        cc.charge_code_id,
                        s.dpstxnref
                    FROM app.windcave_staging s
                    INNER JOIN app.dim_device d ON (d.device_terminal_id = s.device_id)
                    INNER JOIN app.fact_device_assignment da ON (da.device_id = d.device_id AND s.time >= da.assign_date AND s.time < COALESCE(da.end_date, '9999-12-31'))
                    INNER JOIN app.dim_payment_method pm ON (pm.payment_method_brand = s.card_type)
                    INNER JOIN app.dim_charge_code cc ON (cc.location_id = da.location_id AND cc.program_type_id = 1)
                    WHERE 
                        s.processed_to_final = 0
                        AND s.voided = 0
                        AND s.source_file_id = :file_id;
                     """,
                     {"file_id": file_id}
                     )
            )

            # Query for records that won't process
            self.db.exceucte(
                text("""
                    INSERT INTO app.fact_transaction_reject (
                        staging_table,
                        staging_record_id,
                        source_file_id,
                        reject_reason_code,
                        reject_detail_desc,
                        rejected_at,
                        source_device_terminal_id,
                        payment_method_id,
                        device_id,
                        settlement_system_id,
                        location_id,
                        charge_code_id
                    )
                    SELECT
                        'windcave_staging',
                        s.id,
                        s.source_file_id,
                        'NO_DEVICE_ASSIGNMENT',
                        'No active device assignment for transaction time',
                        SYSUTCDATETIME(),
                        s.device_id,
                        pm.payment_method_id,
                        d.device_id,
                        2 As settlement_system_id,
                        da.location_id,
                        cc.charge_code_id
                    FROM app.windcave_staging s
                    LEFT JOIN app.dim_device d ON (d.device_terminal_id = s.device_id)
                    LEFT JOIN app.fact_device_assignment da ON (da.device_id = d.device_id AND s.time >= da.assign_date AND s.time <COALESCE(da.end_date, '9999-12-31'))
                    LEFT JOIN app.dim_payment_method pm On (s.card_type=pm.payment_method_brand)
                    LEFT JOIN app.dim_charge_code cc On (da.location_id=cc.location_id AND 1=cc.program_type_id)
                    WHERE 
                        da.device_id IS NULL
                        AND s.processed_to_final = 0
                        AND s.source_file_id = :file_id;
                    """), 
                    {"file_id": file_id}
            )

            # Mark all processed staging records as processed
            self.db.execute(
                text("""
                    UPDATE s
                    SET
                        processed_to_final = 1,
                        loaded_at = SYSUTCDATETIME()
                    FROM app.windcave_staging s
                    WHERE s.id IN (
                        SELECT staging_record_id
                        FROM app.fact_transaction
                        WHERE 
                            staging_table = 'windcave_staging'
                            AND source_file_id = :file_id
                    );
                    """, {"file_id": file_id}
                )
            )
            
            total_records = self.db.execute(text("SELECT count(*) FROM app.windcave_staging WHERE source_file_id = :file_id"), {"file_id": file_id}).scalar()
            created_count = self.db.execute(text("SELECT count(*) FROM app.fact_transaction WHERE staging_table = 'windcave_staging' AND source_file_id = :file_id"), {"file_id": file_id}).scalar()

            self.db.commit()
            self._complete_log(log, processed=total_records, created=created_count, updated=0, failed=total_records - created_count)
            
            return {
                "success": True,
                "records_processed": total_records,
                "records_created": created_count,
                "records_failed": total_records - created_count
            }
        except Exception as e:
            self.db.rollback()
            self._fail_log(log, str(e))
            raise



    def process_windcave_old(self, file_id: Optional[int] = None) -> Dict[str, Any]:
        """Process Windcave staging records to final transactions"""
        log_entry = self._start_log("windcave_staging", file_id)
        
        #if self.org_code_cache is None:
        #    self.get_org_code()
            
        try:
            # Query unprocessed records
            query = self.db.query(WindcaveStaging).filter(
                WindcaveStaging.processed_to_final == False,
                WindcaveStaging.voided == 0
            )
            
            if file_id:
                query = query.filter(WindcaveStaging.source_file_id == file_id)
            
            # Query to get the records
            records = query.all()
            total_records = len(records)

            # For any device_id with len > 3, use the first part of txnref
            for record in records:
                if len(record.device_id) > 3:
                    record.device_id = record.txnref.split('-')[0]

            for record in records:
                if record.device_id[0].lower() == 'a':
                    record.location_sub_area = 'Exit'
                if record.device_id[0].lower() == 'e':
                    record.location_sub_area = 'Entry'
                if record.device_id[0].lower() == 'h':
                    record.location_sub_area = 'Cashiered'
                if record.device_id[0].lower() == 'k':
                    record.location_sub_area = 'POF'
                if record.device_id[0].lower() not in ['a', 'e', 'h', 'k']:
                    record.location_sub_area = None

            created_count = 0
            failed_count = 0

            # Track record-transaction pairs for the current batch
            batch_pairs = []
            
            # Create transaction records
            for idx, record in enumerate(records):
                try:
                    transaction = Transaction(
                        transaction_date=record.time,
                        transaction_amount=record.amount,
                        settle_date=record.settlement_date,
                        settle_amount=record.amount,
                        source=DataSourceType.WINDCAVE,
                        location_type=LocationType.GARAGE,
                        location_name=self.garage_from_station.get(record.device_id), 
                        location_sub_area=record.location_sub_area,
                        device_terminal_id=record.device_id, 
                        payment_type=self.map_payment_type(record.card_type),
                        reference_number=record.dpstxnref, # Do I need reference number? Is this the best choice?
                        org_code=self.charge_code_from_housing_id.get(record.device_id), # or self.charge_code_from_housing_id.get(record['txn']), # Try using device_id, fail to txn
                        staging_table="windcave_staging",
                        staging_record_id=record.id
                    )
                    
                    self.db.add(transaction)
                    
                    # Mark as processed
                    record.processed_to_final = True
                    
                    # Keep track of this pair for later
                    batch_pairs.append((record, transaction))
                    
                    # Increment created count
                    created_count += 1
                
                    # Batch commit
                    if (idx +1) % self.BATCH_SIZE == 0:
                        # Flush to get all transaction IDs for this batch
                        self.db.flush()

                        # Now link all staging records to their transactions
                        for staging_record, txn in batch_pairs:
                            staging_record.transaction_id = txn.id
                    
                        # Clear the batch tracker
                        batch_pairs = []
                        
                        # Report progress to etl_processing_table
                        self._update_log(log_entry, idx+1, created_count, 0, failed_count)

                        # Commit batch
                        self.db.commit()

                        # Report progress after each batch
                        self._report_progress({
                            "source": "windcave",
                            "processed": idx + 1,
                            "total": len(records),
                            "created": created_count,
                            "failed": failed_count
                        })
                        print(f"Committed batch: {idx + 1} of {len(records)} records processed")

                except Exception as e:
                    failed_count += 1
                    print(f"Error processing Windcave record {record.id}: {e}")
            
            # Final commit for any remaining records (not a full batch)
            if batch_pairs:  # If there are unpaired records
                self.db.flush()
            
                # Link remaining records to transactions
                for staging_record, txn in batch_pairs:
                    staging_record.transaction_id = txn.id
                
                self._update_log(log_entry, len(records), created_count, 0, failed_count)
                self.db.commit()
                print(f"Committed final batch: {len(records)} of {total_records} records processed")
            
            # Mark as complete
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
    
    def process_payments_insider(self, file_id: int) -> Dict[str, Any]:
        """
        Process Payments Insider staging records to final transactions
        Note: PI requires matching Sales and Payments reports
        Handles both PAYMENTS_INSIDER_SALES and PAYMENTS_INSIDER_PAYMENTS data types
        """
        log = self._start_log("windcave_staging", file_id)


    def _process_payments_insider(self, file_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Process Payments Insider staging records to final transactions
        Note: PI requires matching Sales and Payments reports
        Handles both PAYMENTS_INSIDER_SALES and PAYMENTS_INSIDER_PAYMENTS data types
        """
        log_entry = self._start_log("payments_insider_sales_staging", file_id)

        try:
            # Determine the data source type from the file record to know if we're processing Sales or Payments
            is_sales = True  # Default to Sales
            if file_id:
                file_record = self.db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
                if file_record and file_record.data_source_type == DataSourceType.PAYMENTS_INSIDER_PAYMENTS:
                    is_sales = False
            
            # Query sales LEFT JOIN payments (match SQL behavior).
            # Select sales rows (even when there's no matching payment) where
            # the sale hasn't been processed and is not voided (for Sales only).
            filters = [
                PaymentsInsiderSalesStaging.processed_to_final == False,
                PaymentsInsiderSalesStaging.mid == '8016090345'
            ]
            
            # Only filter void_ind if processing Sales data (Payments data doesn't have this field)
            if is_sales:
                filters.append(PaymentsInsiderSalesStaging.void_ind == 'N')
            
            query = self.db.query(
                PaymentsInsiderSalesStaging,
                PaymentsInsiderPaymentsStaging
            ).outerjoin(
                PaymentsInsiderPaymentsStaging,
                and_(
                    PaymentsInsiderSalesStaging.card_number == PaymentsInsiderPaymentsStaging.card_number,
                    PaymentsInsiderSalesStaging.authorization_code == PaymentsInsiderPaymentsStaging.authorization_code
                )
            ).filter(and_(*filters))
            
            if file_id:
                query = query.filter(PaymentsInsiderSalesStaging.source_file_id == file_id)
            
            records = query.all()
            created_count = 0
            failed_count = 0
            
            print( f"Processing {len(records)} Payments Insider records...")
            for idx, (sales_record, payment_record) in enumerate(records):
                # Look up charge code from terminal ID and transaction date
                try:
                    charge_code = int(self.org_code_cache[(self.org_code_cache['TerminalID']==sales_record.terminal_id) &
                                    (self.org_code_cache['DateAssigned'] <= sales_record.transaction_datetime) &
                                    (self.org_code_cache['DateRemoved'] > sales_record.transaction_datetime)]['ChargeCode'].iloc[0])
                except Exception:
                    print(idx, sales_record.terminal_id, sales_record.transaction_datetime)
                    charge_code = None
                    raise
                
                try:
                    transaction = Transaction(
                        transaction_date=sales_record.transaction_datetime,
                        transaction_amount=sales_record.transaction_amount,
                        settle_date=payment_record.payment_date if payment_record else None,
                        settle_amount=payment_record.transaction_amount if payment_record else None,
                        source=DataSourceType.PAYMENTS_INSIDER_SALES,
                        location_type=LocationType.GARAGE,
                        location_name=self.location_from_charge_code.get(charge_code),
                        location_sub_area=self.location_from_charge_code.get(charge_code),
                        device_terminal_id=sales_record.terminal_id,
                        payment_type=self.map_payment_type(sales_record.card_brand),
                        reference_number=payment_record.card_number.replace('*','')+payment_record.authorization_code if payment_record else sales_record.card_number.replace('*','')+sales_record.authorization_code, # probably use invoice if including IPS
                        org_code=charge_code,
                        staging_table="payments_insider_sales_staging",
                        staging_record_id=sales_record.id
                    )
                    
                    self.db.add(transaction)
                    
                    # Mark as processed
                    sales_record.processed_to_final = True
                    if payment_record:
                        payment_record.processed_to_final = True
                    
                    created_count += 1
                    
                    # Batch commit every BATCH_SIZE records
                    if (idx + 1) % self.BATCH_SIZE == 0:
                        self.db.flush()
                        self.db.commit()
                        # Report progress after each PI batch
                        self._report_progress({
                            "source": "payments_insider",
                            "processed": idx + 1,
                            "total": len(records),
                            "created": created_count,
                            "failed": failed_count
                        })
                        print(f"Committed batch: {idx + 1}/{len(records)} records processed")
                    
                except Exception as e:
                    failed_count += 1
                    print(f"Error processing PI record {sales_record.id}: {e}")
            
            self.db.flush()
            self.db.commit()
            print(f"Final commit: {created_count} total records processed")
            
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
    
    
    def process_ips_cc(self, file_id: Optional[int] = None) -> Dict[str, Any]:
        """Process IPS credit card staging records to final transactions"""
        log_entry = self._start_log("ips_cc_staging", file_id)
        self.BATCH_SIZE = 500

        try:
            query = self.db.query(IPSCreditCardStaging).filter(
                IPSCreditCardStaging.processed_to_final == False
            )
            
            if file_id:
                query = query.filter(IPSCreditCardStaging.source_file_id == file_id)

            records = query.all()
            created_count = 0
            failed_count = 0

            for idx, record in enumerate(records):

                if record.area.upper() in self.org_code_from_area.keys():
                    org_code = self.org_code_from_area.get(record.area.upper())
                elif record.pole in ['60010', '60011', '60002', '60005', '60006', '60007', '9501', '60001', '60003', '60004', 'Campus MP']:
                    org_code = 82074
                else:
                    org_code = 82088
    
                try:
                    # For cash, settle date = transaction date
                    transaction = Transaction(
                        transaction_date=record.transaction_date_time,
                        transaction_amount=record.amount,
                        settle_date=record.settlement_date_time,  # Same as transaction date for cash
                        settle_amount=record.amount,
                        source=DataSourceType.IPS_CC,
                        # Need a way to look up the meter type from the pole or terminal
                        location_type=LocationType.MULTI_SPACE_METER if record.pole in ['60010', '60011', '60002', '60005', '60006', '60007', '9501', '60001', '60003', '60004', 'Campus MP'] else LocationType.SINGLE_SPACE_METER,
                        location_name=record.pole,
                        location_sub_area=record.sub_area,
                        device_terminal_id=record.terminal,
                        payment_type=self.map_payment_type(record.card_type),
                        reference_number=record.transaction_reference,
                        org_code=org_code, # Need a way to look up the meter type from the pole or terminal
                        staging_table="ips_cc_staging",
                        staging_record_id=record.id
                    )
                
                    self.db.add(transaction)
                    #self.db.flush()
                    
                    record.processed_to_final = True
                    record.transaction_id = transaction.id
                    created_count += 1

                    # Batch commit every BATCH_SIZE records
                    if (idx + 1) % self.BATCH_SIZE == 0:
                        self.db.flush()
                        self.db.commit()
                        self._report_progress({
                            "source": "ips_cc",
                            "processed": idx + 1,
                            "total": len(records),
                            "created": created_count,
                            "failed": failed_count
                        })
                        print(f"Committed batch: {idx + 1}/{len(records)} records processed")

                except Exception as e:
                    failed_count += 1
                    self._fail_log(log_entry, str(e))
                    print(f"Error processing IPS CC record {record.id}: {e}")
                    raise

            self.db.flush()
            self.db.commit()
            print(f"Final commit: {created_count} total records processed")
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


    def process_ips_mobile(self, file_id: Optional[int] = None) -> Dict[str, Any]:
        """Process IPS mobile staging records to final transactions"""
        log_entry = self._start_log("ips_mobile_staging", file_id)

        try:
            query = self.db.query(IPSMobileStaging).filter(
                IPSMobileStaging.processed_to_final == False
            )

            if file_id:
                query = query.filter(IPSMobileStaging.source_file_id == file_id)

            records = query.all()
            created_count = 0
            failed_count = 0

            for idx, record in enumerate(records):

                if record.area.upper() in self.org_code_from_area.keys():
                    org_code = self.org_code_from_area.get(record.area.upper())
                elif record.pole in ['60010', '60011', '60002', '60005', '60006', '60007', '9501', '60001', '60003', '60004', 'Campus MP']:
                    org_code = 82074
                else:
                    org_code = 82088

                try:
                    # For cash, settle date = transaction date
                    transaction = Transaction(
                        transaction_date=record.session_start_date_time,
                        transaction_amount=record.paid,
                        settle_date=record.received_date_time + timedelta(hours=2),
                        settle_amount=record.paid + record.convenience_fee,
                        source=DataSourceType.IPS_MOBILE,
                        location_type=LocationType.SINGLE_SPACE_METER if record.meter_type == 'MK5' else LocationType.MULTI_SPACE_METER, # Do I need to be more precise?
                        location_name=record.pole,
                        location_sub_area=record.sub_area,
                        device_terminal_id=record.prid,
                        payment_type=self.map_payment_type(record.partner_name),
                        reference_number=record.prid,
                        org_code=org_code, # Need a way to look up the meter type from the pole or terminal
                        staging_table="ips_mobile_staging",
                        staging_record_id=record.id
                    )
                
                    self.db.add(transaction)
                    
                    record.processed_to_final = True
                    record.transaction_id = transaction.id
                    created_count += 1

                    if (idx + 1) % self.BATCH_SIZE == 0:
                        self.db.flush()
                        self.db.commit()
                        self._report_progress({
                            "source": "ips_mobile",
                            "processed": idx + 1,
                            "total": len(records),
                            "created": created_count,
                            "failed": failed_count
                        })
                        print(f"Committed batch: {idx + 1} of {len(records)} records processed")
                    
                except Exception as e:
                    self.db.rollback()
                    failed_count += 1
                    self._fail_log(log_entry, str(e))
                    print(f"Error processing IPS record {record.id}: {e}")
                    raise

            self.db.flush()
            self.db.commit()
            self._complete_log(log_entry, len(records), created_count, 0, failed_count)
             
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
            
            for idx, record in enumerate(records):
                try:
                    # For cash, settle date = transaction date
                    transaction = Transaction(
                        transaction_date=record.collection_date,
                        transaction_amount=record.coin_revenue,
                        settle_date=record.collection_date,  # Same as transaction date for cash
                        settle_amount=record.coin_revenue,
                        source=DataSourceType.IPS_CASH,
                        location_type=LocationType.SINGLE_SPACE_METER if record.meter_type == 'MK5' else LocationType.MULTI_SPACE_METER, # Do I need to be more precise
                        location_name=record.pole_ser_no,
                        location_sub_area=record.sub_area,
                        device_terminal_id=record.terminal,
                        payment_type=PaymentType.CASH,
                        reference_number=str(record.id),
                        org_code=82088 if record.meter_type == 'MK5' else 82074, # 82088 for single, 82074 for multi
                        staging_table="ips_cash_staging",
                        staging_record_id=record.id
                    )
                    
                    self.db.add(transaction)
                    
                    record.processed_to_final = True
                    record.transaction_id = transaction.id
                    created_count += 1
                    
                    if (idx + 1) % self.BATCH_SIZE == 0:
                        self.db.flush()
                        self.db.commit()
                        self._report_progress({
                            "source": "ips_cash",
                            "processed": idx + 1,
                            "total": len(records),
                            "created": created_count,
                            "failed": failed_count
                        })
                        print(f"Committed batch: {idx + 1} of {len(records)} records processed")

                except Exception as e:
                    failed_count += 1
                    print(f"Error processing IPS Cash record {record.id}: {e}")
                    self._fail_log(log_entry, str(e))
                    raise
            
            self.db.flush()
            self.db.commit()
            # Final progress report for this processor
            self._report_progress({
                "source": "final",
                "processed": len(records),
                "total": len(records),
                "created": created_count,
                "failed": failed_count,
                "status": "completed"
            })
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
    

    def process_zms_cash(self, process_date: str = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')) -> Dict[str, Any]:
        """Process ZMS Cash records to final transactions. Skips staging table."""
        
        try:
            self.db.execute(
                text("""
                    DECLARE @dt datetime
                    SET @dt = :process_date;
                    
                    with cte As (
                        -- Pull cash payment data from a particular date from ZMS table
                        select 
                            CASE
                                WHEN p.Id_Parking = 12 THEN 2
                                ELSE p.Id_Parking
                            END Id_Parking,
                            pa.ParkingName, p.Id_Location, l.Name, l.ShortName, TicketNumber,
                            CASE
                                WHEN l.TxnT2StationAdddress LIKE 'A%' THEN 'Exit'
                                WHEN l.TxnT2StationAdddress LIKE 'E%' THEN 'Entry'
                                WHEN l.TxnT2StationAdddress LIKE 'H%' THEN 'Cashier'
                                WHEN l.TxnT2StationAdddress LIKE 'K%' THEN 'POF'
                                ELSE 'Unknown'
                            END As location_sub_area,
                            l.TxnT2StationAdddress station, p.ParkhouseNumber, p.Amount, p.Amount/100. Amount2, p.Date, p.Time,
                            CONVERT(DATETIME, CONVERT(VARCHAR, CAST(p.Date AS DATE), 120) + ' ' + p.Time) transaction_datetime
                        from Opms.dbo.Payments p
                        left join Opms.dbo.Location l On (p.Id_Parking=l.Id_Parking AND p.Id_Location=l.Id_Location)
                        left join Opms.dbo.ParkingAdmin pa On (p.Id_Parking=pa.Id_Parking)
                        where 
                            Date = @dt
                            AND PayMethod = 0
                            AND l.TxnT2StationAdddress NOT LIKE 'A%'
                    ),-- select * from cte
                    -- Pull rebates from the same date. Group by TicketID to get the sum of any rebates used.
                    rebate_group As (
                        select 
                            max(Id_Parking) Id_Parking, max(Id_Location) Id_Location, max(Id_Equipment) Id_Equipment, max(Date) Date, max(Time) Time, TicketId, count(RebateNumber) RebatesApplied, sum(RebateAmount) sumRebateAmount 
                        from Opms.dbo.Rebates 
                        where 
                            Date = @dt
                        GROUP BY TicketId
                    ), penultimate_step As (
                        SELECT
                            cte.transaction_datetime transaction_date, Amount/100 transaction_amount, 
                            CONVERT(VARCHAR, CAST(cte.transaction_datetime AS DATE), 120) settle_date, Amount/100 - COALESCE(sumRebateAmount,0) settle_amount, 
                            'zms_cash_regular' staging_table, NULL source_file_id, cte.TicketNumber staging_record_id, 
                            1 as payment_method_id, d.device_id, 1 As settlement_system_id, l.location_id, 1 As program_id, cc.charge_code_id, cte.TicketNumber reference_number
                            --, cte.location_sub_area, Name LocationName, cte.ParkhouseNumber, cte.Id_Parking, 
                            --Amount/100 total_cash, COALESCE(sumRebateAmount, 0) sum_rebates, 
                            --COALESCE(RebatesApplied, 0) n_rebates
                        FROM cte
                        LEFT JOIN rebate_group r On (cte.TicketNumber=r.TicketId)
                        INNER JOIN PUReporting.app.dim_device d ON (d.device_terminal_id = cte.station)
                        INNER JOIN PUReporting.app.fact_device_assignment da ON (da.device_id = d.device_id AND cte.transaction_datetime >= da.assign_date AND cte.transaction_datetime < COALESCE(da.end_date, '9999-12-31'))
                        inner join PUReporting.app.dim_location l On (cte.Id_Parking=l.facility_id and l.space_id IS NULL)
                        INNER JOIN PUReporting.app.dim_charge_code cc On (cc.program_type_id=1 and cc.location_id=l.location_id)
                        )
                    INSERT INTO PUReporting.app.fact_transaction (transaction_date, transaction_amount, settle_date, settle_amount, staging_table, source_file_id, staging_record_id, payment_method_id, device_id, settlement_system_id, location_id, program_id, charge_code_id, reference_number)
                    SELECT * FROM penultimate_step
                    ORDER BY cte.Id_Parking, cte.transaction_datetime
                    """), {"process_date": process_date})
            
            self.db.commit()

            return {
                "success": True,
                "records_processed": self.db.execute(text("SELECT count(*) FROM PUReporting.app.fact_transaction WHERE staging_table = 'zms_cash_regular' AND transaction_date = :process_date"), {"process_date": process_date}).scalar(),
                "records_created": self.db.execute(text("SELECT count(*) FROM PUReporting.app.fact_transaction WHERE staging_table = 'zms_cash_regular' AND transaction_date = :process_date"), {"process_date": process_date}).scalar(),
                "records_failed": 0
            }
        
        except Exception as e:
            self.db.rollback()
            raise  

    def _process_zms_cash(self, process_date: Optional[str] = None) -> Dict[str, Any]:
        """Process ZMS Cash records to final transactions. Skips staging table."""
        
        # If no date provided, defaults to previous day
        if process_date is None:    
            process_date = datetime.strftime(datetime.now() - timedelta(days=1), ("%Y-%m-%d"))
        
        try:
            records = pd.read_sql(f"""
                DECLARE @dt datetime
                SET @dt = '{process_date}';
                
                with cte As (
                    -- Pull cash payment data from a particular date from ZMS table
                    select 
                        CASE
                            WHEN p.Id_Parking = 12 THEN 2
                            ELSE p.Id_Parking
                        END Id_Parking,
                        pa.ParkingName, p.Id_Location, l.Name, l.ShortName, TicketNumber,
                        CASE
                            WHEN l.TxnT2StationAdddress LIKE 'A%' THEN 'Exit'
                            WHEN l.TxnT2StationAdddress LIKE 'E%' THEN 'Entry'
                            WHEN l.TxnT2StationAdddress LIKE 'H%' THEN 'Cashiered'
                            WHEN l.TxnT2StationAdddress LIKE 'K%' THEN 'POF'
                            ELSE 'Unknown'
                        END As location_sub_area,
                        l.TxnT2StationAdddress station, p.ParkhouseNumber, p.Amount, p.Amount/100. Amount2, p.Date, p.Time,
                        CONVERT(DATETIME, CONVERT(VARCHAR, CAST(p.Date AS DATE), 120) + ' ' + p.Time) transaction_datetime
                    from Opms.dbo.Payments p
                    left join Opms.dbo.Location l On (p.Id_Parking=l.Id_Parking AND p.Id_Location=l.Id_Location)
                    left join Opms.dbo.ParkingAdmin pa On (p.Id_Parking=pa.Id_Parking)
                    where 
                        Date = @dt
                        AND PayMethod = 0
                        AND l.TxnT2StationAdddress NOT LIKE 'A%'
                ),-- select * from cte
                -- Pull rebates from the same date. Group by TicketID to get the sum of any rebates used.
                rebate_group As (
                    select 
                        max(Id_Parking) Id_Parking, max(Id_Location) Id_Location, max(Id_Equipment) Id_Equipment, max(Date) Date, max(Time) Time, TicketId, count(RebateNumber) RebatesApplied, sum(RebateAmount) sumRebateAmount 
                    from Opms.dbo.Rebates 
                    where 
                        Date = @dt
                    GROUP BY TicketId
                )
                select
                    cte.transaction_datetime transaction_date, Amount/100 - COALESCE(sumRebateAmount,0) settle_amount, 
                    CONVERT(VARCHAR, CAST(cte.transaction_datetime AS DATE), 120) settle_date, Amount/100 - COALESCE(sumRebateAmount,0) settle_amount, 
                    'zms_cash_regular' source, 'GARAGE' location_type, ParkingName as location_name, station device_terminal_id, 'CASH' as payment_type, cte.TicketNumber reference_number,
                    CASE
                        WHEN station LIKE '_1_' THEN 82002
                        WHEN station LIKE '_2_' THEN 82007
                        WHEN station LIKE '_4_' THEN 82005
                        WHEN station LIKE '_5_' THEN 82005
                        WHEN station LIKE '_6_' THEN 82001
                        WHEN station LIKE '_7_' THEN 82004
                        WHEN station LIKE '_8_' THEN 82162
                        ELSE 82172
                    END As org_code,
                    'zms_cash_regular' staging_table, cte.TicketNumber staging_record_id,
                    cte.location_sub_area, Name LocationName, cte.ParkhouseNumber, cte.Id_Parking, 
                    Amount/100 transaction_amount, COALESCE(sumRebateAmount, 0) sum_rebates, 
                    COALESCE(RebatesApplied, 0) n_rebates
                from cte
                left join rebate_group r On (cte.TicketNumber=r.TicketId)
                order by Id_Parking, location_sub_area""", self.db.get_bind())

            created_count = 0
            failed_count = 0

            for idx, record in records.iterrows():
                try:
                    transaction = Transaction(
                            transaction_date=record.transaction_date,
                            transaction_amount=record.transaction_amount,
                            settle_date=record.settle_date,
                            settle_amount=record.settle_amount,
                            source=record.source,
                            location_type=LocationType.GARAGE,
                            location_name=record.location_name,
                            location_sub_area=record.location_sub_area,
                            device_terminal_id=record.device_terminal_id,
                            payment_type=PaymentType.CASH,
                            reference_number=record.reference_number,
                            org_code=record.org_code,
                            staging_table=record.staging_table,
                            staging_record_id=record.staging_record_id
                        )
                    self.db.add(transaction)                
                
                    #record.processed_to_final = True
                    #record.transaction_id = transaction.id
                    created_count += 1

                    if (idx + 1) % 500 == 0:
                        self.db.flush()
                        self.db.commit()
                        print(f"Committed batch: {idx + 1} of {len(records)} records processed")
                
                except Exception as e:
                    failed_count += 1
                    print(f"Error processing ZMS Cash Regular record {record.id}: {e}")
                    raise

            self.db.flush()
            self.db.commit()
            print(f"Committed batch: {idx + 1} of {len(records)} records processed")

            return {
                "success": True,
                "records_processed": len(records),
                "records_created": created_count,
                "records_failed": failed_count
            }
        
        except Exception as e:
            self.db.rollback()
            raise        


    def process_all_staging_tables(self, file_id: Optional[int] = None) -> Dict[str, Any]:
        """Process all staging tables to final transactions"""
        results = {}
        
        # Process each staging table
        processors = [
            ("windcave", self.process_windcave),
            ("payments_insider", self.process_payments_insider),
            ("ips_cc", self.process_ips_cc),
            ("ips_mobile", self.process_ips_mobile),
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
    
    def _lookup_charge_code(self, id: str) -> str:
        charge_code = None
        
        try:
            charge_code = self.charge_code_from_terminal_id[id]
        except:
            charge_code = self.charge_code_from_housing_id[id]

        return charge_code
    
    def _parse_time_string(self, t):
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
    
    def _update_log(self, log_entry: ETLProcessingLog, processed: int, created: int, updated: int, failed: int):
        """
        Update log entry with current progress during batch processing.
        Updates running totals without changing status from 'running'.
        
        Args:
            log_entry: The ETLProcessingLog entry being updated
            processed: Total records processed so far
            created: Total records successfully created so far
            failed: Total records that failed so far
        """
        log_entry.records_processed = processed
        log_entry.records_created = created
        log_entry.records_updated = updated
        log_entry.records_failed = failed
        # Status stays "running" - will be updated by _complete_log() or _fail_log()
        
        # Commit the log update in the same transaction as the batch
        # This ensures consistency between data and log
        self.db.flush()
    
    def _complete_log(self, log_entry: ETLProcessingLog, processed: int, created: int, updated: int, failed: int):
        """Complete a processing log entry with status based on record counts"""
        log_entry.end_time = datetime.now()
        log_entry.records_processed = processed
        log_entry.records_created = created
        log_entry.records_updated = updated
        log_entry.records_failed = failed
        
        # Determine status based on success/failure counts
        total_records = created + updated + failed
        
        if failed == 0 and total_records > 0:
            # All records processed successfully
            log_entry.status = "completed"
        elif failed > 0 and (created > 0 or updated > 0):
            # Some records succeeded, some failed
            log_entry.status = "incomplete"
        elif failed > 0 and created == 0 and updated == 0:
            # All records failed
            log_entry.status = "failed"
        else:
            # No records processed at all
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
    
    def load(self, file_path: str, file_id: int) -> int:
        """Dispatch the correct load method based on data_source_type"""
        loader_method = self.mapping.get(self.data_source_type)
        if not loader_method:
            raise ValueError(f"No loader method for data source type: {self.data_source_type}")
        return loader_method(file_path, file_id)
    

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
        
        # --- Remove transactions from other agencies ---
        df = df[df['group_account'].isin(['CityofMadison_Att', 'CityofMadison_Unatt'])]

        # --- Remove voided transactions ---
        df = df[df['voided'] == 0]

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
            df = pd.read_csv(file_path, skiprows=2, dtype=set_dtypes)

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
                # Create a mask for non-null values
                mask = df[col].notna()
                
                # Only process if there are non-null values
                if mask.any():
                    df.loc[mask, col] = df.loc[mask, col].astype(int)
                
                # Replace NaN with None after conversion
                df[col] = df[col].replace({pd.NA: None, np.nan: None})
        
        # --- Convert pandas NaN to None for SQL ---
        df = df.replace({pd.NA: None, np.nan: None, pd.NaT: None})
        # --- Remove voided transactions (Sales files only) ---
        # Some Payments files do not include a `void_ind` column; guard against that.
        if report_type == 'Sales' and 'void_ind' in df.columns:
            try:
                df = df[df['void_ind'] == 'N']
            except Exception:
                # If any unexpected values exist in void_ind, skip this filter
                pass

        # --- Only use merchant IDs that are '8016090345' ---
        # Different report types use different column names; guard columns    
        if 'mid' in df.columns:
            df = df[df['mid'] == '8016090345']
        if 'merchant_id' in df.columns:
            df = df[df['merchant_id'] == '8016090345']

        # --- Check if there are any records ---
        if df.shape[0] > 0:
            # --- Convert to list of dictionaries ---
            records = df.to_dict(orient="records")

            # --- Bulk insert using SQLAlchemy ---
            if report_type == 'Sales':
                self.db.execute(insert(PaymentsInsiderSalesStaging), records)
            else:
                self.db.execute(insert(PaymentsInsiderPaymentsStaging), records)
            self.db.commit()
        else:
            records = []

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
        df.rename(columns=({'amount_($)':'amount', '$ Paid':'paid', '$0.01':'pennies', '$0.05':'nickels', '$0.10':'dimes', '$0.25':'quarters', '$1.00':'dollars'}), inplace=True)
        
        # --- Make sure these columns are floats
        for col in ['amount']:
            df[col] = df[col].astype(float)
            
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

        # --- Remove .0 from Pole Ser No if present ---
        df['pole'] = df['pole'].apply(lambda x: str(x).split('.')[0] if pd.notna(x) else x)
        
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
        df.rename(columns=({'Amount ($)':'amount', '$_paid':'paid', '$0.01':'pennies', '$0.05':'nickels', '$0.10':'dimes', '$0.25':'quarters', '$1.00':'dollars'}), inplace=True)

        # --- Make sure the paid column is a float ---
        df['paid'] = df['paid'].astype(float)
        
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

        # --- Remove .0 from Pole Ser No if present ---
        df['pole'] = df['pole'].apply(lambda x: str(x).split('.')[0] if pd.notna(x) else x)
        
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
        df.rename(columns=({'Amount ($)':'amount', '$_paid':'paid', '$001':'pennies', '$005':'nickels', '$010':'dimes', '$025':'quarters', '$100':'dollars'}), inplace=True)

        # --- Make sure these columns are floats
        for col in ['pennies', 'nickels', 'dimes', 'quarters', 'dollars']:
            df[col] = df[col].astype(float)
        
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

        # --- Remove .0 from Pole Ser No if present ---
        df['pole_ser_no'] = df['pole_ser_no'].apply(lambda x: str(x).split('.')[0] if pd.notna(x) else x)
        
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
