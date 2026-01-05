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
from pathlib import Path
import os
from app.models.database import (
    Transaction, DataSourceType, LocationType, PaymentType,
    WindcaveStaging, PaymentsInsiderPaymentsStaging, PaymentsInsiderSalesStaging, 
    IPSCreditCardStaging, IPSMobileStaging, IPSCashStaging, SQLCashStaging, IPSStaging,
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

    def _report_progress(self, payload: Dict[str, Any]):
        """Invoke progress callback if provided. Swallow any exceptions from callback."""
        if not self.progress_callback:
            return
        try:
            self.progress_callback(payload)
        except Exception:
            # Don't let progress reporting break processing
            pass
    

    def process_file(self, file_id: int, source_key: Optional[str] = None, staging_table: Optional[str] = None) -> Dict[str, Any]:
        """Generic processing entry point for an uploaded file using SQL templates.
        
        Follows the pattern of process_windcave. Uses pure SQL (no ORM).
        1. Determine data source type from uploaded_files record
        2. Load SQL templates (main, failed, update)
        3. Start logging
        4. Execute main transaction INSERT (from template)
        5. Execute failed transaction INSERT (from template)
        6. Update staging table processed_to_final flags (from template or default)
        7. Commit and log completion
        """
        if source_key and staging_table:
            # If both provided, use them directly
            pass
        else:
            # If not provided, look up from file record
            # Look up the file record to determine source type (pure SQL)
            file_record_result = self.db.execute(
                text("SELECT data_source_type FROM app.uploaded_files WHERE id = :file_id"),
                {"file_id": file_id}
            ).first()
            
            if not file_record_result:
                raise ValueError(f"UploadedFile not found: {file_id}")
            
            data_source_type_str = file_record_result[0]
            data_source_type = DataSourceType[data_source_type_str]
            
            source_key, staging_table = self._get_source_key_and_staging_table(data_source_type)
        
        if not staging_table:
            raise ValueError(f"No staging table mapped for source type: {data_source_type}")
        
        print(f"Processing file {file_id} for source '{source_key}' using staging table '{staging_table}'")

        # Start processing log
        log = self._start_log(staging_table, file_id)
        
        try:
            # Load SQL templates
            main_sql = self._load_sql_template(source_key, "main")
            failed_sql = self._load_sql_template(source_key, "failed")
            #update_sql = self._load_sql_template(source_key, "update")
            
            if not main_sql:
                raise ValueError(f"No main SQL template found for {source_key}")
            
            # Execute main SQL: insert successful records into fact_transaction
            result = self.db.execute(text(main_sql), {"file_id": file_id})
            created_count = result.rowcount
            try:
                # Ensure the DB cursor/result is closed so the connection is free
                result.close()
            except Exception:
                pass
            
            # Execute failed SQL: insert rejected records into fact_transaction_reject
            failed_count = 0
            if failed_sql:
                failed_result = self.db.execute(text(failed_sql), {"file_id": file_id})
                failed_count = failed_result.rowcount
                try:
                    failed_result.close()
                except Exception:
                    pass
            
            # Update staging table processed flags
            #if update_sql:
                #self.db.execute(text(update_sql), {"file_id": file_id})
            #else:
            # Default: mark all records with created transactions as processed
            if source_key == 'payments_insider_payments':
                default_update = f"""
                    UPDATE p
                    SET p.processed_to_final = 1, p.loaded_at = GETDATE()
                    FROM app.payments_insider_payments_staging p
                    INNER JOIN app.payments_insider_sales_staging s On (p.card_number=s.card_number and p.authorization_code=s.authorization_code)
                    WHERE 
                        p.source_file_id = 342
                        AND s.id IN (
                            SELECT staging_record_id
                            FROM app.fact_transaction
                            WHERE staging_table = 'payments_insider_sales_staging' AND source_file_id = s.source_file_id
                            );
                """
            else:
                default_update = f"""
                    UPDATE s
                    SET processed_to_final = 1, loaded_at = GETDATE()
                    FROM app.{staging_table} s
                    WHERE s.id IN (
                        SELECT staging_record_id
                        FROM app.fact_transaction
                        WHERE staging_table = :staging_table AND source_file_id = :file_id
                    );
                """
            self.db.execute(text(default_update), {"staging_table": staging_table, "file_id": file_id})
            
            # Get total record count
            total_count = self.db.execute(
                text(f"SELECT COUNT(*) FROM app.{staging_table} WHERE source_file_id = :file_id"),
                {"file_id": file_id}
            ).scalar()
            
            self.db.commit()
            self._complete_log(log, processed=total_count, created=created_count, updated=0, failed=failed_count)
            
            return {
                "success": True,
                "records_processed": total_count,
                "records_created": created_count,
                "records_failed": failed_count
            }
        
        except Exception as e:
            self.db.rollback()
            self._fail_log(log, str(e))
            raise



    def _get_source_key_and_staging_table(self, data_source_type: DataSourceType) -> tuple:
        """Map DataSourceType to a short source key and staging table name."""
        mapping = {
            DataSourceType.WINDCAVE: ("windcave", "windcave_staging"),
            DataSourceType.PAYMENTS_INSIDER_PAYMENTS: ("payments_insider_payments", "payments_insider_payments_staging"),
            DataSourceType.PAYMENTS_INSIDER_SALES: ("payments_insider_sales", "payments_insider_sales_staging"),
            DataSourceType.IPS: ("ips", "ips_staging"),
            DataSourceType.IPS_CC: ("ips_cc", "ips_cc_staging"),
            DataSourceType.IPS_MOBILE: ("ips_mobile", "ips_mobile_staging"),
            DataSourceType.IPS_CASH: ("ips_cash", "ips_cash_staging"),
            # Add other mappings if/when needed
        }
        return mapping.get(data_source_type, ("unknown", ""))
    

     
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
            log_entry.status = "complete"
        elif failed > 0 and (created > 0 or updated > 0):
            # Some records succeeded, some failed
            log_entry.status = "incomplete"
        elif failed > 0 and created == 0 and updated == 0:
            # All records failed
            log_entry.status = "failed"
        else:
            # No records processed at all
            log_entry.status = "complete"
        
        self.db.commit()
    
    def _fail_log(self, log_entry: ETLProcessingLog, error_message: str):
        """Mark a processing log entry as failed"""
        log_entry.end_time = datetime.now()
        log_entry.status = "failed"
        log_entry.error_message = error_message
        self.db.commit()

      # --- Generic SQL-template based processing helpers -----------------
    def _load_sql_template(self, source_key: str, kind: str) -> Optional[str]:
        """Load SQL template file for a given source and kind (main|failed|update).

        Files are expected under app/utils/sql_templates and named
        `{source_key}_{kind}.sql` (e.g. `windcave_main.sql`). Returns None
        if the file is missing or empty.
        """
        templates_dir = Path(__file__).resolve().parent / "sql_templates"
        fname = f"{source_key}_{kind}.sql"
        path = templates_dir / fname
        try:
            if not path.exists():
                return None
            content = path.read_text(encoding="utf-8").strip()
            return content if content else None
        except Exception:
            return None
    

    

class DataLoader:
    """Load data from files to staging tables"""
    
    def __init__(self, db: Session, data_source_type: DataSourceType):
        self.db = db
        self.data_source_type = data_source_type
        self.mapping = {
            DataSourceType.WINDCAVE: self.load_windcave_csv,
            DataSourceType.PAYMENTS_INSIDER_PAYMENTS: self.load_payments_insider,
            DataSourceType.PAYMENTS_INSIDER_SALES: self.load_payments_insider,
            DataSourceType.IPS: self.load_ips,
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
    

    def load_ips(self, file_path: str, file_id: int, convenience_fee: float = 0.45) -> int:
        """Load IPS data to staging table"""

        # Set dtypes
        set_dtypes = {'Pole':str, 'Space Name':str, 'Terminal':str}

        # Determine if Excel or CSV
        if file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path, dtype=set_dtypes)
        else:
            df = pd.read_csv(file_path, dtype=set_dtypes)

        # --- Check for a sum or total at the bottom of the report and remove it ---
        df = df[df['Date'].notna()]

        # --- Normalize column names ---
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("/","").str.replace('\n','').str.replace('.','')
        df.rename(columns=({'card_#':'card_number'}), inplace=True)
        #df.rename(columns=({'Amount ($)':'amount', '$_paid':'paid', '$001':'pennies', '$005':'nickels', '$010':'dimes', '$025':'quarters', '$100':'dollars'}), inplace=True)

        # --- Add metadata columns ---
        df["source_file_id"] = file_id
        df["processed_to_final"] = False

        # --- Make sure these columns are floats
        for col in ['credit_card', 'smart_card', 'total', 'coin', 'bills']:
            df[col] = df[col].astype(float)

        # --- Convert datetimes where possible ---
        for col in df.columns:
            if "date" in col:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception:
                    pass

        # --- Handle integer columns - replace NaN with None ---
        int_columns = ['transaction_hour', 'vendor_id', 'unrecognized_coins']

        for col in int_columns:
            if col in df.columns:
                # Convert to nullable integer type or replace NaN with None
                df[col] = df[col].replace({pd.NA: None, np.nan: None})
                # Convert to int where not None
                df.loc[df[col].notna(), col] = df[col].loc[df[col].notna()].astype(int)

        # --- Convert pandas NaN to None for SQL ---
        df = df.replace({pd.NA: None, np.nan: None, pd.NaT: None})

        # --- Remove .0 from column ---
        for col in ['pole', 'terminal', 'transaction_hour']:
            df[col] = df[col].apply(lambda x: str(x).split('.')[0] if pd.notna(x) else x)

        # --- Convert to list of dictionaries ---
        records = df.to_dict(orient="records")
        
        # --- Bulk insert using SQLAlchemy ---
        self.db.execute(insert(IPSStaging), records)
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

