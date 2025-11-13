"""
Transaction Processor Utility
Handles loading and processing Payments Insider transaction data
"""

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class TransactionProcessor:
    """Processes Payments Insider transaction files and loads into database"""
    
    def __init__(self, db_session: Session):
        """
        Initialize processor with database session
        
        Args:
            db_session: SQLAlchemy database session
        """
        self.db = db_session
    
    def load_from_file(self, file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """
        Load data from Excel or CSV file
        
        Args:
            file_path: Path to the spreadsheet file
            sheet_name: Sheet name for Excel files (optional)
        
        Returns:
            DataFrame with loaded data
        """
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
        
        logger.info(f"Loaded {len(df)} records from {file_path}")
        return df
    
    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Payments Insider dataframe to match database schema
        
        Args:
            df: Raw dataframe from Payments Insider
            
        Returns:
            Transformed dataframe ready for database insertion
        """
        transformed = df.copy()
        
        # Column mapping
        column_mapping = {
            'Business Name': 'business_name',
            'MID': 'mid',
            'Card Brand': 'card_brand',
            'Card Number': 'card_number',
            'Transaction Type': 'transaction_type',
            'Void Ind': 'void_ind',
            'Settled Amount': 'settled_amount',
            'Settled Currency': 'settled_currency',
            'Settled Date': 'settled_date',
            'Transaction Amount': 'transaction_amount',
            'Transaction Currency': 'transaction_currency',
            'Transaction Date': 'transaction_date',
            'Transaction Time': 'transaction_time',
            'Authorization Code': 'authorization_code',
            'GBOK / Batch ID': 'gbok_batch_id',
            'Terminal ID': 'terminal_id',
            'Durbin Regulated': 'durbin_regulated',
            'ROC Text': 'roc_text',
            'Invoice': 'invoice',
            'Order Number': 'order_number',
            'Custom Data 1': 'custom_data_1',
            'Card Swipe Indicator': 'card_swipe_indicator',
            'POS Entry': 'pos_entry'
        }
        transformed.rename(columns=column_mapping, inplace=True)
        
        # Convert datetime columns to date
        transformed['settled_date'] = pd.to_datetime(transformed['settled_date']).dt.date
        transformed['transaction_date'] = pd.to_datetime(transformed['transaction_date']).dt.date
        
        # Ensure numeric columns are properly typed
        transformed['mid'] = transformed['mid'].astype('int64')
        transformed['gbok_batch_id'] = transformed['gbok_batch_id'].astype('int64')
        transformed['terminal_id'] = transformed['terminal_id'].astype('int64')
        transformed['pos_entry'] = transformed['pos_entry'].astype('int64')
        
        # Handle nullable numeric fields
        transformed['roc_text'] = pd.to_numeric(transformed['roc_text'], errors='coerce')
        
        # Strip whitespace from string columns
        string_columns = [
            'business_name', 'card_brand', 'card_number', 'transaction_type',
            'void_ind', 'settled_currency', 'transaction_currency', 'transaction_time',
            'authorization_code', 'durbin_regulated', 'invoice', 'order_number',
            'custom_data_1', 'card_swipe_indicator'
        ]
        for col in string_columns:
            if col in transformed.columns:
                transformed[col] = transformed[col].astype(str).str.strip()
                transformed[col] = transformed[col].replace('nan', None)
        
        logger.info(f"Transformed {len(transformed)} records")
        return transformed
    
    def validate_data(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        """
        Validate transformed data before insertion
        
        Args:
            df: Transformed dataframe
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check required columns
        required_columns = [
            'business_name', 'mid', 'card_brand', 'card_number', 'transaction_type',
            'void_ind', 'settled_amount', 'settled_currency', 'settled_date',
            'transaction_amount', 'transaction_currency', 'transaction_date',
            'transaction_time', 'authorization_code', 'gbok_batch_id', 'terminal_id',
            'durbin_regulated', 'card_swipe_indicator', 'pos_entry'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
        
        # Check for null values in required columns
        for col in required_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    errors.append(f"Column '{col}' has {null_count} null values (required field)")
        
        # Validate currency codes
        if 'settled_currency' in df.columns:
            invalid = df[~df['settled_currency'].str.len().eq(3)]['settled_currency'].unique()
            if len(invalid) > 0:
                errors.append(f"Invalid settled_currency codes: {invalid}")
        
        if 'transaction_currency' in df.columns:
            invalid = df[~df['transaction_currency'].str.len().eq(3)]['transaction_currency'].unique()
            if len(invalid) > 0:
                errors.append(f"Invalid transaction_currency codes: {invalid}")
        
        is_valid = len(errors) == 0
        return is_valid, errors
    
    def insert_data(self, df: pd.DataFrame, if_exists: str = 'append', 
                   batch_size: int = 1000) -> int:
        """
        Insert transformed data into transactions table
        
        Args:
            df: Transformed and validated dataframe
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            batch_size: Number of records to insert per batch
            
        Returns:
            Number of records inserted
        """
        records_inserted = 0
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            batch.to_sql(
                'transactions',
                self.db.bind,
                if_exists=if_exists,
                index=False,
                method='multi',
                schema='dbo'
            )
            records_inserted += len(batch)
            logger.info(f"Inserted batch: {records_inserted}/{len(df)} records")
        
        logger.info(f"Successfully inserted {records_inserted} records")
        return records_inserted
    
    def process_payments_insider_file(
        self, 
        file_path: str, 
        sheet_name: Optional[str] = None,
        if_exists: str = 'append'
    ) -> int:
        """
        Complete pipeline: load, transform, validate, and insert data
        
        Args:
            file_path: Path to spreadsheet file
            sheet_name: Sheet name for Excel files (optional)
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            
        Returns:
            Number of records inserted
        """
        # Load data
        df = self.load_from_file(file_path, sheet_name)
        
        # Transform data
        df_transformed = self.transform_dataframe(df)
        
        # Validate data
        is_valid, errors = self.validate_data(df_transformed)
        if not is_valid:
            error_msg = "\n".join(errors)
            raise ValueError(f"Data validation failed:\n{error_msg}")
        
        # Insert data
        records_inserted = self.insert_data(df_transformed, if_exists=if_exists)
        
        return records_inserted