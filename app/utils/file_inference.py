"""
Utility functions for inferring file metadata from filename patterns.
"""

import re
from app.models.database import DataSourceType


def infer_data_source_type(filename: str) -> DataSourceType:
    """
    Infer the data source type from the uploaded filename using regex rules.
    
    Rules (case-insensitive, checked in order):
    - Payments Insider: starts with "payments" or "sales"
    - IPS: starts with "pbp", "dailybankrecon", or "collection report"
    - Windcave: starts with "windcave", "untitled", or "full"
    - Other: default fallback
    
    Args:
        filename: The original filename (e.g., "Payments_Report_2025.xlsx")
    
    Returns:
        DataSourceType enum value
    """
    if not filename:
        return DataSourceType.OTHER
    
    filename_lower = filename.lower()
    
    # Payments Insider patterns
    if filename_lower.startswith("payments"):
        return DataSourceType.PAYMENTS_INSIDER_PAYMENTS
    
    if filename_lower.contains("sales"):
        return DataSourceType.PAYMENTS_INSIDER_SALES
    
    # IPS patterns
    if filename_lower.startswith("dailybankrecon"):
        return DataSourceType.IPS_CC
    
    if filename_lower.startswith("pbp"):
        return DataSourceType.IPS_MOBILE
    
    if filename_lower.startswith("collection report"):
        return DataSourceType.IPS_CASH
    
    # Windcave patterns
    if (filename_lower.startswith("windcave") or 
        filename_lower.startswith("wc")):
        return DataSourceType.WINDCAVE
    
    # Default fallback
    return DataSourceType.OTHER
