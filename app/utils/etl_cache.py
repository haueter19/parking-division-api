"""
ETL Lookup Cache Management

This module handles initialization and caching of lookup tables used by the ETL processor.
Caches are built once on application startup and reused across requests to avoid repeated DB queries.
"""

import pandas as pd
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)

# Global cache storage
_etl_cache: Dict[str, Any] = {
    'org_code_cache': None,
    'location_from_charge_code': None,
    'is_initialized': False
}


def initialize_etl_cache(db: Session, traffic_db: Optional[Session] = None) -> bool:
    """
    Initialize ETL lookup caches on application startup.
    
    Args:
        db: Primary application database session (PUReporting)
        traffic_db: Optional Traffic database session for org code lookups
        
    Returns:
        bool: True if initialization succeeded, False otherwise
    """
    global _etl_cache
    
    try:
        logger.info("Initializing ETL lookup caches...")
        
        # Initialize org code cache
        org_code_df = _load_org_code_cache(traffic_db)
        if org_code_df is not None:
            _etl_cache['org_code_cache'] = org_code_df
            logger.info(f"Loaded org code cache with {len(org_code_df)} records")
        else:
            logger.warning("Could not load org code cache from Traffic DB")
        
        # Initialize location lookup from charge code
        location_map = _load_location_cache(traffic_db)
        if location_map is not None:
            _etl_cache['location_from_charge_code'] = location_map
            logger.info(f"Loaded location cache with {len(location_map)} charge codes")
        else:
            logger.warning("Could not load location cache from Traffic DB")
        
        _etl_cache['is_initialized'] = True
        logger.info("ETL cache initialization completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing ETL caches: {e}", exc_info=True)
        _etl_cache['is_initialized'] = False
        return False


def _load_org_code_cache(traffic_db: Optional[Session]) -> Optional[pd.DataFrame]:
    """
    Load organization code lookup table from Traffic database.
    
    Returns:
        DataFrame with columns: TerminalID, ChargeCode, DateAssigned, DateRemoved, Facility_Name
        or None if unable to load
    """
    if traffic_db is None:
        logger.warning("Traffic DB session not available for org code cache")
        return None
    
    try:
        query = """
        SELECT
            a.Device_ID,
            a.TerminalID,
            b.ChargeCode,
            a.Facility_Name_Abr,
            a.Facility_Name_Full,
            CAST(a.DateRemoved AS DATE) as DateRemoved,
            'EMV Reader' as source
        FROM data_admin8.PU_PARCS_EQUIP a
        INNER JOIN data_admin8.PU_PARCS_UCD b ON (b.HousingID = a.Device_ID)
        WHERE a.TerminalID IS NOT NULL AND b.ChargeCode IS NOT NULL
        ORDER BY a.TerminalID
        """
        
        df = pd.read_sql(query, traffic_db.get_bind())
        return df if not df.empty else None
        
    except Exception as e:
        logger.error(f"Error loading org code cache: {e}")
        return None


def _load_location_cache(traffic_db: Optional[Session]) -> Optional[Dict[str, str]]:
    """
    Load location/facility name lookup from charge codes.
    
    Returns:
        Dictionary mapping ChargeCode -> Facility_Name_Full
        or None if unable to load
    """
    if traffic_db is None:
        logger.warning("Traffic DB session not available for location cache")
        return None
    
    try:
        query = """
        SELECT DISTINCT
            b.ChargeCode,
            a.Facility_Name_Full
        FROM data_admin8.PU_PARCS_EQUIP a
        INNER JOIN data_admin8.PU_PARCS_UCD b ON (b.HousingID = a.Device_ID)
        WHERE b.ChargeCode IS NOT NULL AND a.Facility_Name_Full IS NOT NULL
        """
        
        df = pd.read_sql(query, traffic_db.get_bind())
        
        if df.empty:
            return None
        
        # Create mapping: ChargeCode -> Facility_Name_Full
        location_map = dict(zip(df['ChargeCode'], df['Facility_Name_Full']))
        return location_map
        
    except Exception as e:
        logger.error(f"Error loading location cache: {e}")
        return None


def get_org_code_cache() -> Optional[pd.DataFrame]:
    """
    Get the cached org code lookup table.
    
    Returns:
        DataFrame with organization code mappings or None if not initialized
    """
    return _etl_cache.get('org_code_cache')


def get_location_cache() -> Optional[Dict[str, str]]:
    """
    Get the cached location/facility name lookup.
    
    Returns:
        Dict mapping ChargeCode -> Facility_Name or None if not initialized
    """
    return _etl_cache.get('location_from_charge_code')


def is_cache_initialized() -> bool:
    """
    Check if ETL caches have been initialized.
    
    Returns:
        True if caches are ready to use, False otherwise
    """
    return _etl_cache.get('is_initialized', False)


def reset_cache():
    """
    Reset all cached lookups. Useful for testing or manual cache refresh.
    """
    global _etl_cache
    _etl_cache = {
        'org_code_cache': None,
        'location_from_charge_code': None,
        'is_initialized': False
    }
    logger.info("ETL cache reset")
