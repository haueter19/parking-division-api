"""
ETL Lookup Cache Management

This module handles initialization and caching of lookup tables used by the ETL processor.
Caches are built once on application startup and reused across requests to avoid repeated DB queries.
"""

import pandas as pd
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
import logging
from db_manager import ConnectionManager
cnxn = ConnectionManager()

logger = logging.getLogger(__name__)

# Global cache storage
_etl_cache: Dict[str, Any] = {
    'org_code_cache': None,
    'location_from_charge_code': None,
    'charge_code_from_housing_id': {},
    'charge_code_from_terminal_id': {},
    'garage_from_station': {},
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
        garage_from_station = _load_garage_cache(db)
        
        if org_code_df is not None:
            _etl_cache['org_code_cache'] = org_code_df
            logger.info(f"Loaded org code cache with {len(org_code_df)} records")

            charge_code_from_housing_id = {a:b for a,b in zip(org_code_df['Device_ID'], org_code_df['ChargeCode']) if a != None}
            charge_code_from_terminal_id = {a:b for a,b in zip(org_code_df['TerminalID'], org_code_df['ChargeCode']) if a != None}

            charge_code_from_terminal_id['0010050008031494050786'] = 82088
            charge_code_from_terminal_id['0010050008031494050908'] = 82074

            location_from_charge_code = {a:b for a,b in zip(org_code_df['ChargeCode'], org_code_df['Location']) if a != None}
            location_from_charge_code[82044] = 'Capitol Square North'
            location_from_charge_code[82045] = 'Overture Center'
            location_from_charge_code[82047] = 'Wilson Street'
            location_from_charge_code[82048] = 'Lake/Frances'
            location_from_charge_code[82050] = 'State Street Capitol'
            location_from_charge_code[82164] = 'Livingston'
            location_from_charge_code[82172] = 'Over/Short/Helpline'
            location_from_charge_code[82055] = 'Blair Lot'
            location_from_charge_code[82057] = 'Wingra Lot'
            location_from_charge_code[82074] = 'Multi-Space Meters'
            location_from_charge_code[82088] = 'Single Space Meters'
            location_from_charge_code[82224] = 'Buckeye Lot'
            location_from_charge_code[82225] = 'Evergreen Lot'
            location_from_charge_code[82935] = 'Meter Over/Short'

                        
            # Save dicts to the class
            _etl_cache['charge_code_from_housing_id'] = charge_code_from_housing_id
            _etl_cache['charge_code_from_terminal_id'] = charge_code_from_terminal_id
            _etl_cache['location_from_charge_code'] = location_from_charge_code
            _etl_cache['garage_from_station'] = garage_from_station
            
            
            
        else:
            logger.warning("Could not load org code cache from Traffic DB")
        
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
                source, ucds.Device_ID, CONCAT('0010050008016090',CAST(ucds.TerminalID As varchar)) TerminalID, '1900-01-01' as DateAssigned, DATEADD(day, 365, GETDATE()) as DateRemoved, ucds.ChargeCode,
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
            """, traffic_db.get_bind())
        return org_lookup_tbl if not org_lookup_tbl.empty else None
        
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


def _load_garage_cache(db: Optional[Session]) -> Optional[Dict[str, str]]:
    """
    Load mapping from station address to garage name from the primary DB.
    Returns a dict mapping TxnT2StationAdddress -> ParkingName (Garage)
    """
    if db is None:
        logger.warning("Primary DB session not available for garage cache")
        return None

    try:
        query = """
            SELECT 
                Location.TxnT2StationAdddress as station,
                pa.ParkingName as garage
            FROM Opms.dbo.Location
            INNER JOIN Opms.dbo.ParkingAdmin pa On (Location.Id_Parking=pa.Id_Parking)
        """
        df = pd.read_sql(query, db.get_bind())
        if df.empty:
            return None
        return {a: b for a, b in zip(df['station'], df['garage']) if a is not None}
    except Exception as e:
        logger.error(f"Error loading garage cache: {e}")
        return None


def get_org_code_cache() -> Optional[pd.DataFrame]:
    """
    Get the cached org code lookup table.
    
    Returns:
        DataFrame with organization code mappings or None if not initialized
    """
    return _etl_cache.get('org_code_cache')


def get_charge_code_from_housing_id() -> Dict[str, int]:
    """Return mapping of housing/device id -> charge code"""
    return _etl_cache.get('charge_code_from_housing_id', {})


def get_charge_code_from_terminal_id() -> Dict[str, int]:
    """Return mapping of terminal id -> charge code"""
    return _etl_cache.get('charge_code_from_terminal_id', {})


def get_garage_from_station() -> Dict[str, str]:
    """Return mapping of station address -> garage name"""
    return _etl_cache.get('garage_from_station', {})


def get_location_from_charge_code() -> Optional[Dict[str, str]]:
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
