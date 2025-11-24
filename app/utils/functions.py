from datetime import datetime, timedelta
import re

def extract_date_from_filename(filename: str) -> datetime | None:
    """
    Extract a date from the filename in the format YYYYMMDD.
    
    Args:
        filename: The original filename (e.g., "Report_20240615.xlsx")
    
    Returns:
        A datetime object if a date is found, otherwise None.
    """
    pattern = r'(\d{4}-\d{2}-\d{2}|\d{8}|\d{2}_\d{2}_\d{4})'


    match = re.search(pattern, filename)
    if match:
        date_str = match.group()
        try:
            if '-' in date_str:  # YYYY-MM-DD
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            elif '_' in date_str:  # MM_DD_YYYY
                dt = datetime.strptime(date_str, '%m_%d_%Y')
            else:  # YYYYMMDD
                dt = datetime.strptime(date_str, '%Y%m%d')
            #print(f"String: {s}\nExtracted Date: {date_str}\nDatetime: {dt}\n")
            return datetime.strftime(dt - timedelta(days=1), '%Y-%m-%d')  # Adjust to previous day
        except ValueError as e:
            print(f"Error converting {date_str}: {e}\n")
    
    return None