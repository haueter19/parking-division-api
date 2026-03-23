from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

from db_manager import ConnectionManager
cnxn = ConnectionManager()

def count_occupancy(df, start, end, garage=None, by_customer_type=True):
    """
    Counts the number of records active for each minute between start and end
    Parameters:
    df (DataFrame): A DataFrame with 'EntryDate' and 'ExitDate' as datetime columns.
    by_customer_type (bool): If True, returns counts broken down by customer type
    Returns:
    Series or DataFrame: If by_customer_type=False, returns Series with minute timestamps 
                        and total counts. If True, returns DataFrame with columns for each 
                        customer type.
    """
    # Define the full minute range
    start = pd.Timestamp(start)
    end = pd.Timestamp(end)
    all_minutes = pd.date_range(start=start, end=end, freq='min')
    
    # Filter on garage
    if garage:
        df = df[df['Garage']==garage].copy()
    
    # Clip Entry and Exit to the date range
    df['EntryDate'] = df['EntryDate'].clip(lower=start, upper=end)
    df['ExitDate'] = df['ExitDate'].clip(lower=start, upper=end)
    
    # Clean data
    #df = df.dropna(subset=['EntryDate', 'ExitDate'])
    df = df[df['EntryDate'] < df['ExitDate']]
    
    if not by_customer_type:
        # Original logic - return total counts
        counts = pd.Series(0, index=all_minutes)
        entry_counts = df['EntryDate'].dt.floor('min').value_counts()
        exit_counts = df['ExitDate'].dt.ceil('min').value_counts()
        counts = counts.add(entry_counts, fill_value=0)
        counts = counts.subtract(exit_counts, fill_value=0)
        return counts.cumsum().astype(int)
    
    else:
        # New logic - return counts by customer type
        if 'customer_type' not in df.columns:
            raise ValueError("DataFrame must have 'customer_type' column when by_customer_type=True")
        
        df = df.dropna(subset=['customer_type'])
        customer_types = sorted(df['customer_type'].unique(), reverse=True)
        
        # Initialize DataFrame with all customer types
        result_df = pd.DataFrame(0, index=all_minutes, columns=customer_types)
        
        # Process each customer type separately
        for ctype in customer_types:
            ctype_df = df[df['customer_type'] == ctype]
            
            # Apply sweep line algorithm for this customer type
            counts = pd.Series(0, index=all_minutes)
            entry_counts = ctype_df['EntryDate'].dt.floor('min').value_counts()
            exit_counts = ctype_df['ExitDate'].dt.ceil('min').value_counts()
            counts = counts.add(entry_counts, fill_value=0)
            counts = counts.subtract(exit_counts, fill_value=0)
            
            result_df[ctype] = counts.cumsum().astype(int)
        
        return result_df
    

def prep_summary(inner_data, outer_data, garage, garage_id):
    # Find the earliest visit
    f = min(inner_data.EntryDate.min(), inner_data.ExitDate.min()).floor('min')
    t = datetime.strftime(inner_data.ExitDate.max().floor('min') - timedelta(minutes=1), '%Y-%m-%d %H:%M:%S')
    
    summary = count_occupancy(outer_data, f, t, garage)
    if 'transient' not in summary.columns:
        summary['transient'] = 0
    if 'permit' not in summary.columns:
        summary['permit'] = 0
    if 'employee' not in summary.columns:
        summary['employee'] = 0
    summary['total'] = summary['transient'] + summary['permit'] + summary['employee']
    summary['GarageID'] = garage_id
    summary['year'] = summary.index.year
    summary['quarter'] = summary.index.quarter
    summary['month'] = summary.index.month
    summary['week'] = summary.index.isocalendar().week
    summary['day'] = summary.index.day
    summary['dayofyear'] = summary.index.dayofyear
    summary['dayofweek'] = summary.index.dayofweek
    summary['hour'] = summary.index.hour
    summary['hms'] = summary.index.strftime('%H:%M:%S')
    summary['weekday_type'] = summary['dayofweek'].apply(lambda z: 1 if z<5 else 0)
    summary['period'] = summary['hour'].apply(lambda z: 'Daytime' if 6 <= z <= 18 else 'Evening')
    summary.loc[summary['hour'].between(6, 18), 'period'] = 'Daytime'
    summary.loc[~summary['hour'].between(6, 18), 'period'] = 'Evening'
    summary.loc[summary['dayofweek']>4, 'period'] = 'Weekend'
    
    return summary.iloc[:-1]


def get_inner_data(garage_id, dateFrom, dateThru):
    # This query finds the visits that could have been affected by an update from the most recent load date
    inner_data = pd.read_sql(f"""
        SELECT
            TripDetailID, GarageID, Garage, TicketNumber, EntryDate, ExitDate, customer_type, status, length_of_stay, LoadDate
            --, impute_duration, exit_classification, Amount, PermitName, GroupName, CustomerName, CustomerID, LoadDate
        FROM dw.VisitDetails
        WHERE
            GarageID = {garage_id}
            AND EntryDate < '{dateThru}'
            AND (ExitDate >= '{dateFrom}' OR ExitDate IS NULL)
        ORDER BY EntryDate, ExitDate DESC
        """, cnxn.get_engine('PUReporting'))
    print(inner_data.shape)
    return inner_data

def get_outer_data(garage_id, dateFrom, dateThru, inner_data):
    # This query returns all visits that occur during any part of the inner_data range. This is required to make sure we can properly build the summary table .
    outer_data = pd.read_sql(f"""
        SELECT
            --TripDetailID, 
            GarageID, Garage, TicketNumber, EntryDate, ExitDate, customer_type, status, length_of_stay
            --, impute_duration, exit_classification, Amount, PermitName, GroupName, CustomerName, CustomerID, LoadDate
        FROM dw.VisitDetails
        WHERE
            GarageID = {garage_id}
            AND EntryDate < '{dateThru}'
            AND (ExitDate >= '{inner_data.EntryDate.min().floor('d')}' OR ExitDate IS NULL)
        ORDER BY EntryDate, ExitDate DESC
        """, cnxn.get_engine('PUReporting'))
    print(outer_data.shape)
    return outer_data


def prep_data(garage_id, garage, now):
    """
    Start by finding the date range to build the occupancy for. This involves looking at dw.VisitDetails and finding all visits where the LoadDate is greater than or equal to the last date in the summary table.
    """
    date_range = pd.read_sql(f"""
        select
        	LoadDate, min(EntryDate) min_entry, min(ExitDate) min_exit, max(EntryDate) max_entry, max(ExitDate) max_exit
        from dw.VisitDetails
        where
        	GarageID = {garage_id}
        	AND	LoadDate >= (
        		select
        			max(date) max_date
        		from dw.VisitSummary 
        		where GarageID = {garage_id}
        		)
        GROUP BY LoadDate
        """, cnxn.get_engine('PUReporting'))

    # Uses the previous query to find the from/thru range
    # Will need to build the summary table for these dates and then update/insert the data warehouse table
    dateFrom, dateThru = date_range.min().min().floor('min'), date_range.max().max().ceil('min')
    #dateFrom = '2022-09-01 00:00:00' # override 
    print(dateFrom, dateThru)

    inner_data = get_inner_data(garage_id, dateFrom, dateThru)
    outer_data = get_outer_data(garage_id, dateFrom, dateThru, inner_data)
    
    print(f"inner_data with los < 0: {inner_data[inner_data['length_of_stay']<0].shape[0]}")

    # Close any existing visits with cutoff of last entry/exit plus 1 minute
    inner_data.fillna({'ExitDate':dateThru.floor('min')+timedelta(minutes=1)}, inplace=True)
    
    # VERY IMPORTANT!!! Must do this for the outer_data to make the summary work
    outer_data.fillna({'ExitDate':dateThru.floor('min')+timedelta(minutes=1)}, inplace=True)

    return inner_data, outer_data


date_range = pd.read_sql(f"""
        select
        	--LoadDate, EntryDate, ExitDate, status, customer_type, PermitName, PermitNumber, GroupName, CustomerName
            LoadDate, min(EntryDate) min_entry, min(ExitDate) min_exit, max(EntryDate) max_entry, max(ExitDate) max_exit
        from dw.VisitDetails
        where
        	GarageID = 1
        	AND	LoadDate >= (
        		select
        			max(date) max_date
        		from dw.VisitSummary 
        		where GarageID = 1
        		)
        GROUP BY LoadDate
        --ORDER BY EntryDate
        """, cnxn.get_engine('PUReporting'))

garages = pd.read_sql("SELECT Id_Parking, ParkingName garage FROM ParkingAdmin WHERE Id_Parking NOT IN (7, 9)", cnxn.get_engine('opms'))
garages = {a:b for a,b in zip(garages.Id_Parking, garages.garage)}

garage_id = 1
garage = garages[garage_id]
now = pd.Timestamp(datetime.now()).floor('min')

inner_data, outer_data = prep_data(garage_id, garage, now)

# Check that there are no duplicate TripDetailID values
assert inner_data.shape[0] == inner_data.TripDetailID.nunique()

# Check that there are no duplicates
assert inner_data.duplicated(keep=False).sum() == 0
#s = data.duplicated(keep=False)
#data.loc[data.index.isin(s[s].index)]

# Check that all length of stays are >= 0
assert inner_data[inner_data['length_of_stay']<0].shape[0] == 0

update_summary_from_date = min(inner_data.EntryDate.min(), inner_data.ExitDate.min()).floor('min')

summary = prep_summary(inner_data, outer_data, garage, garage_id)

##### First need to disable the indexes
# Then rebuild them

# Delete records, then re-insert the updated records
with cnxn.get_engine('PUReporting').begin() as conn:  # begin() auto-commits on success, auto-rollback on error
    print(f"Deleting old records...")
    # Try the DELETE
    delete_result = conn.execute(
        text("DELETE FROM dw.VisitSummary WHERE GarageID = :garage_id AND date >= :from_date"),
        {"garage_id":garage_id, "from_date": update_summary_from_date}
    )
    print(f"Deleted {delete_result.rowcount} records")


with cnxn.get_engine('PUReporting').begin() as conn:
    # Then try the INSERT
    start_insert_time = datetime.now()
    print(f"Inserting new records ({start_insert_time})")    
    insert_result = summary[['GarageID', 'transient', 'permit', 'employee', 'total', 'year', 'quarter', 'month', 'week', 'day', 'dayofyear', 'dayofweek', 'hour', 'hms', 'weekday_type', 'period']].reset_index().rename(columns={'index':'date'})\
        .to_sql('VisitSummary', schema='dw', con=cnxn.get_engine('PUReporting'), if_exists='append', index=False, chunksize=1000)
    print(f"Inserted {summary.shape[0]} records")
    
    # Transaction auto-commits here if we reach this point
    print("Insert completed successfully!")
    end_insert_time = datetime.now()

#Inserting new records (2026-03-18 12:23:49.445883)
#Inserted 1436806 records
#Insert completed successfully!