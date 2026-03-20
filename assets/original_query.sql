DECLARE @Id_Parking int
    DECLARE @fromDate datetime
    DECLARE @thruDate datetime
    DECLARE @impute_duration float
    
    set @Id_Parking = {garage_id} -- Limit to a particular garage or lot
    set @fromDate = '{date_from}' -- Start of date range of interest
    set @thruDate = '{date_thru}' -- End of date range of interest
    set @impute_duration = {impute_duration}; -- in minutes. Use this to fill in NULL entry or exit. Represents the median length of stay. Is a best guess. 
    
    -- All CTEs must be at the same level in SQL Server
    WITH TicketsWithActivity AS (
            SELECT DISTINCT TicketNumber
            FROM Opms.dbo.Transactions
            WHERE Id_parking = @Id_Parking
              AND TransactionType IN (20, 30, 37, 31, 33, 40) -- added 31, 33, 40 on 10/15/25 to correct issue with never completing some over night visits
              AND TransactionDateStamp >= @fromDate AND TransactionDateStamp < @thruDate
              AND TicketNumber NOT IN (0, 31999, 410000000, 1100000000, 220000000,  610000000, 250000000, 810000000, 710000000)
        ),
        
        -- Step 2: Latest Entry (TransactionType = 20) for each TicketNumber
        LatestEntry AS (
            SELECT t.*
            FROM TicketsWithActivity a
            CROSS APPLY (
                SELECT TOP 1 *
                FROM Opms.dbo.Transactions
                WHERE TicketNumber = a.TicketNumber
                  AND TransactionType = 20
                  AND Id_parking = @Id_Parking
                  AND TransactionDateStamp >= DATEADD(week, -1, @fromDate) AND TransactionDateStamp < @thruDate -- can look for entries up to one week prior to fromDate but don't want an entry after thruDate
                ORDER BY CONVERT(DATETIME, CONVERT(VARCHAR, CAST(TransactionDateStamp AS DATE), 120) + ' ' + TransactionTimeStamp) DESC
            ) t
        ),
        
        -- Step 3: Latest Exit (TransactionType = 30) for each TicketNumber
        LatestExit AS (
            SELECT t.*
            FROM TicketsWithActivity a
            CROSS APPLY (
                SELECT TOP 1 *
                FROM Opms.dbo.Transactions
                WHERE TicketNumber = a.TicketNumber
                  AND TransactionType = 30
                  AND Id_parking = @Id_Parking
                  AND TransactionDateStamp >= @fromDate AND TransactionDateStamp < DATEADD(week, 1, @thruDate) -- can look for exits up to week after the thruDate but don't want an exit before the fromDate
                ORDER BY CONVERT(DATETIME, CONVERT(VARCHAR, CAST(TransactionDateStamp AS DATE), 120) + ' ' + TransactionTimeStamp) ASC
            ) t
        ),
        
        -- Step 4: Most recent failed or manual exit (TransactionType IN 31/33/37/40)
        LatestFailedOrManual AS (
            SELECT t.*
            FROM TicketsWithActivity a
            CROSS APPLY (
                SELECT TOP 1 *
                FROM Opms.dbo.Transactions
                WHERE TicketNumber = a.TicketNumber
                  AND TransactionType IN (31, 33, 37, 40) -- may want to include 37
                  AND Id_parking = @Id_Parking
                  AND TransactionDateStamp >= @fromDate AND TransactionDateStamp < DATEADD(week, 1, @thruDate) -- can look for exits up to week after the thruDate but don't want an exit before the fromDate
                ORDER BY CONVERT(DATETIME, CONVERT(VARCHAR, CAST(TransactionDateStamp AS DATE), 120) + ' ' + TransactionTimeStamp) ASC 
            ) t
        ),
        
        -- Step 5: Bring in movements from SPM History in order to identify if parker is a permit holder
        PermitHolderMovements AS (
            -- First gather data from permit holders who are no longer present but were at some point during the date range used
        	SELECT 
        		spm.iParkingId, pa.ParkingName, spm.lVirtualTicketNumber TicketNumber, spm.dtEntryDate, spm.sEntryTime, spm.dtExitDate, spm.sExitTime, 
        		sMediaNumber, c.sName CardHolder, g.sName GroupName, cu.sName CustomerName, cu.lId CustomerID, cAmountPaid, iEntryLocationId, iExitLocationId, eState
        	
        	FROM Opms.dbo.PresenceSpmHistory spm 
        	INNER JOIN Opms.dbo.ParkingAdmin pa On (spm.iParkingId=pa.Id_Parking)
        	LEFT JOIN ECounting.dbo.SPMCards c On (spm.sMediaNumber=c.sNumber)
        	LEFT JOIN Ecounting.dbo.SPMGroups g On (c.lGroupID=g.lId)
        	LEFT JOIN Ecounting.dbo.SPMCustomers cu On (g.lCustomerId=cu.lId)
        	WHERE
        		spm.iParkingId = @Id_Parking
        		AND lVirtualTicketNumber != 0
        		AND ( -- Finds all permit visits that were present during any part of the date range
        			CONVERT(DATETIME, CONVERT(VARCHAR, CAST(dtEntryDate AS DATE), 120) + ' ' + sEntryTime) < @thruDate
        			AND CONVERT(DATETIME, CONVERT(VARCHAR, CAST(dtExitDate AS DATE), 120) + ' ' + sExitTime) >= @fromDate
        		)
        		AND CONVERT(DATETIME, CONVERT(VARCHAR, CAST(dtEntryDate AS DATE), 120) + ' ' + sEntryTime) != CONVERT(DATETIME, CONVERT(VARCHAR, CAST(dtExitDate AS DATE), 120) + ' ' + sExitTime)
        		--AND dtEntryDate >= @fromDate and dtEntryDate < @thruDate -- Finds any permit entries from the date range
        	UNION
        	-- This section brings in any present permit holders according to PresenceSPM
        	-- There is some bad data in this table. eState = 2 seems to be bad data where the entry/exit is out of whack
        	-- I also found some very old records that can't be true. It seems that when we delete a card from the system it can leave these abandoned records. Tying to current customers forces the table to drop those old/erroneous records.
        	SELECT 
        		spm.iParkingId, pa.ParkingName, spm.lVirtualTicketNumber TicketNumber, spm.dtEntryDate, spm.sEntryTime, NULL ExitDate, NULL sExitTime,
        		sMediaNumber, c.sName CardHolder, g.sName GroupName, cu.sName CustomerName, cu.lId CustomerID, cAmountPaid, iEntryLocationId, iExitLocationId, eState
        	
        	FROM Opms.dbo.PresenceSpm spm
        	INNER JOIN Opms.dbo.ParkingAdmin pa On (spm.iParkingId=pa.Id_Parking)
        	LEFT JOIN ECounting.dbo.SPMCards c On (spm.sMediaNumber=c.sNumber)
        	INNER JOIN Ecounting.dbo.SPMGroups g On (c.lGroupID=g.lId)
        	INNER JOIN Ecounting.dbo.SPMCustomers cu On (g.lCustomerId=cu.lId)
        	WHERE
        		spm.iParkingId = @Id_Parking
        		AND spm.lVirtualTicketNumber != 0
        		AND spm.eState != 2
                AND spm.dtEntryDate < @thruDate -- Don't need to bring in all present permit holders if date range is in the past
        ),
        
        -- Step 6: Combine results with accurate status classification. This is where we get some attributes for the records that were selected
        PenultimateStep As (
            SELECT
                -- For many, first try the value from the entry, then the exit (30), then another exit
                COALESCE(e.Id_Parking, x.Id_Parking, fm.Id_Parking, spm.iParkingId) Id_Parking,
    			COALESCE(pa.ParkingName, spm.ParkingName) Garage,
                CASE 
    				WHEN e.TicketNumber IS NOT NULL AND (x.TicketNumber IS NOT NULL OR fm.TicketNumber IS NOT NULL) THEN 'Complete'
    				WHEN spm.TicketNumber IS NOT NULL AND spm.dtExitDate IS NOT NULL THEN 'Complete'
                    WHEN spm.TicketNumber IS NOT NULL AND spm.dtExitDate IS NULL THEN 'Entry Only - Permit Holder' -- Added this changed classification so further parts of the query wouldn't automatically impute. These are customers known to still be present
    				WHEN e.TicketNumber IS NOT NULL AND x.TicketNumber IS NULL AND fm.TicketNumber IS NULL THEN 'Entry Only'
    				WHEN (x.TicketNumber IS NOT NULL OR fm.TicketNumber IS NOT NULL) AND e.TicketNumber IS NULL THEN 'Exit Only'
    				ELSE 'Unknown'
    			END AS status,
                CONVERT(DATETIME, CONVERT(VARCHAR, CAST(spm.dtEntryDate AS DATE), 120) + ' ' + spm.sEntryTime) spmEntryDate, 
    			CONVERT(DATETIME, CONVERT(VARCHAR, CAST(spm.dtExitDate AS DATE), 120) + ' ' + spm.sExitTime) spmExitDate, 
    			spm.TicketNumber As TicketNumberSPM,
    			COALESCE(e.TicketNumber, x.TicketNumber, fm.TicketNumber) AS TicketNumber,
    			COALESCE(
    				CONVERT(DATETIME, CONVERT(VARCHAR, CAST(spm.dtEntryDate AS DATE), 120) + ' ' + spm.sEntryTime),
    				CONVERT(DATETIME, CONVERT(VARCHAR, CAST(e.TransactionDateStamp AS DATE), 120) + ' ' + e.TransactionTimeStamp)
    			) As EntryDate,
    			e.TransactionDateStamp,
    			e.TransactionTimeStamp,
    			COALESCE(
    				CONVERT(DATETIME, CONVERT(VARCHAR, CAST(spm.dtExitDate AS DATE), 120) + ' ' + spm.sExitTime),
    				CONVERT(DATETIME, CONVERT(VARCHAR, CAST(x.TransactionDateStamp AS DATE), 120) + ' ' + x.TransactionTimeStamp)			
    			) AS ExitDate,
    			COALESCE(
    				CONVERT(DATETIME, CONVERT(VARCHAR, CAST(fm.TransactionDateStamp AS DATE), 120) + ' ' + fm.TransactionTimeStamp), 
    				CONVERT(DATETIME, CONVERT(VARCHAR, CAST(spm.dtExitDate AS DATE), 120) + ' ' + spm.sExitTime)
    			) AS ExitAttemptDate,
    			CASE
    				--WHEN e.TransactionDateStamp IS NOT NULL THEN CONVERT(DATETIME, CONVERT(VARCHAR, CAST(e.TransactionDateStamp AS DATE), 120) + ' ' + e.TransactionTimeStamp)
    				WHEN e.TransactionDateStamp IS NULL AND x.TransactionDate > '2000-01-01' THEN CONVERT(DATETIME, CONVERT(VARCHAR, CAST(x.TransactionDate AS DATE), 120) + ' ' + x.TransactionTime)
    				WHEN e.TransactionDateStamp IS NULL AND fm.TransactionDate > '2000-01-01' THEN CONVERT(DATETIME, CONVERT(VARCHAR, CAST(fm.TransactionDate AS DATE), 120) + ' ' + fm.TransactionTime)
    				WHEN e.TransactionDateStamp IS NULL AND x.TransactionDate = '2000-01-01' AND fm.TransactionDate = '2000-01-01' THEN DATEADD(MINUTE, -@impute_duration, CONVERT(DATETIME, CONVERT(VARCHAR, CAST(x.TransactionDateStamp AS DATE), 120) + ' ' + x.TransactionTimeStamp))
    			END As EntryDate2,
    
    			CONVERT(DATETIME, CONVERT(VARCHAR, CAST(x.TransactionDate AS DATE), 120) + ' ' + x.TransactionTime) ExitOtherTimeStamp,
    			CONVERT(DATETIME, CONVERT(VARCHAR, CAST(fm.TransactionDate AS DATE), 120) + ' ' + fm.TransactionTime) ExitAttemptOtherTimeStamp,
    
    			e.TransactionType AS TransactionType_entry,
    			x.TransactionType AS TransactionType_exit, 
    			fm.TransactionType AS TransactionType_exit_attempt,
    
    			e.TicketType EntryTicketType,
    			CASE
    				WHEN e.TicketType = -1 THEN 'lost ticket'
    				WHEN e.TicketType = -11 THEN 'manual ticket'
    				WHEN e.TicketType = 0 THEN 'KP'
    				WHEN e.TicketType = 1 THEN 'value card'
    				WHEN e.TicketType = 2 THEN 'normal DP'
    				WHEN e.TicketType = 3 THEN 'congress'
    				WHEN e.TicketType = 4 THEN 'visitor'
    				WHEN e.TicketType = 5 THEN 'fixed time per day'
    				WHEN e.TicketType = 7 THEN 'fixed number of entries'
    				WHEN e.TicketType = 8 THEN 'pool car'
    				ELSE NULL
    			END As EntryTicketTypeDesc,
    			COALESCE(x.TicketType, fm.TicketType) ExitTicketType,
    			CASE
    				WHEN x.TicketType = -1 THEN 'lost ticket'
    				WHEN x.TicketType = -11 THEN 'manual ticket'
    				WHEN x.TicketType = 0 THEN 'KP'
    				WHEN x.TicketType = 1 THEN 'value card'
    				WHEN x.TicketType = 2 THEN 'normal DP'
    				WHEN x.TicketType = 3 THEN 'congress'
    				WHEN x.TicketType = 4 THEN 'visitor'
    				WHEN x.TicketType = 5 THEN 'fixed time per day'
    				WHEN x.TicketType = 7 THEN 'fixed number of entries'
    				WHEN x.TicketType = 8 THEN 'pool car'
    				WHEN x.TicketType IS NULL THEN CASE 
    												WHEN fm.TicketType = -1 THEN 'lost ticket'
    												WHEN fm.TicketType = -11 THEN 'manual ticket'
    												WHEN fm.TicketType = 0 THEN 'KP'
    												WHEN fm.TicketType = 1 THEN 'value card'
    												WHEN fm.TicketType = 2 THEN 'normal DP'
    												WHEN fm.TicketType = 3 THEN 'congress'
    												WHEN fm.TicketType = 4 THEN 'visitor'
    												WHEN fm.TicketType = 5 THEN 'fixed time per day'
    												WHEN fm.TicketType = 7 THEN 'fixed number of entries'
    												WHEN fm.TicketType = 8 THEN 'pool car'
    												WHEN fm.TicketType IS NULL THEN NULL
    												ELSE 'unknown'
    											END
    				ELSE 'unknown'
    			END As ExitTicketTypeDesc,
    			CASE
    				WHEN e.TransactionDateStamp IS NULL AND COALESCE(x.TicketType, fm.TicketType) = -1 AND COALESCE(x.Amount, fm.Amount) = 0 AND COALESCE(x.T2Amount, fm.T2Amount) = 3000 AND COALESCE(x.IdCashier, fm.IdCashier) = 0 THEN 'lost ticket: POF' -- calc entry using best guess
    				WHEN e.TransactionDateStamp IS NULL AND COALESCE(x.TicketType, fm.TicketType) = -1 AND COALESCE(x.Amount, fm.Amount) != 3000 AND COALESCE(x.IdCashier, fm.IdCashier) = 0 THEN 'lost ticket: help line' -- calc entry from best guess
    				WHEN e.TransactionDateStamp IS NULL AND COALESCE(x.TicketType, fm.TicketType) = -1 AND COALESCE(x.Amount, fm.Amount) = 3000 AND COALESCE(x.IdCashier, fm.IdCashier) != 0 THEN 'lost ticket: cashier' --calc entry from best guess
    				WHEN e.TransactionDateStamp IS NULL AND COALESCE(x.TicketType, fm.TicketType) = -1 AND COALESCE(x.Amount, fm.Amount) != 3000 AND COALESCE(x.IdCashier, fm.IdCashier) != 0 THEN 'manual: cashier' -- calc entry from OtherTimeStamp or Amount
    				WHEN e.TransactionDateStamp IS NULL AND COALESCE(x.TicketType, fm.TicketType) = 0 AND COALESCE(x.Parking, fm.Parking) = 0 AND COALESCE(x.Amount, fm.Amount) = 3000 AND COALESCE(x.IdCashier, fm.IdCashier) = 0 THEN 'lost ticket: help line' -- calc entry from best guess
    				WHEN e.TransactionDateStamp IS NULL AND SUBSTRING(CAST(COALESCE(x.TicketNumber, fm.TicketNumber) AS VARCHAR(9)),3,1) = 9 THEN 'permit: no entry found'
    				WHEN e.TransactionDateStamp IS NULL AND COALESCE(x.TicketType, fm.TicketType) = -11 AND COALESCE(x.IdCashier, fm.IdCashier) != 0 THEN 'manual: cashier' -- calc entry from OtherTimeStamp or Amount
    				WHEN e.TransactionDateStamp IS NULL AND COALESCE(x.TicketType, fm.TIcketType) = 0 AND COALESCE(x.IdCashier, fm.IdCashier) = 0 THEN 'manual: help line' -- calc entry from OtherTimeStamp
    				WHEN e.TransactionDateStamp IS NULL AND SUBSTRING(CAST(COALESCE(x.TicketNumber, fm.TicketNumber) AS VARCHAR(9)),3,3) = '000' AND x.TransactionType = 30 AND spm.sMediaNumber IS NOT NULL THEN 'permit: manual help line exit'
    				WHEN e.TransactionDateStamp IS NULL AND spm.sMediaNumber IS NOT NULL AND COALESCE(x.Parking, fm.Parking) = 1 AND x.TransactionType = 30 THEN 'permit: manual exit'
    				WHEN e.TransactionDateStamp IS NOT NULL AND (x.TransactionDateStamp IS NOT NULL OR fm.TransactionDateStamp IS NOT NULL OR spm.dtExitDate IS NOT NULL) THEN NULL
                    WHEN spm.dtEntryDate IS NOT NULL AND spm.dtExitDate IS NULL THEN NULL -- applies to present permit holders
    				WHEN spm.dtEntryDate IS NOT NULL AND spm.dtExitDate IS NOT NULL THEN NULL
    				WHEN e.TransactionDateStamp IS NOT NULL AND x.TransactionDateStamp IS NULL AND fm.TransactionDateStamp IS NULL AND spm.dtExitDate IS NULL THEN 'imputed' -- Do I want to have something here? Simply means status is Entry Only
    				ELSE NULL
    			END As exit_classification,
    	
    			e.Parking Parking_entry,
    			x.Parking Parking_exit, 
    			fm.Parking Parking_exit_attempt,
    	
    			COALESCE(x.Amount, fm.Amount)/100. Amount, 
    			COALESCE(x.T2Amount, fm.T2Amount)/100. T2Amount, 
    	
    			COALESCE(x.IdCashier, fm.IdCashier) IdCashier, 
    			COALESCE(x.IdShift, fm.IdShift) IdShift,	
    
    			e.T2StationAdddress EntryStation,
    			x.T2StationAdddress ExitStation, 
    			fm.T2StationAdddress ExitAttemptStation,
    			spm.sMediaNumber,
    			spm.CardHolder, 
    			spm.GroupName, 
    			spm.CustomerName, 
    			spm.CustomerID, 
    			CASE
    				WHEN spm.sMediaNumber IS NULL THEN 'transient'
    				WHEN spm.sMediaNumber IS NOT NULL AND spm.CustomerID = 929 THEN 'employee'
    				ELSE 'permit'
    			END As customer_type
    
            FROM TicketsWithActivity a
            LEFT JOIN LatestEntry e ON a.TicketNumber = e.TicketNumber
            LEFT JOIN LatestExit x ON a.TicketNumber = x.TicketNumber
            LEFT JOIN LatestFailedOrManual fm ON a.TicketNumber = fm.TicketNumber
            LEFT JOIN Opms.dbo.ParkingAdmin pa On (COALESCE(e.Id_Parking, x.Id_Parking, fm.Id_Parking)=pa.Id_Parking)
            FULL OUTER JOIN PermitHolderMovements spm On (a.TicketNumber = spm.TicketNumber AND e.TransactionDateStamp=spm.dtEntryDate AND e.TransactionTimeStamp=spm.sEntryTime
                                                    OR a.TicketNumber = spm.TicketNumber AND x.TransactionDateStamp=spm.dtExitDate AND x.TransactionTimeStamp=spm.sExitTime)
    ),
    
    -- Final BatchData CTE that wraps everything
    BatchData AS (
        SELECT 
    		CASE
    			WHEN status = 'Exit Only' AND EntryDate2 > ExitDate THEN CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(Id_Parking, '_', COALESCE(TicketNumber, TicketNumberSPM), '_', ISNULL(CONVERT(VARCHAR, DATEADD(MINUTE, -@impute_duration, ExitDate), 121), 'NULL'))), 2)
    			WHEN status = 'Exit Only' AND DATEDIFF(DAY, EntryDate2, ExitDate) > 8 THEN  CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(Id_Parking, '_', COALESCE(TicketNumber, TicketNumberSPM), '_', ISNULL(CONVERT(VARCHAR, DATEADD(MINUTE, -@impute_duration, ExitDate), 121), 'NULL'))), 2) -- Added this line. A transient vehicle should not be in the garage for more than 2 weeks. There is data with an exit only that has a EntryDate2 that was 7 years earlier.
    			ELSE CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(Id_Parking, '_', COALESCE(TicketNumber, TicketNumberSPM), '_', ISNULL(CONVERT(VARCHAR, COALESCE(EntryDate, EntryDate2, spmEntryDate), 121), 'NULL'))), 2)
    		END As TripDetailID,
            Id_Parking as GarageID,
            Garage,
            status,
            COALESCE(TicketNumber, TicketNumberSPM) TicketNumber, -- Ticket Number	
    		-- Calculate the final EntryDate --
            CASE
                WHEN status = 'Exit Only' AND EntryDate2 > ExitDate THEN DATEADD(MINUTE, -@impute_duration, ExitDate)
    			WHEN status = 'Exit Only' AND DATEDIFF(DAY, EntryDate2, ExitDate) > 8 THEN DATEADD(MINUTE, -@impute_duration, ExitDate)
                ELSE COALESCE(EntryDate, EntryDate2, spmEntryDate)
            END As EntryDate,
            -- Calculate the final ExitDate --
    		CASE
                -- May need to look at this again and make it a longer window. If etl is run in the morning, are entries from just before midnight actually out? 
                WHEN status = 'Entry Only' AND EntryDate >= CONCAT(SUBSTRING(CONVERT(VARCHAR, GETDATE(), 120),1,10),' 00:00:00') THEN NULL -- if EntryDate is same day and status is Entry Only, leave exit blank
                
                WHEN status = 'Entry Only' THEN DATEADD(MINUTE, @impute_duration, EntryDate) -- otherwise impute ExitDate
                ELSE COALESCE(ExitDate, ExitAttemptDate, spmExitDate) 
            END As ExitDate,
            CASE
                WHEN status = 'Exit Only' AND EntryDate2 > ExitDate THEN @impute_duration
    			WHEN status = 'Exit Only' AND DATEDIFF(DAY, EntryDate2, ExitDate) > 8 THEN @impute_duration
                WHEN status = 'Entry Only' THEN @impute_duration
                ELSE DATEDIFF(second, COALESCE(EntryDate, EntryDate2, spmEntryDate), COALESCE(ExitDate, ExitAttemptDate, spmExitDate))/60. 
            END As length_of_stay,
            CASE
                WHEN status = 'Exit Only' AND EntryDate2 > ExitDate THEN @impute_duration
    			WHEN status = 'Exit Only' AND DATEDIFF(DAY, EntryDate2, ExitDate) > 8 THEN @impute_duration
                WHEN status = 'Entry Only' THEN @impute_duration
                WHEN status = 'Exit Only' AND COALESCE(ExitOtherTimeStamp, ExitAttemptOtherTimeStamp) = '2000-01-01 00:00:00' THEN @impute_duration
                ELSE NULL
            END AS impute_duration,
            customer_type,
            exit_classification,
            COALESCE(TransactionType_exit, TransactionType_exit_attempt) TransactionType_exit,
            EntryTicketType TicketType_entry,
            EntryTicketTypeDesc TicketTypeDesc_entry,
            ExitTicketType TicketType_exit,
            ExitTicketTypeDesc TicketTypeDesc_exit,
            Parking_entry,
            COALESCE(Parking_exit, Parking_exit_attempt) Parking_exit,
            Amount,
            T2Amount,
            IdCashier,
            IdShift,
            EntryStation,
            ExitStation, 
    		ExitAttemptStation,
            sMediaNumber As PermitNumber,
            CardHolder As PermitName,
            GroupName,
            CustomerName,
            CustomerID,
            'ZMS' As SourceSystem,
            GETDATE() As LoadDate
    
        FROM PenultimateStep
    	WHERE (TicketNumber IS NOT NULL OR TicketNumberSPM IS NOT NULL)
    )