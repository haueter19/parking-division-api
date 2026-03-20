-- ============================================================
-- Parking Garage Visit Report
-- Pairs entry/exit transactions into complete visits.
--
-- Sources:
--   Permit visits  -> PresenceSpmHistory (closed visits)
--                  -> PresenceSpm        (currently parked)
--   KP visits      -> Transactions self-join (type 20 + 30/32)
--   Payment detail -> Payments table
--
-- Parameters: @start_date, @end_date, @parking_id
-- ============================================================

DECLARE @start_date DATE     = '2026-03-03';
DECLARE @end_date   DATE     = '2026-03-10';
DECLARE @parking_id SMALLINT = 2;

-- Widen entry window 48 h before range start so multi-day
-- stays that began just before the range are still captured.
DECLARE @entry_floor DATETIME = DATEADD(hour, -48, CAST(@start_date AS DATETIME));
DECLARE @exit_ceil   DATETIME = DATEADD(day,   1,  CAST(@end_date   AS DATETIME));

WITH

-- ----------------------------------------------------------
-- 1. PERMIT VISITS
--    PresenceSpmHistory : closed visits (eState 3 or 4)
--       eState 3 = entry + exit present
--       eState 4 = lost exit (entry only, vehicle never exited cleanly)
--       eState 5 = EXCLUDED — lost entry; sync issue (permit holder
--                  pulled a KP ticket or got a manual gate raise)
--    PresenceSpm        : open visits (eState 1 = currently parked)
--       INNER JOINs to ECounting ensure abandoned/deleted cards are
--       excluded — when a card is removed from the system it can leave
--       orphaned PresenceSpm records.
-- ----------------------------------------------------------
permit_visits AS (

    -- Closed visits
    SELECT
        p.lId                                           AS presence_id,
        p.lVirtualTicketNumber                          AS ticket_number,
        p.sMediaNumber                                  AS card_number,
        p.iParkingId,
        p.iEntryLocationId                              AS entry_location_id,
        p.iExitLocationId                               AS exit_location_id,
        p.eState,
        DATEADD(second,
            DATEDIFF(second, '00:00:00', CAST(p.sEntryTime AS TIME)),
            CAST(p.dtEntryDate AS DATETIME))            AS entry_dt,
        CASE p.eState
            WHEN 3 THEN
                DATEADD(second,
                    DATEDIFF(second, '00:00:00', CAST(p.sExitTime AS TIME)),
                    CAST(p.dtExitDate AS DATETIME))
            ELSE NULL
        END                                             AS exit_dt,
        p.cCalculatedCosts                              AS calculated_cost,
        p.cAmountPaid + p.cAmountPaidTagPayment         AS amount_paid
    FROM OPMS.dbo.PresenceSpmHistory p
    WHERE p.iParkingId  = @parking_id
      AND p.eState     IN (3, 4)
      AND p.lVirtualTicketNumber <> 0       -- offline presences have no Transactions link
      AND p.dtExitDate >= @start_date
      AND p.dtEntryDate < @exit_ceil
      -- Guard against bad data rows where entry and exit are identical
      AND NOT (p.eState = 3
               AND p.dtEntryDate = p.dtExitDate
               AND p.sEntryTime  = p.sExitTime)

    UNION ALL

    -- Open visits (vehicle still in garage)
    -- INNER JOINs to ECounting drop orphaned records from deleted cards.
    SELECT
        p.lId                                           AS presence_id,
        p.lVirtualTicketNumber                          AS ticket_number,
        p.sMediaNumber                                  AS card_number,
        p.iParkingId,
        p.iEntryLocationId                              AS entry_location_id,
        NULL                                            AS exit_location_id,
        p.eState,
        DATEADD(second,
            DATEDIFF(second, '00:00:00', CAST(p.sEntryTime AS TIME)),
            CAST(p.dtEntryDate AS DATETIME))            AS entry_dt,
        NULL                                            AS exit_dt,
        p.cCalculatedCosts                              AS calculated_cost,
        p.cAmountPaid + p.cAmountPaidTagPayment         AS amount_paid
    FROM OPMS.dbo.PresenceSpm p
    INNER JOIN ECounting.dbo.SPMCards     c  ON c.sNumber      = p.sMediaNumber
    INNER JOIN ECounting.dbo.SPMGroups    g  ON g.lId          = c.lGroupID
    INNER JOIN ECounting.dbo.SPMCustomers cu ON cu.lId         = g.lCustomerId
    WHERE p.iParkingId  = @parking_id
      AND p.eState      = 1             -- clean "currently parked"; eState=2 is bad data
      AND p.lVirtualTicketNumber <> 0
      AND p.dtEntryDate >= @entry_floor
      AND p.dtEntryDate <  @exit_ceil
),

-- ----------------------------------------------------------
-- 2. DISTINCT KP TICKET NUMBERS ACTIVE IN DATE RANGE
--    Driving from DISTINCT ticket numbers (not individual
--    transaction rows) prevents duplicates when the same
--    ticket number appears as type 20 more than once in the
--    window (recycled or double-recorded entries).
--    Excludes permit card virtual ticket numbers and known
--    invalid/sentinel ticket numbers.
-- ----------------------------------------------------------
kp_tickets AS (
    SELECT DISTINCT
        t.TicketNumber,
        t.Id_Parking
    FROM OPMS.dbo.Transactions t
    WHERE t.Id_Parking          = @parking_id
      AND t.TransactionType    IN (20, 30, 32, 33, 37, 40)
      AND t.TransactionDateStamp >= @entry_floor
      AND t.TransactionDateStamp <  @exit_ceil
      -- Known invalid/sentinel ticket numbers
      AND t.TicketNumber NOT IN (0, 31999, 410000000, 1100000000,
                                 220000000, 610000000, 250000000,
                                 810000000, 710000000)
      -- Exclude permit card virtual ticket numbers (covered by permit_visits)
      AND NOT EXISTS (
            SELECT 1
            FROM OPMS.dbo.PresenceSpmHistory ph
            WHERE ph.iParkingId           = @parking_id
              AND ph.lVirtualTicketNumber  = t.TicketNumber
              AND ph.lVirtualTicketNumber <> 0
          )
      AND NOT EXISTS (
            SELECT 1
            FROM OPMS.dbo.PresenceSpm ps
            WHERE ps.iParkingId           = @parking_id
              AND ps.lVirtualTicketNumber  = t.TicketNumber
              AND ps.lVirtualTicketNumber <> 0
          )
),

-- ----------------------------------------------------------
-- 3. KP VISITS
--    One row per distinct ticket number.
--    Entry  : most recent type-20 in the window.
--    Exit   : earliest type-30/32 after entry, within 48 h.
--    Payment: first payment record after entry, within 48 h.
--    T2StationAdddress is used for station labels directly
--    from the transaction row, avoiding a join to Location
--    which can fan out if it has one row per parking per
--    location ID.
-- ----------------------------------------------------------
kp_visits AS (
    SELECT
        k.TicketNumber,
        k.Id_Parking,
        e.entry_dt,
        e.entry_station,
        x.exit_dt,
        x.exit_station,
        CASE WHEN x.exit_dt IS NOT NULL THEN 3 ELSE 4 END AS eState,
        COALESCE(pay.calculated_cost, 0)            AS calculated_cost,
        COALESCE(pay.amount_paid,     0)            AS amount_paid
    FROM kp_tickets k

    -- Most recent entry for this ticket in the window
    OUTER APPLY (
        SELECT TOP 1
            t.TransactionDateStamp  AS entry_dt,
            t.T2StationAdddress     AS entry_station
        FROM OPMS.dbo.Transactions t
        WHERE t.TicketNumber        = k.TicketNumber
          AND t.Id_Parking          = k.Id_Parking
          AND t.TransactionType     = 20
          AND t.TransactionDateStamp >= @entry_floor
          AND t.TransactionDateStamp <  @exit_ceil
        ORDER BY t.TransactionDateStamp DESC
    ) e

    -- Earliest successful exit after entry, within 48 h
    OUTER APPLY (
        SELECT TOP 1
            t.TransactionDateStamp  AS exit_dt,
            t.T2StationAdddress     AS exit_station
        FROM OPMS.dbo.Transactions t
        WHERE t.TicketNumber        = k.TicketNumber
          AND t.Id_Parking          = k.Id_Parking
          AND t.TransactionType    IN (30, 32)
          AND t.TransactionDateStamp >= COALESCE(e.entry_dt, @entry_floor)
          AND t.TransactionDateStamp <= DATEADD(hour, 48, COALESCE(e.entry_dt, @entry_floor))
        ORDER BY t.TransactionDateStamp ASC
    ) x

    -- First payment for this ticket after entry, within 48 h
    -- AmountPaid is after validations/discounts; Amount is gross calculated.
    OUTER APPLY (
        SELECT TOP 1
            py.Amount               AS calculated_cost,
            py.AmountPaid           AS amount_paid
        FROM OPMS.dbo.Payments py
        WHERE py.TicketNumber = k.TicketNumber
          AND py.Id_Parking   = k.Id_Parking
          AND DATEADD(second,
                DATEDIFF(second, '00:00:00', CAST(py.Time AS TIME)),
                CAST(py.Date AS DATETIME)) >= COALESCE(e.entry_dt, @entry_floor)
          AND DATEADD(second,
                DATEDIFF(second, '00:00:00', CAST(py.Time AS TIME)),
                CAST(py.Date AS DATETIME)) <= DATEADD(hour, 48, COALESCE(e.entry_dt, @entry_floor))
        ORDER BY DATEADD(second,
                     DATEDIFF(second, '00:00:00', CAST(py.Time AS TIME)),
                     CAST(py.Date AS DATETIME)) ASC
    ) pay

    -- Drop pre-window entries that never produced an exit in the range
    WHERE COALESCE(x.exit_dt, e.entry_dt) >= @start_date
),

-- ----------------------------------------------------------
-- FINAL OUTPUT
-- ----------------------------------------------------------
-- NOTE: entry_location and exit_location for PERMIT rows are
-- raw integer IDs from PresenceSpmHistory. To resolve them to
-- names, join OPMS.dbo.Location on Id_Location, but first
-- verify whether Location has one row per (Id_Location,
-- Id_Parking) or just per Id_Location -- a 1:many join here
-- is what caused duplicates in the previous version.
-- KP rows use T2StationAdddress from the transaction directly
-- (e.g. "E22", "A24") so no Location join is needed there.
-- ----------------------------------------------------------
SELECT
    'PERMIT'                                        AS visit_type,
    v.iParkingId,
    CAST(v.presence_id AS VARCHAR(20))              AS visit_key,
    v.ticket_number,
    v.card_number,
    v.entry_dt,
    CAST(v.entry_location_id AS VARCHAR(10))        AS entry_location,
    v.exit_dt,
    CAST(v.exit_location_id AS VARCHAR(10))         AS exit_location,
    DATEDIFF(minute, v.entry_dt, v.exit_dt)         AS duration_minutes,
    v.calculated_cost,
    v.amount_paid,
    CASE v.eState
        WHEN 1 THEN 'Currently Parked'
        WHEN 3 THEN 'Paired'
        WHEN 4 THEN 'Lost Exit'
    END                                             AS pair_status
FROM permit_visits v

UNION ALL

SELECT
    'KP'                                            AS visit_type,
    v.Id_Parking,
    CAST(v.TicketNumber AS VARCHAR(20))             AS visit_key,
    v.TicketNumber,
    NULL                                            AS card_number,
    v.entry_dt,
    v.entry_station                                 AS entry_location,
    v.exit_dt,
    v.exit_station                                  AS exit_location,
    DATEDIFF(minute, v.entry_dt, v.exit_dt)         AS duration_minutes,
    v.calculated_cost,
    v.amount_paid,
    CASE v.eState
        WHEN 3 THEN 'Paired'
        WHEN 4 THEN 'No Exit Found'
    END                                             AS pair_status
FROM kp_visits v

ORDER BY entry_dt;
