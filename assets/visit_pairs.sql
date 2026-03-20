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
--       No exit date yet; exit_dt returned as NULL.
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
      AND p.dtExitDate >= @start_date
      AND p.dtEntryDate < @exit_ceil

    UNION ALL

    -- Open visits (vehicle still in garage)
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
    WHERE p.iParkingId  = @parking_id
      AND p.eState      = 1
      AND p.dtEntryDate >= @entry_floor
      AND p.dtEntryDate <  @exit_ceil
),

-- ----------------------------------------------------------
-- 2. KP (TICKET) ENTRIES
--    Exclude any ticket number that is a permit card virtual
--    ticket number so we do not double-count those visits.
-- ----------------------------------------------------------
kp_entries AS (
    SELECT
        t.TransactionDateStamp  AS entry_dt,
        t.TicketNumber,
        t.Id_Parking,
        t.Id_Location           AS entry_location_id
    FROM OPMS.dbo.Transactions t
    WHERE t.TransactionType    = 20
      AND t.Id_Parking         = @parking_id
      AND t.TransactionDateStamp >= @entry_floor
      AND t.TransactionDateStamp <  @exit_ceil
      AND NOT EXISTS (
            SELECT 1
            FROM OPMS.dbo.PresenceSpmHistory ph
            WHERE ph.iParkingId           = @parking_id
              AND ph.lVirtualTicketNumber  = t.TicketNumber
              AND ph.lVirtualTicketNumber <> 0
          )
      -- Also exclude ticket numbers belonging to currently-parked permit holders
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
--    For each entry, find the nearest type-30/32 exit within
--    48 hours and the most recent payment from Payments.
--    Uses OUTER APPLY so unmatched entries still appear
--    (pair_status = 'No Exit Found').
-- ----------------------------------------------------------
kp_visits AS (
    SELECT
        e.TicketNumber,
        e.Id_Parking,
        e.entry_dt,
        e.entry_location_id,
        x.exit_dt,
        x.exit_location_id,
        CASE WHEN x.exit_dt IS NOT NULL THEN 3 ELSE 4 END AS eState,
        COALESCE(pay.Amount,     0)                 AS calculated_cost,
        COALESCE(pay.AmountPaid, 0)                 AS amount_paid
    FROM kp_entries e

    -- Nearest successful exit within 48 h
    OUTER APPLY (
        SELECT TOP 1
            t.TransactionDateStamp  AS exit_dt,
            t.Id_Location           AS exit_location_id
        FROM OPMS.dbo.Transactions t
        WHERE t.TicketNumber        = e.TicketNumber
          AND t.Id_Parking          = e.Id_Parking
          AND t.TransactionType    IN (30, 32)
          AND t.TransactionDateStamp >= e.entry_dt
          AND t.TransactionDateStamp <= DATEADD(hour, 48, e.entry_dt)
        ORDER BY t.TransactionDateStamp ASC
    ) x

    -- Payment record for this ticket (pay-on-foot or pay-at-exit)
    -- AmountPaid is after validations/discounts; Amount is gross calculated.
    OUTER APPLY (
        SELECT TOP 1
            py.Amount,
            py.AmountPaid
        FROM OPMS.dbo.Payments py
        WHERE py.TicketNumber = e.TicketNumber
          AND py.Id_Parking   = e.Id_Parking
          AND DATEADD(second,
                DATEDIFF(second, '00:00:00', CAST(py.Time AS TIME)),
                CAST(py.Date AS DATETIME)) >= e.entry_dt
          AND DATEADD(second,
                DATEDIFF(second, '00:00:00', CAST(py.Time AS TIME)),
                CAST(py.Date AS DATETIME)) <= DATEADD(hour, 48, e.entry_dt)
        ORDER BY DATEADD(second,
                     DATEDIFF(second, '00:00:00', CAST(py.Time AS TIME)),
                     CAST(py.Date AS DATETIME)) ASC
    ) pay

    -- Only return visits that touch the requested date range.
    -- Entries pulled from the 48h pre-window that never exit in range are dropped.
    WHERE e.entry_dt < @exit_ceil
      AND COALESCE(x.exit_dt, e.entry_dt) >= @start_date
)

-- ----------------------------------------------------------
-- FINAL OUTPUT
-- ----------------------------------------------------------
SELECT
    'PERMIT'                                        AS visit_type,
    v.iParkingId,
    CAST(v.presence_id AS VARCHAR(20))              AS visit_key,
    v.ticket_number,
    v.card_number,
    v.entry_dt,
    -- TODO: verify column name on OPMS.dbo.Location
    el.sAddress                                     AS entry_location,
    v.exit_dt,
    xl.sAddress                                     AS exit_location,
    DATEDIFF(minute, v.entry_dt, v.exit_dt)         AS duration_minutes,
    v.calculated_cost,
    v.amount_paid,
    CASE v.eState
        WHEN 1 THEN 'Currently Parked'
        WHEN 3 THEN 'Paired'
        WHEN 4 THEN 'Lost Exit'
    END                                             AS pair_status
FROM permit_visits v
LEFT JOIN OPMS.dbo.Location el ON el.Id_Location = v.entry_location_id
LEFT JOIN OPMS.dbo.Location xl ON xl.Id_Location = v.exit_location_id

UNION ALL

SELECT
    'KP'                                            AS visit_type,
    v.Id_Parking,
    CAST(v.TicketNumber AS VARCHAR(20))             AS visit_key,
    v.TicketNumber,
    NULL                                            AS card_number,
    v.entry_dt,
    el.sAddress                                     AS entry_location,
    v.exit_dt,
    xl.sAddress                                     AS exit_location,
    DATEDIFF(minute, v.entry_dt, v.exit_dt)         AS duration_minutes,
    v.calculated_cost,
    v.amount_paid,
    CASE v.eState
        WHEN 3 THEN 'Paired'
        WHEN 4 THEN 'No Exit Found'
    END                                             AS pair_status
FROM kp_visits v
LEFT JOIN OPMS.dbo.Location el ON el.Id_Location = v.entry_location_id
LEFT JOIN OPMS.dbo.Location xl ON xl.Id_Location = v.exit_location_id

ORDER BY entry_dt;
