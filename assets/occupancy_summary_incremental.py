"""
occupancy_summary_incremental.py

Replaces the delete-all / reinsert-all approach in occupancy_summary.py with
three targeted phases per garage per run:

  Phase A — Remove overshoot
      Visits that were open at last_min (ExitDate was NULL) and have since
      closed with ExitDate <= last_min. Subtract 1 from existing summary rows
      in [ceil(real ExitDate), last_min] for the appropriate customer type.

  Phase B — Add missing historical rows  (rare)
      Brand-new visits (LoadDate > last_min) whose EntryDate <= last_min,
      meaning a prior ETL run missed them. Add +1 to existing summary rows in
      [floor(EntryDate), last_min]. Their exit event (if any) is handled by
      Phase C.

  Phase C — Insert new minutes
      Compute and INSERT rows for [last_min + 1 min, current_run_time].
      Start from the known occupancy at last_min (read after A and B commit),
      then apply only the delta events — new entries and exits that fall in
      the new window — via cumulative sum. Visits already open at last_min
      carry forward implicitly through the base count.

Use full_rebuild() for initial population or after data corruption. It reuses
the sweep-line logic from the original script and replaces only the affected
date range for the target garage.
"""

from datetime import datetime
import pandas as pd
from sqlalchemy import text

from db_manager import ConnectionManager

cnxn = ConnectionManager()

CUSTOMER_TYPES = ['transient', 'permit', 'employee']

INSERT_COLS = [
    'GarageID', 'date',
    'transient', 'permit', 'employee', 'total',
    'year', 'quarter', 'month', 'week', 'day', 'dayofyear', 'dayofweek',
    'hour', 'hms', 'weekday_type', 'period',
]


# ── Shared helpers ────────────────────────────────────────────────────────────

def add_date_columns(df):
    """Append calendar/period columns to a summary DataFrame (datetime index)."""
    df['year']        = df.index.year
    df['quarter']     = df.index.quarter
    df['month']       = df.index.month
    df['week']        = df.index.isocalendar().week.astype(int)
    df['day']         = df.index.day
    df['dayofyear']   = df.index.dayofyear
    df['dayofweek']   = df.index.dayofweek
    df['hour']        = df.index.hour
    df['hms']         = df.index.strftime('%H:%M:%S')
    df['weekday_type'] = (df['dayofweek'] < 5).astype(int)
    df['period']      = 'Evening'
    df.loc[df['hour'].between(6, 18), 'period'] = 'Daytime'
    df.loc[df['dayofweek'] > 4,       'period'] = 'Weekend'
    return df


def get_last_min(garage_id, engine):
    """Return MAX(date) from dw.VisitSummary for this garage, or None."""
    val = pd.read_sql(
        text("SELECT MAX(date) FROM dw.VisitSummary WHERE GarageID = :gid"),
        engine, params={'gid': garage_id}
    ).iloc[0, 0]
    return pd.Timestamp(val) if val is not None else None


def get_changed_visits(garage_id, last_min, engine):
    """All VisitDetails rows with LoadDate > last_min for this garage."""
    df = pd.read_sql(f"""
        SELECT TripDetailID, GarageID, Garage, EntryDate, ExitDate,
               customer_type, status, LoadDate
        FROM dw.VisitDetails
        WHERE GarageID = {garage_id}
          AND LoadDate > '{last_min}'
    """, engine)
    if not df.empty:
        df['EntryDate'] = pd.to_datetime(df['EntryDate'])
        df['ExitDate']  = pd.to_datetime(df['ExitDate'])
    return df


# ── Phase A ───────────────────────────────────────────────────────────────────

def phase_a_remove_overshoot(garage_id, newly_closed, last_min, engine):
    """
    For each newly-closed visit, subtract 1 from VisitSummary rows in the
    range [ceil(ExitDate), last_min]. These minutes were previously counted
    because ExitDate was NULL; the real exit makes them incorrect.
    """
    if newly_closed.empty:
        return

    with engine.begin() as conn:
        for _, v in newly_closed.iterrows():
            ctype = v['customer_type']
            if ctype not in CUSTOMER_TYPES:
                continue
            exit_ceil = v['ExitDate'].ceil('min')
            if exit_ceil > last_min:
                # ExitDate rounded up past last_min — no overshoot exists
                continue
            r = conn.execute(text(f"""
                UPDATE dw.VisitSummary
                SET [{ctype}] = [{ctype}] - 1, total = total - 1
                WHERE GarageID = :gid
                  AND date >= :exit_ceil
                  AND date <= :last_min
            """), {'gid': garage_id, 'exit_ceil': exit_ceil, 'last_min': last_min})
            print(f"    Phase A: {v['TripDetailID']} ({ctype}) — removed {r.rowcount} rows")


# ── Phase B ───────────────────────────────────────────────────────────────────

def phase_b_add_historical_new_visits(garage_id, new_historical, last_min, engine):
    """
    For each brand-new visit with EntryDate <= last_min, add +1 to existing
    summary rows in [floor(EntryDate), last_min]. Their exit event (if any)
    falls in the new window and is handled by Phase C.

    This phase is rare in normal operation — it fires only when the VisitDetails
    ETL discovers a visit that earlier pipeline runs missed.
    """
    if new_historical.empty:
        return

    with engine.begin() as conn:
        for _, v in new_historical.iterrows():
            ctype = v['customer_type']
            if ctype not in CUSTOMER_TYPES:
                continue
            entry_floor = v['EntryDate'].floor('min')
            r = conn.execute(text(f"""
                UPDATE dw.VisitSummary
                SET [{ctype}] = [{ctype}] + 1, total = total + 1
                WHERE GarageID = :gid
                  AND date >= :entry
                  AND date <= :last_min
            """), {'gid': garage_id, 'entry': entry_floor, 'last_min': last_min})
            print(f"    Phase B: {v['TripDetailID']} ({ctype}) — updated {r.rowcount} historical rows")


# ── Phase C ───────────────────────────────────────────────────────────────────

def phase_c_insert_new_minutes(garage_id, changed, last_min, current_run_time, engine):
    """
    Compute and INSERT summary rows for [last_min + 1 min, current_run_time].

    The occupancy at last_min+1 is the occupancy at last_min (base) adjusted
    for any new entries/exits that occur in the window. Visits that were already
    open at last_min carry forward through the base count without generating
    entry events — only new arrivals and departures create delta events.

    Base counts are read AFTER Phases A and B have committed so that corrections
    to existing rows are already reflected.
    """
    new_minutes = pd.date_range(
        start=last_min + pd.Timedelta('1min'),
        end=current_run_time,
        freq='min'
    )
    if len(new_minutes) == 0:
        return

    # Read base counts from last_min after A/B have committed
    base = pd.read_sql(
        text("""
            SELECT transient, permit, employee
            FROM dw.VisitSummary
            WHERE GarageID = :gid AND date = :last_min
        """),
        engine, params={'gid': garage_id, 'last_min': last_min}
    ).iloc[0]

    new_summary = pd.DataFrame(
        {ct: float(base[ct]) for ct in CUSTOMER_TYPES},
        index=new_minutes
    )

    if not changed.empty:
        for ctype in CUSTOMER_TYPES:
            ct_df = changed[changed['customer_type'] == ctype]

            # Entry events: visits that entered AFTER last_min (new to this window).
            # Visits open at last_min don't generate an entry event here — they're
            # already reflected in the base count.
            entries = ct_df.loc[
                ct_df['EntryDate'] > last_min, 'EntryDate'
            ].dt.floor('min')

            # Exit events: visits whose exit falls within the new window.
            # Covers both (a) previously open visits that just closed and
            # (b) brand-new complete visits that entered and exited this run.
            exits = ct_df.loc[
                ct_df['ExitDate'].notna() &
                (ct_df['ExitDate'] > last_min) &
                (ct_df['ExitDate'] <= current_run_time),
                'ExitDate'
            ].dt.ceil('min')

            delta = pd.Series(0.0, index=new_minutes)
            if not entries.empty:
                delta = delta.add(
                    entries.value_counts().reindex(new_minutes, fill_value=0)
                )
            if not exits.empty:
                delta = delta.subtract(
                    exits.value_counts().reindex(new_minutes, fill_value=0)
                )

            new_summary[ctype] = (new_summary[ctype] + delta.cumsum()).clip(lower=0)

    new_summary[CUSTOMER_TYPES] = new_summary[CUSTOMER_TYPES].round().astype(int)
    new_summary['total']    = new_summary[CUSTOMER_TYPES].sum(axis=1)
    new_summary['GarageID'] = garage_id
    add_date_columns(new_summary)

    rows = new_summary.reset_index().rename(columns={'index': 'date'})[INSERT_COLS]
    rows.to_sql(
        'VisitSummary', schema='dw', con=engine,
        if_exists='append', index=False, chunksize=1000
    )
    print(f"    Phase C: inserted {len(rows)} new minute rows")


# ── Incremental update ────────────────────────────────────────────────────────

def incremental_update(garage_id, garage, engine):
    """
    Incrementally update dw.VisitSummary for one garage from the last
    processed minute up to now. Returns True on success, False if a
    full_rebuild() is needed first.
    """
    current_run_time = pd.Timestamp(datetime.now()).floor('min')

    last_min = get_last_min(garage_id, engine)
    if last_min is None:
        print(f"[{garage}] No existing summary — run full_rebuild() first.")
        return False

    if current_run_time <= last_min:
        print(f"[{garage}] Already up to date (last_min={last_min}).")
        return True

    elapsed = int((current_run_time - last_min).total_seconds() / 60)
    print(f"[{garage}] Updating {last_min} → {current_run_time} ({elapsed} min elapsed)")

    changed = get_changed_visits(garage_id, last_min, engine)
    print(f"  {len(changed)} changed visit(s) since last run")

    if not changed.empty:
        # Visits that closed before last_min — their rows in VisitSummary
        # extend too far forward and need the overshoot trimmed.
        newly_closed = changed[
            changed['ExitDate'].notna() & (changed['ExitDate'] <= last_min)
        ]

        # Brand-new visits with EntryDate before last_min — not yet in the
        # summary, need to be added to existing rows.
        in_window_or_open = changed[
            changed['ExitDate'].isna() | (changed['ExitDate'] > last_min)
        ]
        new_historical = in_window_or_open[in_window_or_open['EntryDate'] <= last_min]

        if not newly_closed.empty:
            print(f"  {len(newly_closed)} newly closed visit(s) — removing overshoot")
        if not new_historical.empty:
            print(f"  {len(new_historical)} historical new visit(s) — backfilling")

        phase_a_remove_overshoot(garage_id, newly_closed, last_min, engine)
        phase_b_add_historical_new_visits(garage_id, new_historical, last_min, engine)

    phase_c_insert_new_minutes(garage_id, changed, last_min, current_run_time, engine)
    print(f"[{garage}] Done.\n")
    return True


# ── Full rebuild ──────────────────────────────────────────────────────────────

def _sweep_line(df, start, end, garage):
    """
    Sweep-line occupancy by customer type over [start, end].
    Identical logic to the original count_occupancy() with by_customer_type=True.
    """
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    all_minutes = pd.date_range(start=start, end=end, freq='min')

    df = df[df['Garage'] == garage].copy()
    df['EntryDate'] = df['EntryDate'].clip(lower=start, upper=end)
    df['ExitDate']  = df['ExitDate'].clip(lower=start, upper=end)
    df = df[df['EntryDate'] < df['ExitDate']].dropna(subset=['customer_type'])

    result = pd.DataFrame(0, index=all_minutes, columns=CUSTOMER_TYPES)
    for ctype in CUSTOMER_TYPES:
        ct = df[df['customer_type'] == ctype]
        counts = pd.Series(0, index=all_minutes)
        counts = counts.add(ct['EntryDate'].dt.floor('min').value_counts(), fill_value=0)
        counts = counts.subtract(ct['ExitDate'].dt.ceil('min').value_counts(), fill_value=0)
        result[ctype] = counts.cumsum().astype(int)
    return result


def full_rebuild(garage_id, garage, engine, from_date=None):
    """
    Full sweep-line rebuild of dw.VisitSummary for one garage.

    Deletes existing rows from `from_date` (or the earliest affected date
    derived from recently loaded visits) forward, then reinserts the full
    recomputed range.

    Use this for initial population or recovery after data corruption.
    Pass from_date='YYYY-MM-DD' to rebuild from a specific date rather than
    deriving it automatically.
    """
    now = pd.Timestamp(datetime.now()).floor('min')

    last_summary_date = get_last_min(garage_id, engine)
    since_clause = (
        f"AND LoadDate >= '{last_summary_date}'"
        if last_summary_date is not None
        else ""
    )

    date_range = pd.read_sql(f"""
        SELECT LoadDate,
               MIN(EntryDate) min_entry, MIN(ExitDate) min_exit,
               MAX(EntryDate) max_entry, MAX(ExitDate) max_exit
        FROM dw.VisitDetails
        WHERE GarageID = {garage_id}
        {since_clause}
        GROUP BY LoadDate
    """, engine)

    if date_range.empty:
        print(f"[{garage}] No visits found — nothing to rebuild.")
        return

    date_from = date_range.min().min().floor('min')
    date_thru = date_range.max().max().ceil('min')

    if from_date is not None:
        date_from = pd.Timestamp(from_date)

    print(f"[{garage}] Full rebuild: {date_from} → {date_thru}")

    inner_data = pd.read_sql(f"""
        SELECT TripDetailID, GarageID, Garage, EntryDate, ExitDate,
               customer_type, status, length_of_stay, LoadDate
        FROM dw.VisitDetails
        WHERE GarageID = {garage_id}
          AND EntryDate < '{date_thru}'
          AND (ExitDate >= '{date_from}' OR ExitDate IS NULL)
        ORDER BY EntryDate, ExitDate DESC
    """, engine)

    outer_start = inner_data['EntryDate'].min().floor('D')
    outer_data = pd.read_sql(f"""
        SELECT GarageID, Garage, EntryDate, ExitDate, customer_type, status, length_of_stay
        FROM dw.VisitDetails
        WHERE GarageID = {garage_id}
          AND EntryDate < '{date_thru}'
          AND (ExitDate >= '{outer_start}' OR ExitDate IS NULL)
        ORDER BY EntryDate, ExitDate DESC
    """, engine)

    imputed_exit = date_thru.floor('min') + pd.Timedelta('1min')
    inner_data['ExitDate'] = inner_data['ExitDate'].fillna(imputed_exit)
    outer_data['ExitDate'] = outer_data['ExitDate'].fillna(imputed_exit)

    assert inner_data.shape[0] == inner_data['TripDetailID'].nunique(), \
        "Duplicate TripDetailID values found"
    assert inner_data[inner_data['length_of_stay'] < 0].shape[0] == 0, \
        "Negative length_of_stay values found"

    f = min(inner_data['EntryDate'].min(), inner_data['ExitDate'].min()).floor('min')
    t = inner_data['ExitDate'].max().floor('min') - pd.Timedelta('1min')

    summary = _sweep_line(outer_data, f, t, garage)
    for ct in CUSTOMER_TYPES:
        if ct not in summary.columns:
            summary[ct] = 0

    summary['total']    = summary[CUSTOMER_TYPES].sum(axis=1)
    summary['GarageID'] = garage_id
    add_date_columns(summary)
    summary = summary.iloc[:-1]  # drop last (still-open) minute

    update_from = from_date if from_date is not None else f

    with engine.begin() as conn:
        r = conn.execute(
            text("DELETE FROM dw.VisitSummary WHERE GarageID = :gid AND date >= :from_date"),
            {'gid': garage_id, 'from_date': update_from}
        )
        print(f"  Deleted {r.rowcount} existing rows")

    rows = summary.reset_index().rename(columns={'index': 'date'})[INSERT_COLS]
    rows.to_sql(
        'VisitSummary', schema='dw', con=engine,
        if_exists='append', index=False, chunksize=1000
    )
    print(f"  Inserted {len(rows)} rows")
    print(f"[{garage}] Full rebuild done.\n")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    reporting_engine = cnxn.get_engine('PUReporting')

    garages_df = pd.read_sql(
        "SELECT Id_Parking, ParkingName garage FROM ParkingAdmin WHERE Id_Parking NOT IN (7, 9)",
        cnxn.get_engine('opms')
    )
    garages = dict(zip(garages_df['Id_Parking'], garages_df['garage']))

    for garage_id, garage in garages.items():
        try:
            success = incremental_update(garage_id, garage, reporting_engine)
            if not success:
                # Uncomment to auto-rebuild garages with no existing summary:
                # full_rebuild(garage_id, garage, reporting_engine)
                pass
        except Exception as e:
            print(f"[{garage}] ERROR: {e}")
            raise
