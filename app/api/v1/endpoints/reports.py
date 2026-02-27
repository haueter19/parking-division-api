from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from datetime import datetime

from app.db.session import get_db
from app.api.dependencies import get_current_active_user, require_role
from app.models.database import User, UserRole

router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("/settle")
async def settlement_report(
    start_date: Optional[str] = '2025-11-05',
    end_date: Optional[str] = '2025-11-05',
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.MANAGER, UserRole.ADMIN]))
):
    """Return aggregated settlement totals grouped by location_type, org_code and payment_type.

    Query parameters:
    - start_date: YYYY-MM-DD (inclusive)
    - end_date: YYYY-MM-DD (inclusive)
    """
    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="start_date and end_date query parameters are required (YYYY-MM-DD)")

    # Parse inputs to ensure valid dates
    try:
        # make inclusive datetimes
        start_dt = datetime.fromisoformat(start_date + "T00:00:00")
        end_dt = datetime.fromisoformat(end_date + "T23:59:59")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")

    # Aggregated groups
    group_sql = text(
        """
        SELECT
            COALESCE(CAST(cc.charge_code AS VARCHAR(20)), '*** GRAND TOTAL ***') AS charge_code,
            COALESCE(f.facility_name, '** Subtotal **') AS facility_name,
            COALESCE(CASE WHEN t.program_id = 1 THEN 'regular' ELSE 'special event' END, '* Subtotal *') AS program_type,
            COALESCE(d.device_terminal_id, 'Subtotal') AS device_terminal_id,
            COUNT(*) AS transaction_count,
            SUM(t.transaction_amount) AS total_transaction_amount,
            SUM(t.settle_amount) AS total_settle_amount,
            MIN(t.transaction_date) AS earliest_transaction_date,
            MAX(t.transaction_date) AS latest_transaction_date,
            GROUPING(cc.charge_code) AS is_charge_code_total,
            GROUPING(f.facility_name) AS is_facility_total,
            GROUPING(CASE WHEN t.program_id = 1 THEN 'regular' ELSE 'special event' END) AS is_program_type_total,
            GROUPING(d.device_terminal_id) AS is_device_total,
            GROUPING_ID(cc.charge_code, f.facility_name, 
                        CASE WHEN t.program_id = 1 THEN 'regular' ELSE 'special event' END,
                        d.device_terminal_id) AS grouping_level
        FROM app.fact_transaction t
        INNER JOIN app.dim_charge_code cc ON (t.charge_code_id = cc.charge_code_id)
        INNER JOIN app.dim_location l ON (t.location_id = l.location_id)
        INNER JOIN app.dim_facility f ON (l.facility_id = f.facility_id)
        INNER JOIN app.dim_settlement_system ss ON (t.settlement_system_id = ss.settlement_system_id)
        INNER JOIN app.dim_payment_method pm ON (t.payment_method_id = pm.payment_method_id)
        INNER JOIN app.dim_device d ON (t.device_id = d.device_id)
        WHERE t.settle_date >= :start_dt
          AND t.settle_date < :end_dt
        GROUP BY 
            ROLLUP(
                cc.charge_code,
                f.facility_name,
                CASE WHEN t.program_id = 1 THEN 'regular' ELSE 'special event' END,
                d.device_terminal_id
            )
        ORDER BY 
            CASE WHEN cc.charge_code IS NULL THEN 0 ELSE 1 END,
            cc.charge_code,
            GROUPING(f.facility_name) DESC,
            f.facility_name,
            GROUPING(CASE WHEN t.program_id = 1 THEN 'regular' ELSE 'special event' END) DESC,
            CASE WHEN t.program_id = 1 THEN 'regular' ELSE 'special event' END,
            GROUPING(d.device_terminal_id) DESC,
            d.device_terminal_id;
        """
    )

    rows = db.execute(group_sql, {"start_dt": start_dt, "end_dt": end_dt}).fetchall()

    groups = []
    for row in rows:
        groups.append({
            "location_type": row.location_type,
            "source": row.source,
            "org_code": row.org_code,
            "location_name": row.location_name,
            #"payment_type": row.payment_type,
            "count": int(row.count),
            "total_settled": float(row.total_settled) if row.total_settled is not None else 0.0
        })

    # totals
    total_sql = text(
        """
        SELECT
            COUNT(*) as total_transactions,
            SUM(COALESCE(settle_amount, transaction_amount)) as total_settled
        FROM app.fact_transaction
        WHERE settle_date IS NOT NULL
          AND settle_date >= :start_dt
          AND settle_date <= :end_dt
        """
    )

    totals_row = db.execute(total_sql, {"start_dt": start_dt, "end_dt": end_dt}).fetchone()

    totals = {
        "total_transactions": int(totals_row.total_transactions or 0),
        "total_settled": float(totals_row.total_settled or 0.0)
    }

    return {"groups": groups, "totals": totals}


@router.get('/verify')
async def settle_by_source(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.MANAGER, UserRole.ADMIN]))
):
    """Return a pivoted table (daily rows) of counts by transaction source.

    Uses SQL Server PIVOT syntax. Filtering is applied using settle_date inclusive.
    """
    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="start_date and end_date are required (YYYY-MM-DD)")

    try:
        start_dt = datetime.fromisoformat(start_date + 'T00:00:00')
        end_dt = datetime.fromisoformat(end_date + 'T23:59:59')
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")

    # User supplied pivot SQL - limit to provided date range in the subquery
    pivot_sql = text(
        """
        SELECT *
        FROM (
            SELECT CONVERT(CHAR(10), settle_date, 120) AS settle_date, staging_table
            FROM app.fact_transaction t
            WHERE settle_date IS NOT NULL
              AND settle_date >= :start_dt
              AND settle_date <= :end_dt
        ) AS SourceTable
        PIVOT (
            COUNT(staging_table)
            FOR staging_table IN ([windcave_staging], [payments_insider_sales_staging], [ips_staging], [zms_cash_regular])
        ) AS PivotTable
        ORDER BY settle_date DESC
        """
    )

    rows = db.execute(pivot_sql, {"start_dt": start_dt, "end_dt": end_dt}).fetchall()

    pivot_cols = ['windcave_staging', 'payments_insider_sales_staging', 'ips_staging', 'zms_cash_regular']

    mapping = {}
    for row in rows:
        try:
            d = dict(row._mapping)
        except Exception:
            d = {col: row[idx] for idx, col in enumerate(row.keys())}

        settle = d.get('settle_date')
        if hasattr(settle, 'strftime'):
            settle = settle.strftime('%Y-%m-%d')
        else:
            settle = str(settle) if settle is not None else None

        out = {'settle_date': settle}
        for c in pivot_cols:
            v = d.get(c)
            try:
                out[c] = int(v) if v is not None else 0
            except Exception:
                out[c] = 0

        mapping[settle] = out

    # Build full date range including missing days - return descending (newest first)
    from datetime import timedelta
    start_only = start_dt.date()
    end_only = end_dt.date()
    result_rows = []
    current = end_only
    while current >= start_only:
        ds = current.strftime('%Y-%m-%d')
        if ds in mapping:
            result_rows.append(mapping[ds])
        else:
            empty = {'settle_date': ds}
            for c in pivot_cols:
                empty[c] = 0
            result_rows.append(empty)
        current = current - timedelta(days=1)

    return {"rows": result_rows}



@router.get('/settle-rollup')
async def settle_rollup_report(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.MANAGER, UserRole.ADMIN]))
):
    """Return hierarchical settlement report using ROLLUP for drill-down display.

    Returns all levels: grand total, charge code subtotals, facility subtotals,
    payment method type subtotals, device type subtotals, device subtotals, and detail rows.

    Query parameters:
    - start_date: YYYY-MM-DD (inclusive)
    - end_date: YYYY-MM-DD (inclusive)
    """
    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="start_date and end_date are required (YYYY-MM-DD)")

    try:
        start_dt = datetime.fromisoformat(start_date + "T00:00:00")
        end_dt = datetime.fromisoformat(end_date + "T23:59:59")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")

    rollup_sql = text("""
        SELECT
            COALESCE(CAST(cc.charge_code AS VARCHAR(10)), '*** GRAND TOTAL ***') AS charge_code,
            COALESCE(f.facility_name, '** Subtotal **') AS facility_name,
            COALESCE(pm.payment_method_type, '* Subtotal *') AS payment_method_type,
            COALESCE(d.device_type, 'Subtotal') AS device_type,
            COALESCE(d.device_terminal_id, 'Subtotal') AS device_terminal_id,
            COUNT(*) AS transaction_count,
            SUM(t.transaction_amount) AS total_transaction_amount,
            SUM(t.settle_amount) AS total_settle_amount,
            MIN(t.transaction_date) AS earliest_transaction_date,
            MAX(t.transaction_date) AS latest_transaction_date,
            GROUPING(cc.charge_code) AS is_charge_code_total,
            GROUPING(f.facility_name) AS is_facility_total,
            GROUPING(pm.payment_method_type) AS is_payment_method_type_total,
            GROUPING(d.device_type) AS is_device_type_total,
            GROUPING(d.device_terminal_id) AS is_device_total,
            GROUPING_ID(cc.charge_code, f.facility_name, 
                        pm.payment_method_type,
                        d.device_type,
                        d.device_terminal_id) AS grouping_level
        FROM app.fact_transaction t
        INNER JOIN app.dim_charge_code cc ON (t.charge_code_id = cc.charge_code_id)
        INNER JOIN app.dim_location l ON (t.location_id = l.location_id)
        INNER JOIN app.dim_facility f ON (l.facility_id = f.facility_id)
        INNER JOIN app.dim_settlement_system ss ON (t.settlement_system_id = ss.settlement_system_id)
        INNER JOIN app.dim_payment_method pm ON (t.payment_method_id = pm.payment_method_id)
        INNER JOIN app.dim_device d ON (t.device_id = d.device_id)
        WHERE t.settle_date >= :start_dt
          AND t.settle_date <= :end_dt
        GROUP BY 
            ROLLUP(
                cc.charge_code,
                f.facility_name,
                pm.payment_method_type,
                d.device_type,
                d.device_terminal_id
            )
        ORDER BY 
            CASE WHEN cc.charge_code IS NULL THEN 0 ELSE 1 END,
            cc.charge_code,
            GROUPING(f.facility_name) DESC,
            f.facility_name,
            GROUPING(pm.payment_method_type) DESC,
            pm.payment_method_type,
            GROUPING(d.device_type) DESC,
            d.device_type,
            GROUPING(d.device_terminal_id) DESC,
            d.device_terminal_id
    """)

    rows = db.execute(rollup_sql, {"start_dt": start_dt, "end_dt": end_dt}).fetchall()

    result_rows = []
    for row in rows:
        result_rows.append({
            "charge_code": row.charge_code,
            "facility_name": row.facility_name,
            "payment_method_type": row.payment_method_type,
            "device_type": row.device_type,
            "device_terminal_id": row.device_terminal_id,
            "transaction_count": int(row.transaction_count),
            "total_transaction_amount": float(row.total_transaction_amount or 0),
            "total_settle_amount": float(row.total_settle_amount or 0),
            "earliest_transaction_date": row.earliest_transaction_date.isoformat() if row.earliest_transaction_date else None,
            "latest_transaction_date": row.latest_transaction_date.isoformat() if row.latest_transaction_date else None,
            "is_charge_code_total": bool(row.is_charge_code_total),
            "is_facility_total": bool(row.is_facility_total),
            "is_payment_method_type_total": bool(row.is_payment_method_type_total),
            "is_device_type_total": bool(row.is_device_type_total),
            "is_device_total": bool(row.is_device_total),
            "grouping_level": int(row.grouping_level)
        })

    return {"rows": result_rows}


@router.get('/revenue/filters')
async def revenue_filter_options(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.MANAGER, UserRole.ADMIN]))
):
    """Return distinct values for all filter dropdowns."""

    filters = {}

    # Settlement systems
    result = db.execute(text("SELECT DISTINCT system_name FROM app.dim_settlement_system ORDER BY system_name"))
    filters['settlement_systems'] = [row[0] for row in result.fetchall() if row[0]]

    # Payment methods
    result = db.execute(text("SELECT DISTINCT payment_method_type FROM app.dim_payment_method ORDER BY payment_method_type"))
    filters['payment_methods'] = [row[0] for row in result.fetchall() if row[0]]

    # Charge codes
    result = db.execute(text("SELECT DISTINCT charge_code FROM app.dim_charge_code ORDER BY charge_code"))
    filters['charge_codes'] = [str(row[0]) for row in result.fetchall() if row[0]]

    # Device types
    result = db.execute(text("SELECT DISTINCT device_type FROM app.dim_device WHERE device_type IS NOT NULL ORDER BY device_type"))
    filters['device_types'] = [row[0] for row in result.fetchall() if row[0]]

    # Facility types
    result = db.execute(text("SELECT DISTINCT facility_type FROM app.dim_facility WHERE facility_type IS NOT NULL ORDER BY facility_type"))
    filters['facility_types'] = [row[0] for row in result.fetchall() if row[0]]

    # Facility names
    result = db.execute(text("SELECT DISTINCT facility_name FROM app.dim_facility ORDER BY facility_name"))
    filters['facility_names'] = [row[0] for row in result.fetchall() if row[0]]

    return filters


@router.get('/revenue')
async def revenue_report(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: Optional[str] = 'month',
    settlement_system: Optional[str] = None,
    payment_method: Optional[str] = None,
    charge_code: Optional[str] = None,
    device_type: Optional[str] = None,
    facility_type: Optional[str] = None,
    facility_name: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.MANAGER, UserRole.ADMIN]))
):
    """Return revenue data grouped by period with optional filters.

    Query parameters:
    - start_date: YYYY-MM-DD (inclusive)
    - end_date: YYYY-MM-DD (inclusive)
    - period: 'day', 'week', 'month', 'quarter', or 'year' (default: 'month')
    - settlement_system: Filter by settlement system
    - payment_method: Filter by payment method type
    - charge_code: Filter by charge code
    - device_type: Filter by device type
    - facility_type: Filter by facility type
    - facility_name: Filter by facility name
    """
    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="start_date and end_date are required (YYYY-MM-DD)")

    try:
        start_dt = datetime.fromisoformat(start_date + "T00:00:00")
        end_dt = datetime.fromisoformat(end_date + "T23:59:59")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")

    # Build period grouping expression based on SQL Server syntax
    period_expressions = {
        'day': "CONVERT(VARCHAR(10), t.settle_date, 120)",
        'week': "CONVERT(VARCHAR(10), DATEADD(DAY, 1 - DATEPART(WEEKDAY, t.settle_date), t.settle_date), 120)",
        'month': "FORMAT(t.settle_date, 'yyyy-MM')",
        'quarter': "CONCAT(YEAR(t.settle_date), '-Q', DATEPART(QUARTER, t.settle_date))",
        'year': "CAST(YEAR(t.settle_date) AS VARCHAR(4))"
    }

    period_labels = {
        'day': "CONVERT(VARCHAR(10), t.settle_date, 120)",
        'week': "CONCAT('Week of ', CONVERT(VARCHAR(10), DATEADD(DAY, 1 - DATEPART(WEEKDAY, t.settle_date), t.settle_date), 120))",
        'month': "FORMAT(t.settle_date, 'MMM yyyy')",
        'quarter': "CONCAT('Q', DATEPART(QUARTER, t.settle_date), ' ', YEAR(t.settle_date))",
        'year': "CAST(YEAR(t.settle_date) AS VARCHAR(4))"
    }

    if period not in period_expressions:
        raise HTTPException(status_code=400, detail=f"Invalid period. Must be one of: day, week, month, quarter, year")

    period_expr = period_expressions[period]
    period_label_expr = period_labels[period]

    # Build WHERE clause with optional filters
    where_conditions = ["t.settle_date >= :start_dt", "t.settle_date <= :end_dt"]
    params = {"start_dt": start_dt, "end_dt": end_dt}

    if settlement_system:
        where_conditions.append("ss.system_name = :system_name")
        params["system_name"] = settlement_system

    if payment_method:
        where_conditions.append("pm.payment_method_type = :payment_method")
        params["payment_method"] = payment_method

    if charge_code:
        where_conditions.append("CAST(cc.charge_code AS VARCHAR(20)) = :charge_code")
        params["charge_code"] = charge_code

    if device_type:
        where_conditions.append("d.device_type = :device_type")
        params["device_type"] = device_type

    if facility_type:
        where_conditions.append("f.facility_type = :facility_type")
        params["facility_type"] = facility_type

    if facility_name:
        where_conditions.append("f.facility_name = :facility_name")
        params["facility_name"] = facility_name

    where_clause = " AND ".join(where_conditions)

    query = text(f"""
        SELECT
            {period_expr} AS period_key,
            {period_label_expr} AS period_label,
            COUNT(*) AS transaction_count,
            SUM(t.settle_amount) AS amount
        FROM app.fact_transaction t
        INNER JOIN app.dim_charge_code cc ON t.charge_code_id = cc.charge_code_id
        INNER JOIN app.dim_location l ON t.location_id = l.location_id
        INNER JOIN app.dim_facility f ON l.facility_id = f.facility_id
        INNER JOIN app.dim_settlement_system ss ON t.settlement_system_id = ss.settlement_system_id
        INNER JOIN app.dim_payment_method pm ON t.payment_method_id = pm.payment_method_id
        INNER JOIN app.dim_device d ON t.device_id = d.device_id
        WHERE {where_clause}
        GROUP BY {period_expr}, {period_label_expr}
        ORDER BY {period_expr}
    """)

    rows = db.execute(query, params).fetchall()

    results = []
    total_revenue = 0.0
    total_transactions = 0

    for row in rows:
        amount = float(row.amount or 0)
        count = int(row.transaction_count)
        total_revenue += amount
        total_transactions += count
        results.append({
            "period_key": row.period_key,
            "period_label": row.period_label,
            "transaction_count": count,
            "amount": amount
        })

    summary = {
        "total_revenue": total_revenue,
        "transaction_count": total_transactions,
        "avg_per_transaction": total_revenue / total_transactions if total_transactions > 0 else 0,
        "period_count": len(results)
    }

    return {"data": results, "summary": summary}


@router.get('/revenue-landing')
async def revenue_landing_data(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.REVENUE, UserRole.MANAGER, UserRole.ADMIN]))
):
    """Return all data needed for the Revenue section landing page.

    Returns:
    - recent_uploads: last 10 uploaded files with status
    - source_pivot: 7-day settled-by-source pivot table
    - facility_totals: total settled amount by facility for the last 30 days
    - summary: overall totals for the last 30 days
    """
    from datetime import timedelta

    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    seven_days_ago = today - timedelta(days=7)
    thirty_days_ago = today - timedelta(days=30)

    # ── 1. Last 10 uploaded files ─────────────────────────────
    uploads_sql = text("""
        SELECT TOP 10
            uf.id,
            uf.original_filename,
            uf.data_source_type,
            uf.upload_date,
            uf.is_processed,
            uf.records_processed,
            e.first_name + ' ' + e.last_name AS uploaded_by_name
        FROM app.uploaded_files uf
        LEFT JOIN pt.employees e ON uf.uploaded_by = e.employee_id
        ORDER BY uf.upload_date DESC
    """)
    upload_rows = db.execute(uploads_sql).fetchall()
    recent_uploads = []
    for r in upload_rows:
        recent_uploads.append({
            "id": r.id,
            "filename": r.original_filename,
            "source_type": r.data_source_type,
            "upload_date": r.upload_date.isoformat() if r.upload_date else None,
            "is_processed": bool(r.is_processed),
            "records_processed": r.records_processed,
            "uploaded_by": r.uploaded_by_name
        })

    # ── 2. 7-day source pivot ─────────────────────────────────
    pivot_sql = text("""
        SELECT *
        FROM (
            SELECT CONVERT(CHAR(10), settle_date, 120) AS settle_date, staging_table
            FROM app.fact_transaction t
            WHERE settle_date IS NOT NULL
              AND settle_date >= :seven_days_ago
              AND settle_date <= :yesterday
        ) AS SourceTable
        PIVOT (
            COUNT(staging_table)
            FOR staging_table IN (
                [windcave_staging],
                [payments_insider_sales_staging],
                [ips_staging],
                [zms_cash_regular]
            )
        ) AS PivotTable
        ORDER BY settle_date DESC
    """)
    pivot_cols = ['windcave_staging', 'payments_insider_sales_staging', 'ips_staging', 'zms_cash_regular']
    pivot_rows_raw = db.execute(pivot_sql, {
        "seven_days_ago": seven_days_ago.strftime('%Y-%m-%d') + 'T00:00:00',
        "yesterday": yesterday.strftime('%Y-%m-%d') + 'T23:59:59'
    }).fetchall()

    pivot_map = {}
    for row in pivot_rows_raw:
        d = dict(row._mapping)
        settle = d.get('settle_date')
        if hasattr(settle, 'strftime'):
            settle = settle.strftime('%Y-%m-%d')
        else:
            settle = str(settle) if settle is not None else None
        out = {'settle_date': settle}
        for c in pivot_cols:
            v = d.get(c)
            try:
                out[c] = int(v) if v is not None else 0
            except Exception:
                out[c] = 0
        pivot_map[settle] = out

    source_pivot = []
    current = yesterday
    for _ in range(7):
        ds = current.strftime('%Y-%m-%d')
        source_pivot.append(pivot_map.get(ds, {
            'settle_date': ds,
            'windcave_staging': 0,
            'payments_insider_sales_staging': 0,
            'ips_staging': 0,
            'zms_cash_regular': 0
        }))
        current -= timedelta(days=1)

    # ── 3. Facility totals – last 30 days ─────────────────────
    facility_sql = text("""
        SELECT
            f.facility_name,
            f.facility_type,
            COUNT(*) AS transaction_count,
            SUM(t.settle_amount) AS total_settled
        FROM app.fact_transaction t
        INNER JOIN app.dim_location l ON t.location_id = l.location_id
        INNER JOIN app.dim_facility f ON l.facility_id = f.facility_id
        WHERE t.settle_date >= :thirty_days_ago
          AND t.settle_date <= :yesterday
        GROUP BY f.facility_name, f.facility_type
        ORDER BY total_settled DESC
    """)
    facility_rows = db.execute(facility_sql, {
        "thirty_days_ago": thirty_days_ago.strftime('%Y-%m-%d') + 'T00:00:00',
        "yesterday": yesterday.strftime('%Y-%m-%d') + 'T23:59:59'
    }).fetchall()
    facility_totals = [{
        "facility_name": r.facility_name,
        "facility_type": r.facility_type,
        "transaction_count": int(r.transaction_count),
        "total_settled": float(r.total_settled or 0)
    } for r in facility_rows]

    # ── 4. 30-day summary ─────────────────────────────────────
    summary_sql = text("""
        SELECT
            COUNT(*) AS total_transactions,
            SUM(t.settle_amount) AS total_settled,
            MAX(t.settle_date) AS last_settle_date
        FROM app.fact_transaction t
        WHERE t.settle_date >= :thirty_days_ago
          AND t.settle_date <= :yesterday
    """)
    summary_row = db.execute(summary_sql, {
        "thirty_days_ago": thirty_days_ago.strftime('%Y-%m-%d') + 'T00:00:00',
        "yesterday": yesterday.strftime('%Y-%m-%d') + 'T23:59:59'
    }).fetchone()

    summary = {
        "total_transactions": int(summary_row.total_transactions or 0),
        "total_settled": float(summary_row.total_settled or 0),
        "last_settle_date": summary_row.last_settle_date.strftime('%Y-%m-%d') if summary_row.last_settle_date else None,
        "period_days": 30
    }

    return {
        "recent_uploads": recent_uploads,
        "source_pivot": source_pivot,
        "facility_totals": facility_totals,
        "summary": summary
    }
