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
            location_type,
            max(source) source, 
            org_code,
            max(location_name) AS location_name,
            --payment_type,
            COUNT(*) AS count,
            SUM(COALESCE(settle_amount, transaction_amount)) AS total_settled
        FROM app.fact_transaction
        WHERE settle_date IS NOT NULL
            AND settle_date >= :start_dt
            AND settle_date <= :end_dt
        GROUP BY location_type, source, org_code--, payment_type
        ORDER BY location_type, source, org_code--, payment_type
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
            FOR staging_table IN ([windcave_staging], [payments_insider_sales_staging], [ips_cc_staging], [ips_mobile_staging], [ips_cash_staging], [zms_cash_regular])
        ) AS PivotTable
        ORDER BY settle_date DESC
        """
    )

    rows = db.execute(pivot_sql, {"start_dt": start_dt, "end_dt": end_dt}).fetchall()

    pivot_cols = ['windcave_staging', 'payments_insider_sales_staging', 'ips_cc_staging', 'ips_mobile_staging', 'ips_cash_staging', 'zms_cash_regular']

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
