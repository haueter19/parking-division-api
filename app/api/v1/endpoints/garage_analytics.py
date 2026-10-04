"""
Garage Performance Analytics Endpoints
Occupancy and revenue data for the Analytics > Garage Performance dashboard
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from datetime import date as date_type, datetime, timedelta
from collections import defaultdict

from app.db.session import get_db
from app.api.dependencies import get_current_active_user, UserProxy

router = APIRouter(prefix="/garage-analytics", tags=["garage-analytics"])

# Garage capacity isn't stored in dim_facility - fixed per Dan, keyed by facility_id/GarageID
GARAGE_CAPACITY = {1: 609, 2: 776, 5: 939, 6: 605, 18: 640, 19: 555}


@router.get("/garages")
async def get_garages(
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(get_current_active_user)
):
    """List garages available for the performance dashboard"""

    query = text("""
        SELECT facility_id, facility_name
        FROM app.dim_facility
        WHERE facility_type = 'garage'
        ORDER BY facility_name
    """)
    rows = db.execute(query).fetchall()

    return [
        {
            "garage_id": r.facility_id,
            "garage_name": r.facility_name,
            "capacity": GARAGE_CAPACITY.get(r.facility_id)
        }
        for r in rows
    ]


@router.get("/occupancy")
async def get_garage_occupancy(
    garage_id: int = Query(...),
    target_date: Optional[date_type] = Query(None, alias="date"),
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(get_current_active_user)
):
    """
    Minute-level occupancy for one garage: yesterday's trace, the
    same-weekday avg/max over the trailing year, days-at-capacity
    over that same window, and trips by customer type for yesterday.
    """

    target = target_date or (date_type.today() - timedelta(days=1))
    start_date = target - timedelta(days=365)
    capacity = GARAGE_CAPACITY.get(garage_id)

    # dayofweek is matched by looking up the value stored for the target
    # date itself, rather than assuming a 0/1-based convention for the
    # column - this way it's correct no matter how dw.VisitSummary encodes it.
    occupancy_query = text("""
        SELECT GarageID, date, transient, permit, employee, total, hms
        FROM dw.VisitSummary
        WHERE GarageID = :garage_id
          AND date BETWEEN :start_date AND :target_date
          AND dayofweek = (
              SELECT TOP 1 dayofweek FROM dw.VisitSummary
              WHERE GarageID = :garage_id AND date = :target_date
          )
        ORDER BY date, hms
    """)
    rows = db.execute(occupancy_query, {
        "garage_id": garage_id,
        "start_date": start_date,
        "target_date": target
    }).fetchall()

    yesterday_by_minute = {}
    history_by_minute = defaultdict(list)
    max_total_by_date = defaultdict(float)

    for r in rows:
        # hms carries seconds (e.g. "13:44:09") but readings aren't
        # guaranteed to land on :00 - bucket to the minute so yesterday's
        # trace lines up with the historical same-weekday minutes.
        minute_key = r.hms[:5]
        total = float(r.total or 0)

        max_total_by_date[r.date] = max(max_total_by_date[r.date], total)

        if r.date == target:
            yesterday_by_minute[minute_key] = total
        else:
            history_by_minute[minute_key].append(total)

    history_by_minute_avg_max = {
        minute: {"avg_total": sum(vals) / len(vals), "max_total": max(vals)}
        for minute, vals in history_by_minute.items()
    }

    days_at_capacity = sum(
        1 for max_total in max_total_by_date.values()
        if capacity and max_total >= capacity
    )

    # Trips by customer type: visits overlapping yesterday's calendar day
    day_start = datetime.combine(target, datetime.min.time())
    day_end = datetime.combine(target, datetime.max.time())

    customer_type_query = text("""
        SELECT customer_type, COUNT(*) AS trip_count
        FROM dw.VisitDetails
        WHERE GarageID = :garage_id
          AND EntryDate <= :day_end
          AND (ExitDate IS NULL OR ExitDate >= :day_start)
        GROUP BY customer_type
    """)
    customer_rows = db.execute(customer_type_query, {
        "garage_id": garage_id,
        "day_start": day_start,
        "day_end": day_end
    }).fetchall()

    return {
        "garage_id": garage_id,
        "capacity": capacity,
        "date": target.isoformat(),
        "yesterday": [
            {"minute": m, "total": t}
            for m, t in sorted(yesterday_by_minute.items())
        ],
        "history": [
            {"minute": m, **vals}
            for m, vals in sorted(history_by_minute_avg_max.items())
        ],
        "days_at_capacity": days_at_capacity,
        "days_in_window": len(max_total_by_date),
        "trips_by_customer_type": [
            {"customer_type": r.customer_type, "trip_count": r.trip_count}
            for r in customer_rows
        ]
    }


@router.get("/revenue")
async def get_garage_revenue(
    garage_id: int = Query(...),
    target_date: Optional[date_type] = Query(None, alias="date"),
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(get_current_active_user)
):
    """
    Revenue for one garage on the target date, broken down by
    payment method, payment method brand, and device type.
    """

    target = target_date or (date_type.today() - timedelta(days=1))
    day_start = datetime.combine(target, datetime.min.time())
    day_end = datetime.combine(target, datetime.max.time())

    # TODO(Dan): pm/ss joins weren't in the SQL you sent - confirm the
    # actual table/column names for payment method and settlement system.
    # Assuming app.dim_payment_method (keyed by t.payment_method_id) and
    # app.dim_system (keyed by t.system_id) below.
    revenue_query = text("""
        SELECT
            t.transaction_id, t.transaction_date, t.transaction_amount,
            t.settle_amount, t.settle_date,
            ss.system_name,
            d.device_terminal_id, d.device_type,
            cc.charge_code,
            f.facility_type, f.facility_name, f.facility_id,
            pm.payment_method_type, pm.payment_method_brand
        FROM app.fact_transaction t
        INNER JOIN app.dim_charge_code cc ON (t.charge_code_id = cc.charge_code_id)
        INNER JOIN app.dim_location l ON (t.location_id = l.location_id)
        INNER JOIN app.dim_facility f ON (l.facility_id = f.facility_id)
        INNER JOIN app.dim_device d ON (t.device_id = d.device_id)
        LEFT JOIN app.dim_payment_method pm ON (t.payment_method_id = pm.payment_method_id)
        LEFT JOIN app.dim_system ss ON (t.system_id = ss.system_id)
        WHERE t.transaction_date BETWEEN :day_start AND :day_end
          AND f.facility_type = 'garage'
          AND f.facility_id = :garage_id
    """)
    rows = db.execute(revenue_query, {
        "day_start": day_start,
        "day_end": day_end,
        "garage_id": garage_id
    }).fetchall()

    by_payment_method = defaultdict(float)
    by_payment_brand = defaultdict(float)
    by_device_type = defaultdict(float)
    total_revenue = 0.0

    for r in rows:
        amount = float(r.transaction_amount or 0)
        total_revenue += amount
        by_payment_method[r.payment_method_type or "Unknown"] += amount
        by_payment_brand[r.payment_method_brand or "Unknown"] += amount
        by_device_type[r.device_type or "Unknown"] += amount

    def to_list(breakdown):
        return [
            {"label": label, "amount": round(amount, 2)}
            for label, amount in sorted(breakdown.items(), key=lambda kv: -kv[1])
        ]

    return {
        "garage_id": garage_id,
        "date": target.isoformat(),
        "total_revenue": round(total_revenue, 2),
        "transaction_count": len(rows),
        "by_payment_method": to_list(by_payment_method),
        "by_payment_brand": to_list(by_payment_brand),
        "by_device_type": to_list(by_device_type)
    }
