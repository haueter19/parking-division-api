"""
Cash Variance Endpoints
CRUD operations for cashier bag entries - accessible to all authenticated users
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from datetime import datetime

from app.db.session import get_db
from app.api.dependencies import get_current_active_user, UserProxy
from app.models.schemas import (
    CashVarianceCreate, CashVarianceUpdate, CashVarianceResponse
)

router = APIRouter(prefix="/cash-variance", tags=["cash-variance"])


@router.get("/metadata")
async def get_cash_variance_metadata(
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(get_current_active_user)
):
    """
    Get metadata for cash variance form dropdowns.
    Returns locations (garages) and devices (Cashier, Exit, Entrance types).
    """
    # Get facilities where facility_type = 'garage'
    facilities_q = text("""
        SELECT
            facility_id,
            facility_name
        FROM app.dim_facility
        WHERE facility_type = 'garage'
        ORDER BY facility_name
    """)
    facilities = [dict(r._mapping) for r in db.execute(facilities_q).fetchall()]

    # Get devices where device_type IN ('Cashier', 'Exit', 'Entrance')
    devices_q = text("""
        SELECT
            device_id,
            device_terminal_id,
            device_type
        FROM app.dim_device
        WHERE device_type IN ('Cashier', 'Exit', 'Entrance')
        ORDER BY device_type, device_terminal_id
    """)
    devices = [dict(r._mapping) for r in db.execute(devices_q).fetchall()]

    return {
        "facilities": facilities,
        "devices": devices,
        "bag_types": [
            {"value": "regular", "label": "Regular"},
            {"value": "special_event", "label": "Special Event"}
        ]
    }


@router.get("", response_model=List[CashVarianceResponse])
async def list_cash_variance_entries(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    cashier_number: Optional[str] = None,
    facility_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(get_current_active_user)
):
    """List cash variance entries with optional filtering"""

    where_clauses = ["1=1"]
    params = {"skip": skip, "limit": limit}

    if start_date:
        where_clauses.append("cv.date >= :start_date")
        params["start_date"] = start_date

    if end_date:
        where_clauses.append("cv.date <= :end_date")
        params["end_date"] = end_date

    if cashier_number:
        where_clauses.append("cv.cashier_number LIKE :cashier_number")
        params["cashier_number"] = f"%{cashier_number}%"

    if facility_id:
        where_clauses.append("cv.location_id = :facility_id")
        params["facility_id"] = facility_id

    query = text(f"""
        SELECT
            cv.id, cv.date, cv.cashier_number, cv.bag_number, cv.bag_type,
            cv.location_id, cv.device_id, cv.amount, cv.turnarounds,
            cv.ftp_count, cv.coupons, cv.other_non_paying,
            cv.created_by, cv.created_at, cv.updated_by, cv.updated_at,
            f.facility_name as location_name,
            d.device_terminal_id,
            CONCAT(e.first_name, ' ', e.last_name) as created_by_name
        FROM app.cash_variance cv
        LEFT JOIN app.dim_facility f ON cv.location_id = f.facility_id
        LEFT JOIN app.dim_device d ON cv.device_id = d.device_id
        LEFT JOIN pt.employees e ON cv.created_by = e.employee_id
        WHERE {' AND '.join(where_clauses)}
        ORDER BY cv.date DESC, cv.id DESC
        OFFSET :skip ROWS
        FETCH NEXT :limit ROWS ONLY
    """)

    results = db.execute(query, params).fetchall()

    return [
        CashVarianceResponse(
            id=r.id,
            date=r.date,
            cashier_number=r.cashier_number,
            bag_number=r.bag_number,
            bag_type=r.bag_type,
            location_id=r.location_id,
            device_id=r.device_id,
            amount=float(r.amount) if r.amount else None,
            turnarounds=r.turnarounds or 0,
            ftp_count=r.ftp_count or 0,
            coupons=float(r.coupons) if r.coupons else 0,
            other_non_paying=r.other_non_paying or 0,
            created_by=r.created_by,
            created_at=r.created_at,
            updated_by=r.updated_by,
            updated_at=r.updated_at,
            location_name=r.location_name,
            device_terminal_id=r.device_terminal_id,
            created_by_name=r.created_by_name
        )
        for r in results
    ]


@router.get("/{entry_id}", response_model=CashVarianceResponse)
async def get_cash_variance_entry(
    entry_id: int,
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(get_current_active_user)
):
    """Get a specific cash variance entry by ID"""

    query = text("""
        SELECT
            cv.id, cv.date, cv.cashier_number, cv.bag_number, cv.bag_type,
            cv.location_id, cv.device_id, cv.amount, cv.turnarounds,
            cv.ftp_count, cv.coupons, cv.other_non_paying,
            cv.created_by, cv.created_at, cv.updated_by, cv.updated_at,
            f.facility_name as location_name,
            d.device_terminal_id,
            CONCAT(e.first_name, ' ', e.last_name) as created_by_name
        FROM app.cash_variance cv
        LEFT JOIN app.dim_facility f ON cv.location_id = f.facility_id
        LEFT JOIN app.dim_device d ON cv.device_id = d.device_id
        LEFT JOIN pt.employees e ON cv.created_by = e.employee_id
        WHERE cv.id = :entry_id
    """)

    result = db.execute(query, {"entry_id": entry_id}).first()

    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cash variance entry {entry_id} not found"
        )

    return CashVarianceResponse(
        id=result.id,
        date=result.date,
        cashier_number=result.cashier_number,
        bag_number=result.bag_number,
        bag_type=result.bag_type,
        location_id=result.location_id,
        device_id=result.device_id,
        amount=float(result.amount) if result.amount else None,
        turnarounds=result.turnarounds or 0,
        ftp_count=result.ftp_count or 0,
        coupons=float(result.coupons) if result.coupons else 0,
        other_non_paying=result.other_non_paying or 0,
        created_by=result.created_by,
        created_at=result.created_at,
        updated_by=result.updated_by,
        updated_at=result.updated_at,
        location_name=result.location_name,
        device_terminal_id=result.device_terminal_id,
        created_by_name=result.created_by_name
    )


@router.post("", response_model=CashVarianceResponse, status_code=status.HTTP_201_CREATED)
async def create_cash_variance_entry(
    entry_data: CashVarianceCreate,
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(get_current_active_user)
):
    """Create a new cash variance entry (any authenticated user)"""

    insert_sql = text("""
        INSERT INTO app.cash_variance (
            date, cashier_number, bag_number, bag_type,
            location_id, device_id, amount, turnarounds,
            ftp_count, coupons, other_non_paying, created_by
        )
        OUTPUT INSERTED.id
        VALUES (
            :date, :cashier_number, :bag_number, :bag_type,
            :location_id, :device_id, :amount, :turnarounds,
            :ftp_count, :coupons, :other_non_paying, :created_by
        )
    """)

    result = db.execute(insert_sql, {
        "date": entry_data.date,
        "cashier_number": entry_data.cashier_number,
        "bag_number": entry_data.bag_number,
        "bag_type": entry_data.bag_type.value,
        "location_id": entry_data.location_id,
        "device_id": entry_data.device_id,
        "amount": entry_data.amount,
        "turnarounds": entry_data.turnarounds,
        "ftp_count": entry_data.ftp_count,
        "coupons": entry_data.coupons,
        "other_non_paying": entry_data.other_non_paying,
        "created_by": current_user.employee_id
    })

    new_id = result.scalar()
    db.commit()

    # Fetch and return the created entry
    return await get_cash_variance_entry(new_id, db, current_user)


@router.put("/{entry_id}", response_model=CashVarianceResponse)
async def update_cash_variance_entry(
    entry_id: int,
    entry_data: CashVarianceUpdate,
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(get_current_active_user)
):
    """Update an existing cash variance entry (any authenticated user)"""

    # Check if entry exists
    existing = db.execute(
        text("SELECT id FROM app.cash_variance WHERE id = :entry_id"),
        {"entry_id": entry_id}
    ).first()

    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cash variance entry {entry_id} not found"
        )

    # Build dynamic update query
    update_fields = []
    params = {"entry_id": entry_id, "updated_by": current_user.employee_id}

    if entry_data.date is not None:
        update_fields.append("date = :date")
        params["date"] = entry_data.date

    if entry_data.cashier_number is not None:
        update_fields.append("cashier_number = :cashier_number")
        params["cashier_number"] = entry_data.cashier_number

    if entry_data.bag_number is not None:
        update_fields.append("bag_number = :bag_number")
        params["bag_number"] = entry_data.bag_number

    if entry_data.bag_type is not None:
        update_fields.append("bag_type = :bag_type")
        params["bag_type"] = entry_data.bag_type.value

    if entry_data.location_id is not None:
        update_fields.append("location_id = :location_id")
        params["location_id"] = entry_data.location_id

    if entry_data.device_id is not None:
        update_fields.append("device_id = :device_id")
        params["device_id"] = entry_data.device_id

    if entry_data.amount is not None:
        update_fields.append("amount = :amount")
        params["amount"] = entry_data.amount

    if entry_data.turnarounds is not None:
        update_fields.append("turnarounds = :turnarounds")
        params["turnarounds"] = entry_data.turnarounds

    if entry_data.ftp_count is not None:
        update_fields.append("ftp_count = :ftp_count")
        params["ftp_count"] = entry_data.ftp_count

    if entry_data.coupons is not None:
        update_fields.append("coupons = :coupons")
        params["coupons"] = entry_data.coupons

    if entry_data.other_non_paying is not None:
        update_fields.append("other_non_paying = :other_non_paying")
        params["other_non_paying"] = entry_data.other_non_paying

    if not update_fields:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update"
        )

    # Add audit fields
    update_fields.append("updated_at = GETUTCDATE()")
    update_fields.append("updated_by = :updated_by")

    update_sql = text(f"""
        UPDATE app.cash_variance
        SET {', '.join(update_fields)}
        WHERE id = :entry_id
    """)

    db.execute(update_sql, params)
    db.commit()

    # Fetch and return the updated entry
    return await get_cash_variance_entry(entry_id, db, current_user)


@router.delete("/{entry_id}")
async def delete_cash_variance_entry(
    entry_id: int,
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(get_current_active_user)
):
    """Delete a cash variance entry (any authenticated user)"""

    # Check if entry exists
    existing = db.execute(
        text("SELECT id FROM app.cash_variance WHERE id = :entry_id"),
        {"entry_id": entry_id}
    ).first()

    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cash variance entry {entry_id} not found"
        )

    db.execute(
        text("DELETE FROM app.cash_variance WHERE id = :entry_id"),
        {"entry_id": entry_id}
    )
    db.commit()

    return {"success": True, "message": f"Cash variance entry {entry_id} deleted"}
