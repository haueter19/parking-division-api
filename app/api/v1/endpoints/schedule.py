"""
Schedule management endpoints.

Supervisors define shift slots for a given week. A solver (integrated later)
fills those slots with employees. Supervisors can override assignments.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional

from app.db.session import get_db
from app.api.dependencies import require_role
from app.models.database import UserRole
from app.models.schemas import (
    ShiftCreate, ShiftUpdate, ShiftResponse,
    AssignmentUpdate, AssignmentResponse,
    WeekSummary,
)

router = APIRouter(prefix="/schedule", tags=["schedule"])

SCHEDULE_ROLES = [UserRole.SUPERVISOR, UserRole.MANAGER, UserRole.ADMIN]


def _require_schedule_role(current_user=Depends(require_role(SCHEDULE_ROLES))):
    return current_user


def _derive_period(start_hour: float) -> str:
    """AM if start_hour (mod 24) < 12, else PM."""
    return "AM" if (start_hour % 24) < 12 else "PM"


# ---------------------------------------------------------------------------
# GET /schedule/weeks  — list all weeks that have at least one shift
# ---------------------------------------------------------------------------
@router.get("/weeks", response_model=List[WeekSummary])
async def list_weeks(
    db: Session = Depends(get_db),
    current_user=Depends(require_role(SCHEDULE_ROLES)),
):
    sql = text("""
        SELECT
            CONVERT(VARCHAR(10), s.week_start_date, 120) AS week_start_date,
            COUNT(s.shift_id)                            AS shift_count,
            CAST(
                CASE WHEN EXISTS (
                    SELECT 1 FROM app.schedule_assignments a
                    WHERE a.shift_id IN (
                        SELECT shift_id FROM app.schedule_shifts
                        WHERE week_start_date = s.week_start_date
                    )
                ) THEN 1 ELSE 0 END
            AS BIT)                                      AS is_solved
        FROM app.schedule_shifts s
        GROUP BY s.week_start_date
        ORDER BY s.week_start_date DESC
    """)
    rows = db.execute(sql).fetchall()
    return [
        WeekSummary(
            week_start_date=row.week_start_date,
            shift_count=row.shift_count,
            is_solved=bool(row.is_solved),
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# GET /schedule/shifts?week=YYYY-MM-DD  — shifts (+ assignments) for one week
# ---------------------------------------------------------------------------
@router.get("/shifts", response_model=List[ShiftResponse])
async def get_shifts(
    week: str = Query(..., description="Week start date as YYYY-MM-DD (Sunday)"),
    db: Session = Depends(get_db),
    current_user=Depends(require_role(SCHEDULE_ROLES)),
):
    sql = text("""
        SELECT
            s.shift_id,
            CONVERT(VARCHAR(10), s.week_start_date, 120) AS week_start_date,
            s.location,
            s.booth,
            s.day_of_week,
            s.start_hour,
            s.end_hour,
            s.created_at,
            s.created_by,
            s.updated_at,
            s.updated_by,
            a.assignment_id,
            a.employee_id,
            a.solver_employee_id,
            a.is_manual_override,
            NULLIF(LTRIM(RTRIM(
                ISNULL(e.first_name, '') + ' ' + ISNULL(e.last_name, '')
            )), '') AS employee_name
        FROM app.schedule_shifts s
        LEFT JOIN app.schedule_assignments a ON a.shift_id = s.shift_id
        LEFT JOIN pt.employees e ON e.employee_id = a.employee_id
        WHERE s.week_start_date = :week
        ORDER BY
            CASE s.day_of_week
                WHEN 'Sun' THEN 0 WHEN 'Mon' THEN 1 WHEN 'Tue' THEN 2
                WHEN 'Wed' THEN 3 WHEN 'Thu' THEN 4 WHEN 'Fri' THEN 5
                WHEN 'Sat' THEN 6 ELSE 7 END,
            s.location,
            s.booth,
            s.start_hour
    """)
    rows = db.execute(sql, {"week": week}).fetchall()
    return [
        ShiftResponse(
            shift_id=row.shift_id,
            week_start_date=row.week_start_date,
            location=row.location,
            booth=row.booth,
            day_of_week=row.day_of_week,
            start_hour=row.start_hour,
            end_hour=row.end_hour,
            period=_derive_period(row.start_hour),
            created_at=row.created_at,
            created_by=row.created_by,
            updated_at=row.updated_at,
            updated_by=row.updated_by,
            assignment_id=row.assignment_id,
            employee_id=row.employee_id,
            employee_name=row.employee_name,
            solver_employee_id=row.solver_employee_id,
            is_manual_override=bool(row.is_manual_override) if row.is_manual_override is not None else None,
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# POST /schedule/shifts  — create a shift slot
# ---------------------------------------------------------------------------
@router.post("/shifts", response_model=ShiftResponse, status_code=201)
async def create_shift(
    shift: ShiftCreate,
    db: Session = Depends(get_db),
    current_user=Depends(require_role(SCHEDULE_ROLES)),
):
    sql = text("""
        INSERT INTO app.schedule_shifts
            (week_start_date, location, booth, day_of_week, start_hour, end_hour, created_by)
        OUTPUT INSERTED.shift_id
        VALUES (:week, :location, :booth, :day, :start_hour, :end_hour, :created_by)
    """)
    result = db.execute(sql, {
        "week": shift.week_start_date,
        "location": shift.location,
        "booth": shift.booth,
        "day": shift.day_of_week,
        "start_hour": shift.start_hour,
        "end_hour": shift.end_hour,
        "created_by": current_user.employee_id,
    }).first()
    db.commit()

    if not result:
        raise HTTPException(status_code=500, detail="Failed to create shift")

    new_id = result[0]
    shifts = await get_shifts(week=shift.week_start_date, db=db, current_user=current_user)
    for s in shifts:
        if s.shift_id == new_id:
            return s
    raise HTTPException(status_code=500, detail="Shift created but not found on re-fetch")


# ---------------------------------------------------------------------------
# PUT /schedule/shifts/{shift_id}  — update a shift definition
# ---------------------------------------------------------------------------
@router.put("/shifts/{shift_id}", response_model=ShiftResponse)
async def update_shift(
    shift_id: int,
    shift: ShiftUpdate,
    db: Session = Depends(get_db),
    current_user=Depends(require_role(SCHEDULE_ROLES)),
):
    # Fetch existing to get week (needed for re-fetch after update)
    existing = db.execute(
        text("SELECT week_start_date FROM app.schedule_shifts WHERE shift_id = :id"),
        {"id": shift_id}
    ).first()
    if not existing:
        raise HTTPException(status_code=404, detail="Shift not found")

    week = str(existing.week_start_date)

    # Build dynamic SET clause from provided fields
    updates = {}
    if shift.location is not None:
        updates["location"] = shift.location
    if shift.booth is not None:
        updates["booth"] = shift.booth
    if shift.day_of_week is not None:
        updates["day_of_week"] = shift.day_of_week
    if shift.start_hour is not None:
        updates["start_hour"] = shift.start_hour
    if shift.end_hour is not None:
        updates["end_hour"] = shift.end_hour

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    params = {**updates, "id": shift_id, "updated_by": current_user.employee_id}

    db.execute(
        text(f"UPDATE app.schedule_shifts SET {set_clause}, updated_at = GETDATE(), updated_by = :updated_by WHERE shift_id = :id"),
        params,
    )
    db.commit()

    shifts = await get_shifts(week=week[:10], db=db, current_user=current_user)
    for s in shifts:
        if s.shift_id == shift_id:
            return s
    raise HTTPException(status_code=500, detail="Shift updated but not found on re-fetch")


# ---------------------------------------------------------------------------
# DELETE /schedule/shifts/{shift_id}
# ---------------------------------------------------------------------------
@router.delete("/shifts/{shift_id}", status_code=204)
async def delete_shift(
    shift_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(require_role(SCHEDULE_ROLES)),
):
    existing = db.execute(
        text("SELECT shift_id FROM app.schedule_shifts WHERE shift_id = :id"),
        {"id": shift_id}
    ).first()
    if not existing:
        raise HTTPException(status_code=404, detail="Shift not found")

    # ON DELETE CASCADE handles app.schedule_assignments
    db.execute(text("DELETE FROM app.schedule_shifts WHERE shift_id = :id"), {"id": shift_id})
    db.commit()


# ---------------------------------------------------------------------------
# POST /schedule/solve?week=YYYY-MM-DD  — solver stub
# ---------------------------------------------------------------------------
@router.post("/solve")
async def solve_schedule(
    week: str = Query(..., description="Week start date as YYYY-MM-DD"),
    db: Session = Depends(get_db),
    current_user=Depends(require_role(SCHEDULE_ROLES)),
):
    # Check that shifts exist for the week
    count = db.execute(
        text("SELECT COUNT(*) FROM app.schedule_shifts WHERE week_start_date = :week"),
        {"week": week}
    ).scalar()
    if not count:
        raise HTTPException(status_code=400, detail="No shifts defined for this week")

    raise HTTPException(
        status_code=501,
        detail="Solver not yet integrated. Define shifts and check back after solver implementation."
    )


# ---------------------------------------------------------------------------
# PUT /schedule/assignments/{assignment_id}  — supervisor override
# ---------------------------------------------------------------------------
@router.put("/assignments/{assignment_id}", response_model=AssignmentResponse)
async def update_assignment(
    assignment_id: int,
    update: AssignmentUpdate,
    db: Session = Depends(get_db),
    current_user=Depends(require_role(SCHEDULE_ROLES)),
):
    existing = db.execute(
        text("SELECT assignment_id, employee_id FROM app.schedule_assignments WHERE assignment_id = :id"),
        {"id": assignment_id}
    ).first()
    if not existing:
        raise HTTPException(status_code=404, detail="Assignment not found")

    db.execute(
        text("""
            UPDATE app.schedule_assignments
            SET employee_id       = :employee_id,
                is_manual_override = 1,
                notes             = :notes,
                updated_at        = GETDATE(),
                updated_by        = :updated_by
            WHERE assignment_id = :id
        """),
        {
            "employee_id": update.employee_id,
            "notes": update.notes,
            "updated_by": current_user.employee_id,
            "id": assignment_id,
        }
    )
    db.commit()

    row = db.execute(
        text("""
            SELECT
                a.assignment_id, a.shift_id, a.employee_id,
                a.solver_employee_id, a.is_manual_override, a.notes,
                a.updated_at, a.updated_by,
                NULLIF(LTRIM(RTRIM(
                    ISNULL(e.first_name, '') + ' ' + ISNULL(e.last_name, '')
                )), '') AS employee_name
            FROM app.schedule_assignments a
            LEFT JOIN pt.employees e ON e.employee_id = a.employee_id
            WHERE a.assignment_id = :id
        """),
        {"id": assignment_id}
    ).first()

    return AssignmentResponse(
        assignment_id=row.assignment_id,
        shift_id=row.shift_id,
        employee_id=row.employee_id,
        employee_name=row.employee_name,
        solver_employee_id=row.solver_employee_id,
        is_manual_override=bool(row.is_manual_override),
        notes=row.notes,
        updated_at=row.updated_at,
        updated_by=row.updated_by,
    )


# ---------------------------------------------------------------------------
# GET /schedule/metadata  — dropdown data for UI
# ---------------------------------------------------------------------------
@router.get("/metadata")
async def get_metadata(
    db: Session = Depends(get_db),
    current_user=Depends(require_role(SCHEDULE_ROLES)),
):
    locations_sql = text("""
        SELECT DISTINCT location
        FROM app.schedule_shifts
        ORDER BY location
    """)
    locations = [row.location for row in db.execute(locations_sql).fetchall()]

    employees_sql = text("""
        SELECT
            employee_id,
            LTRIM(RTRIM(ISNULL(first_name, '') + ' ' + ISNULL(last_name, ''))) AS full_name
        FROM pt.employees
        WHERE is_active = 1
        ORDER BY last_name, first_name
    """)
    employees = [
        {"employee_id": row.employee_id, "full_name": row.full_name}
        for row in db.execute(employees_sql).fetchall()
    ]

    return {"locations": locations, "employees": employees}
