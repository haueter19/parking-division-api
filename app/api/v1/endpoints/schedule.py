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
from app.api.dependencies import require_role, get_current_active_user
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
            s.special_event,
            s.safe_num,
            s.bag_num,
            s.rush,
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
            special_event=bool(row.special_event),
            safe_num=row.safe_num,
            bag_num=row.bag_num,
            rush=row.rush,
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
            (week_start_date, location, booth, day_of_week, start_hour, end_hour,
             special_event, safe_num, bag_num, rush, created_by)
        OUTPUT INSERTED.shift_id
        VALUES (:week, :location, :booth, :day, :start_hour, :end_hour,
                :special_event, :safe_num, :bag_num, :rush, :created_by)
    """)
    result = db.execute(sql, {
        "week": shift.week_start_date,
        "location": shift.location,
        "booth": shift.booth,
        "day": shift.day_of_week,
        "start_hour": shift.start_hour,
        "end_hour": shift.end_hour,
        "special_event": shift.special_event,
        "safe_num": shift.safe_num,
        "bag_num": shift.bag_num,
        "rush": shift.rush,
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

    # Build dynamic SET clause from provided fields.
    # Use model_fields_set so explicitly-null values (e.g. clearing bag_num) are included.
    updates = {}
    provided = shift.model_fields_set
    if "location" in provided and shift.location is not None:
        updates["location"] = shift.location
    if "booth" in provided and shift.booth is not None:
        updates["booth"] = shift.booth
    if "day_of_week" in provided and shift.day_of_week is not None:
        updates["day_of_week"] = shift.day_of_week
    if "start_hour" in provided and shift.start_hour is not None:
        updates["start_hour"] = shift.start_hour
    if "end_hour" in provided and shift.end_hour is not None:
        updates["end_hour"] = shift.end_hour
    if "special_event" in provided and shift.special_event is not None:
        updates["special_event"] = shift.special_event
    # Nullable int fields — include whenever explicitly sent (even if None/null)
    for field in ("safe_num", "bag_num", "rush"):
        if field in provided:
            updates[field] = getattr(shift, field)

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
# POST /schedule/solve?week=YYYY-MM-DD
# ---------------------------------------------------------------------------
@router.post("/solve")
async def solve_schedule(
    week: str = Query(..., description="Week start date as YYYY-MM-DD"),
    db: Session = Depends(get_db),
    current_user=Depends(require_role(SCHEDULE_ROLES)),
):
    from datetime import date as _date
    from ortools.sat.python import cp_model
    from app.utils.schedule_solver import ParkingScheduler

    count = db.execute(
        text("SELECT COUNT(*) FROM app.schedule_shifts WHERE week_start_date = :week"),
        {"week": week}
    ).scalar()
    if not count:
        raise HTTPException(status_code=400, detail="No shifts defined for this week")

    # Build a per-key deque of shift_ids so that two shifts with identical
    # (location, booth, day, start, end) each get their own assignment row
    # and don't collide on UQ_assignments_shift.
    from collections import deque as _deque, defaultdict as _defaultdict

    shift_rows = db.execute(text("""
        SELECT shift_id, location, booth, day_of_week,
               CAST(start_hour AS FLOAT) AS start_hour,
               CAST(end_hour   AS FLOAT) AS end_hour
        FROM app.schedule_shifts
        WHERE week_start_date = :week
    """), {"week": week}).fetchall()
    shift_queue: dict = _defaultdict(_deque)
    for r in shift_rows:
        key = (r.location, int(r.booth), r.day_of_week, float(r.start_hour), float(r.end_hour))
        shift_queue[key].append(r.shift_id)

    scheduler = ParkingScheduler(week_start=_date.fromisoformat(week), db=db)
    scheduler.build().solve()

    if scheduler._solution not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        raise HTTPException(
            status_code=422,
            detail=f"Solver could not find a valid schedule: {scheduler.status_label()}"
        )

    # Replace all existing assignments for the week
    db.execute(text("""
        DELETE FROM app.schedule_assignments
        WHERE shift_id IN (
            SELECT shift_id FROM app.schedule_shifts WHERE week_start_date = :week
        )
    """), {"week": week})

    saved = 0
    for day, day_shifts in scheduler.schedule.items():
        for garage, booth, start, end, employee_id in day_shifts:
            key = (garage, int(booth), day, float(start), float(end))
            if not shift_queue[key]:
                continue  # no matching DB shift — solver/DB mismatch
            sid = shift_queue[key].popleft()
            db.execute(text("""
                INSERT INTO app.schedule_assignments
                    (shift_id, employee_id, solver_employee_id, is_manual_override)
                VALUES (:sid, :emp, :emp, 0)
            """), {"sid": sid, "emp": int(employee_id)})
            saved += 1

    db.commit()
    return {"status": scheduler.status_label(), "assignments_saved": saved}


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
# GET /schedule/employee-weeks?employee_id=X  — weeks with assignments for an employee
# Accessible to any authenticated user; non-supervisors can only query themselves.
# ---------------------------------------------------------------------------
@router.get("/employee-weeks")
async def get_employee_weeks(
    employee_id: int = Query(...),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    user_role = current_user.role
    if isinstance(user_role, str):
        try:
            user_role = UserRole(user_role.lower())
        except ValueError:
            pass
    if user_role not in SCHEDULE_ROLES and current_user.employee_id != employee_id:
        raise HTTPException(status_code=403, detail="Not authorized to view this employee's schedule")

    rows = db.execute(text("""
        SELECT DISTINCT CONVERT(VARCHAR(10), s.week_start_date, 120) AS week_start_date
        FROM app.schedule_assignments a
        INNER JOIN app.schedule_shifts s ON s.shift_id = a.shift_id
        WHERE a.employee_id = :employee_id
        ORDER BY week_start_date DESC
    """), {"employee_id": employee_id}).fetchall()
    return [row.week_start_date for row in rows]


# ---------------------------------------------------------------------------
# GET /schedule/employee-schedule?week=YYYY-MM-DD&employee_id=X
# Accessible to any authenticated user; non-supervisors can only query themselves.
# ---------------------------------------------------------------------------
@router.get("/employee-schedule")
async def get_employee_schedule(
    week: str = Query(...),
    employee_id: int = Query(...),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    user_role = current_user.role
    if isinstance(user_role, str):
        try:
            user_role = UserRole(user_role.lower())
        except ValueError:
            pass
    if user_role not in SCHEDULE_ROLES and current_user.employee_id != employee_id:
        raise HTTPException(status_code=403, detail="Not authorized to view this employee's schedule")

    rows = db.execute(text("""
        SELECT
            s.shift_id,
            s.location,
            s.booth,
            s.day_of_week,
            CAST(s.start_hour AS FLOAT) AS start_hour,
            CAST(s.end_hour   AS FLOAT) AS end_hour,
            s.special_event,
            NULLIF(LTRIM(RTRIM(
                ISNULL(e.first_name, '') + ' ' + ISNULL(e.last_name, '')
            )), '') AS employee_name
        FROM app.schedule_shifts s
        INNER JOIN app.schedule_assignments a ON a.shift_id = s.shift_id
        INNER JOIN pt.employees e ON e.employee_id = a.employee_id
        WHERE s.week_start_date = :week
          AND a.employee_id = :employee_id
        ORDER BY
            CASE s.day_of_week
                WHEN 'Sun' THEN 0 WHEN 'Mon' THEN 1 WHEN 'Tue' THEN 2
                WHEN 'Wed' THEN 3 WHEN 'Thu' THEN 4 WHEN 'Fri' THEN 5
                WHEN 'Sat' THEN 6 ELSE 7 END,
            s.start_hour
    """), {"week": week, "employee_id": employee_id}).fetchall()

    return [
        {
            "shift_id":     row.shift_id,
            "location":     row.location,
            "booth":        row.booth,
            "day_of_week":  row.day_of_week,
            "start_hour":   row.start_hour,
            "end_hour":     row.end_hour,
            "special_event": bool(row.special_event),
            "employee_name": row.employee_name,
        }
        for row in rows
    ]


# ---------------------------------------------------------------------------
# POST /schedule/preload?week=YYYY-MM-DD
# Inserts routine fixed shifts (Chan/McConley) and event-implied shifts
# ---------------------------------------------------------------------------
@router.post("/preload", status_code=201)
async def preload_shifts(
    week: str = Query(..., description="Week start date as YYYY-MM-DD (Sunday)"),
    db: Session = Depends(get_db),
    current_user=Depends(require_role(SCHEDULE_ROLES)),
):
    from datetime import date as _date, timedelta as _td

    week_date = _date.fromisoformat(week)
    week_end  = week_date + _td(days=6)

    # Sun=0 … Sat=6 (Python weekday: Mon=0, Sun=6)
    _DOW = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}

    created = 0

    def _insert(location, booth, day, start_hour, end_hour,
                special_event=False, safe_num=None, bag_num=None, rush=None):
        nonlocal created
        db.execute(text("""
            INSERT INTO app.schedule_shifts
                (week_start_date, location, booth, day_of_week, start_hour, end_hour,
                 special_event, safe_num, bag_num, rush, created_by)
            VALUES (:week, :location, :booth, :day, :start_hour, :end_hour,
                    :special_event, :safe_num, :bag_num, :rush, :created_by)
        """), {
            "week":          week,
            "location":      location,
            "booth":         booth,
            "day":           day,
            "start_hour":    start_hour,
            "end_hour":      end_hour,
            "special_event": special_event,
            "safe_num":      safe_num,
            "bag_num":       bag_num,
            "rush":          rush,
            "created_by":    current_user.employee_id,
        })
        created += 1

    # ── Chan: Frances Booth 1, AM, Mon–Fri ──
    for day in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']:
        _insert('Frances', 1, day, 11.0, 19.5)

    # ── McConley: Frances Booth 2, PM, Tue–Sat ──
    for day in ['Tue', 'Wed']:
        _insert('Frances', 2, day, 15.0, 23.0)
    _insert('Frances', 2, 'Thu', 15.0, 23.5)
    for day in ['Fri', 'Sat']:
        _insert('Frances', 2, day, 16.0, 25.0)

    # ── Special-event implied shifts ──
    events = db.execute(text("""
        SELECT se.*, f.facility_name
        FROM app.special_events se
        INNER JOIN app.dim_location l ON se.location_id = l.location_id
        INNER JOIN app.dim_facility f ON l.facility_id  = f.facility_id
        WHERE se.event_start BETWEEN :start_date AND :end_date
    """), {
        "start_date": str(week_date),
        "end_date":   str(week_end) + " 23:59:59",
    }).fetchall()

    for ev in events:
        facility = (ev.facility_name or '').strip()
        fname_lc = facility.lower()

        # Booth assignment by venue
        if 'overture' in fname_lc:
            booths = [1, 3]
        elif 'state' in fname_lc:
            booths = [2, 4]
        else:  # Livingston and any other venue
            booths = [1, 2]

        # Decimal hours from event datetimes
        ev_start = ev.event_start   # datetime from SQL Server
        ev_end   = ev.event_end
        start_hour = ev_start.hour + ev_start.minute / 60.0
        end_hour   = ev_end.hour   + ev_end.minute   / 60.0
        if ev_end.date() > ev_start.date():
            end_hour += 24.0

        rush = int((5 - (end_hour - start_hour)) * 60)

        # Day-of-week from event_start (Python weekday: Mon=0 … Sun=6)
        day_of_week = _DOW[ev_start.weekday()]

        for booth in booths:
            _insert(facility, booth, day_of_week, start_hour, end_hour,
                    special_event=True, rush=rush)

    db.commit()
    return {"shifts_created": created}


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
    #locations = [row.location for row in db.execute(locations_sql).fetchall()]
    locations = ['Frances', 'SSCo', 'OC', 'SLS', 'CSN', 'Lake', 'Wilson']
    
    employees_sql = text("""
        SELECT
            e.employee_id,
            LTRIM(RTRIM(ISNULL(first_name, '') + ' ' + ISNULL(last_name, ''))) AS full_name,
            e.role,
            c.cashier_id
        FROM pt.employees e
        INNER JOIN app.cashier_id c ON e.employee_id = c.employee_id
        WHERE is_active = 1
        ORDER BY last_name, first_name
    """)
    employees = [
        {"employee_id": row.employee_id, "full_name": row.full_name, "role": row.role, "cashier_id": row.cashier_id}
        for row in db.execute(employees_sql).fetchall()
    ]

    return {"locations": locations, "employees": employees}
