"""Time-off request management endpoints.

DB table required (run once):

  CREATE TABLE app.time_off_requests (
      request_id    INT IDENTITY(1,1) PRIMARY KEY,
      employee_id   INT NOT NULL,
      request_type  VARCHAR(30)  NOT NULL,
      request_date  DATE         NOT NULL,
      submit_date   DATETIME     NOT NULL DEFAULT GETDATE(),
      submit_by     INT          NOT NULL,
      updated_at    DATETIME     NULL,
      updated_by    INT          NULL,
      is_cancelled     BIT          NOT NULL DEFAULT 0
  );
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from datetime import date

from app.db.session import get_db
from app.api.dependencies import require_role, get_current_active_user
from app.models.database import UserRole

router = APIRouter(prefix="/time-off", tags=["time-off"])

MANAGE_ROLES = [UserRole.SUPERVISOR, UserRole.MANAGER, UserRole.ADMIN]

REQUEST_TYPES = [
    "Off - Hourly", "Vacation", "Comp", "Paid Leave",
    "Sick", "Holiday", "FMLA", "In Late", "RDO",
]


def _can_manage(role: str) -> bool:
    return role in [r.value for r in MANAGE_ROLES]


def _role_str(current_user) -> str:
    return current_user.role.value if hasattr(current_user.role, "value") else current_user.role


# ---------------------------------------------------------------------------
# GET /time-off/requests
# ---------------------------------------------------------------------------
@router.get("/requests")
async def list_requests(
    employee_id: Optional[int] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    include_cancelled: bool = Query(False),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    user_role = _role_str(current_user)

    # Non-managers can only see their own
    if not _can_manage(user_role):
        employee_id = current_user.employee_id

    filters = ["1=1"]
    params: dict = {}

    if employee_id is not None:
        filters.append("r.employee_id = :employee_id")
        params["employee_id"] = employee_id

    if date_from:
        filters.append("r.request_date >= :date_from")
        params["date_from"] = date_from
    else:
        filters.append("r.request_date >= CAST(GETDATE() AS DATE)")  # default to today
        params["date_from"] = date.today()

    if date_to:
        filters.append("r.request_date <= :date_to")
        params["date_to"] = date_to

    if not include_cancelled:
        filters.append("r.is_cancelled = 0")

    where = " AND ".join(filters)

    sql = text(f"""
        SELECT
            r.request_id,
            r.employee_id,
            LTRIM(RTRIM(ISNULL(e.first_name,'') + ' ' + ISNULL(e.last_name,''))) AS employee_name,
            r.request_type,
            CONVERT(VARCHAR(10), r.request_date, 120) AS request_date,
            CONVERT(VARCHAR(19), r.submit_date,  120) AS submit_date,
            r.submit_by,
            LTRIM(RTRIM(ISNULL(sb.first_name,'') + ' ' + ISNULL(sb.last_name,''))) AS submit_by_name,
            CONVERT(VARCHAR(19), r.updated_at,   120) AS updated_at,
            r.updated_by,
            LTRIM(RTRIM(ISNULL(ub.first_name,'') + ' ' + ISNULL(ub.last_name,''))) AS updated_by_name,
            r.is_cancelled
        FROM app.time_off_requests r
        INNER JOIN  pt.employees e   ON e.employee_id  = r.employee_id
        LEFT JOIN  pt.employees sb  ON sb.employee_id = r.submit_by
        LEFT JOIN pt.employees ub ON ub.employee_id = r.updated_by
        WHERE {where}
        ORDER BY r.request_date DESC, r.submit_date DESC
    """)

    rows = db.execute(sql, params).fetchall()
    return [dict(row._mapping) for row in rows]


# ---------------------------------------------------------------------------
# POST /time-off/requests
# ---------------------------------------------------------------------------
@router.post("/requests", status_code=201)
async def create_request(
    payload: dict,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    user_role = _role_str(current_user)

    employee_id  = payload.get("employee_id")
    request_type = payload.get("request_type")
    request_date = payload.get("request_date")

    if not employee_id or not request_type or not request_date:
        raise HTTPException(status_code=422, detail="employee_id, request_type, and request_date are required")

    if request_type not in REQUEST_TYPES:
        raise HTTPException(status_code=422, detail=f"Invalid request_type. Allowed: {REQUEST_TYPES}")

    if not _can_manage(user_role) and int(employee_id) != current_user.employee_id:
        raise HTTPException(status_code=403, detail="You can only submit requests for yourself")

    sql = text("""
        INSERT INTO app.time_off_requests
            (employee_id, request_type, request_date, submit_date, submit_by, is_cancelled)
        OUTPUT INSERTED.request_id
        VALUES (:employee_id, :request_type, :request_date, GETDATE(), :submit_by, 0)
    """)
    result = db.execute(sql, {
        "employee_id":  employee_id,
        "request_type": request_type,
        "request_date": request_date,
        "submit_by":    current_user.employee_id,
    })
    db.commit()
    return {"request_id": result.scalar()}


# ---------------------------------------------------------------------------
# PUT /time-off/requests/{request_id}
# ---------------------------------------------------------------------------
@router.put("/requests/{request_id}")
async def update_request(
    request_id: int,
    payload: dict,
    db: Session = Depends(get_db),
    current_user=Depends(require_role(MANAGE_ROLES)),
):
    request_type = payload.get("request_type")
    request_date = payload.get("request_date")

    if not request_type and not request_date:
        raise HTTPException(status_code=422, detail="Nothing to update")

    if request_type and request_type not in REQUEST_TYPES:
        raise HTTPException(status_code=422, detail="Invalid request_type")

    sets = ["updated_at = GETDATE()", "updated_by = :updated_by"]
    params: dict = {"request_id": request_id, "updated_by": current_user.employee_id}

    if request_type:
        sets.append("request_type = :request_type")
        params["request_type"] = request_type

    if request_date:
        sets.append("request_date = :request_date")
        params["request_date"] = request_date

    result = db.execute(
        text(f"UPDATE app.time_off_requests SET {', '.join(sets)} WHERE request_id = :request_id AND is_cancelled = 0"),
        params,
    )
    db.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Request not found or already cancelled")

    return {"ok": True}


# ---------------------------------------------------------------------------
# PUT /time-off/requests/{request_id}/cancel
# ---------------------------------------------------------------------------
@router.put("/requests/{request_id}/cancel")
async def cancel_request(
    request_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    user_role = _role_str(current_user)

    row = db.execute(
        text("SELECT employee_id, is_cancelled FROM app.time_off_requests WHERE request_id = :id"),
        {"id": request_id},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Request not found")

    if not _can_manage(user_role) and row.employee_id != current_user.employee_id:
        raise HTTPException(status_code=403, detail="You can only cancel your own requests")

    if row.is_cancelled:
        raise HTTPException(status_code=400, detail="Request already cancelled")

    db.execute(
        text("""
            UPDATE app.time_off_requests
            SET is_cancelled = 1, updated_at = GETDATE(), updated_by = :updated_by
            WHERE request_id = :id
        """),
        {"id": request_id, "updated_by": current_user.employee_id},
    )
    db.commit()
    return {"ok": True}


# ---------------------------------------------------------------------------
# GET /time-off/metadata — employee list + request types for dropdowns
# ---------------------------------------------------------------------------
@router.get("/metadata")
async def get_metadata(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    rows = db.execute(text("""
        SELECT
            e.employee_id,
            LTRIM(RTRIM(ISNULL(first_name, '') + ' ' + ISNULL(last_name, ''))) AS full_name
        FROM pt.employees e
        WHERE is_active = 1
        ORDER BY last_name, first_name
    """)).fetchall()

    return {
        "request_types": REQUEST_TYPES,
        "employees": [{"employee_id": r.employee_id, "full_name": r.full_name} for r in rows],
        "user_role":    current_user.role.value if hasattr(current_user.role, "value") else current_user.role,
        "employee_id":  current_user.employee_id,
    }
