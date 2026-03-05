"""
Special Events endpoints.

Staff can log events that affect parking operations (concerts, sports, festivals, etc.).
These events inform cashier scheduling and are inputs for future revenue prediction models.

"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from pydantic import BaseModel
from datetime import datetime

from app.db.session import get_db
from app.api.dependencies import require_role, get_current_active_user
from app.models.database import UserRole

router = APIRouter(prefix="/special-events", tags=["special-events"])

MANAGE_ROLES = [UserRole.SUPERVISOR, UserRole.MANAGER, UserRole.ADMIN]
VIEW_ROLES = list(UserRole)  # all authenticated roles can view

EVENT_TYPES = [
    "Concert", "Sports", "Festival", "Convention", "Parade",
    "Marathon/Race", "Fair/Expo", "Government/Civic", "Other"
]
STATUSES = ["Planned", "Confirmed", "Cancelled", "Completed"]


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class EventCreate(BaseModel):
    event_name: str
    event_start: str          # ISO datetime string from frontend
    event_end: str
    location_id: Optional[int] = None
    event_venue: Optional[str] = None
    event_type: Optional[str] = None
    status: str = "Planned"
    notes: Optional[str] = None
    ops_notes: Optional[str] = None


class EventUpdate(BaseModel):
    event_name: Optional[str] = None
    event_start: Optional[str] = None
    event_end: Optional[str] = None
    location_id: Optional[int] = None
    event_venue: Optional[str] = None
    event_type: Optional[str] = None
    status: Optional[str] = None
    notes: Optional[str] = None
    ops_notes: Optional[str] = None


# ── GET /special-events/metadata — dropdown data for the page ─────────────────
@router.get("/metadata")
async def get_metadata(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    locations = db.execute(text("""
        SELECT l.location_id, f.facility_name location_name
        FROM app.dim_location l
        INNER JOIN app.dim_facility f ON f.facility_id = l.facility_id
        WHERE 
            l.space_id IS NULL 
            AND f.facility_type IN ('lot', 'garage')
        ORDER BY f.facility_type, f.facility_name
    """)).fetchall()

    return {
        "event_types": EVENT_TYPES,
        "statuses":    STATUSES,
        "locations": [
            {"location_id": r.location_id, "location_name": r.location_name}
            for r in locations
        ],
    }


# ── GET /special-events — list with optional filters ─────────────────────────
@router.get("")
async def list_events(
    date_from:   Optional[str] = None,
    date_to:     Optional[str] = None,
    status:      Optional[str] = None,
    event_type:  Optional[str] = None,
    location_id: Optional[int] = None,
    search:      Optional[str] = None,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    where = ["1=1"]
    params: dict = {}

    if date_from:
        where.append("e.event_start >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("e.event_start <= :date_to")
        params["date_to"] = date_to + " 23:59:59"
    if status:
        where.append("e.status = :status")
        params["status"] = status
    if event_type:
        where.append("e.event_type = :event_type")
        params["event_type"] = event_type
    if location_id:
        where.append("e.location_id = :location_id")
        params["location_id"] = location_id
    if search:
        where.append("e.event_name LIKE :search")
        params["search"] = f"%{search}%"

    sql = text(f"""
        SELECT
            e.event_id,
            e.event_name,
            CONVERT(VARCHAR(19), e.event_start, 120) AS event_start,
            CONVERT(VARCHAR(19), e.event_end,   120) AS event_end,
            e.location_id,
            f.facility_name,
            e.event_venue,
            e.event_type,
            e.status,
            e.notes,
            e.ops_notes,
            e.created_by,
            CONVERT(VARCHAR(19), e.created_at, 120) AS created_at,
            e.updated_by,
            CONVERT(VARCHAR(19), e.updated_at, 120) AS updated_at
        FROM app.special_events e
        LEFT JOIN app.dim_location l ON l.location_id = e.location_id
        LEFT JOIN app.dim_facility f ON f.facility_id = l.facility_id
        WHERE {' AND '.join(where)}
        ORDER BY e.event_start ASC
    """)

    rows = db.execute(sql, params).fetchall()
    return [dict(r._mapping) for r in rows]


# ── POST /special-events — create ─────────────────────────────────────────────
@router.post("", status_code=201)
async def create_event(
    body: EventCreate,
    db: Session = Depends(get_db),
    current_user=Depends(require_role(MANAGE_ROLES)),
):
    sql = text("""
        INSERT INTO app.special_events
            (event_name, event_start, event_end, location_id, event_venue, event_type,
             status, notes, ops_notes, created_by, created_at)
        OUTPUT INSERTED.event_id
        VALUES
            (:event_name, :event_start, :event_end, :location_id, :event_venue, :event_type,
             :status, :notes, :ops_notes, :created_by, GETDATE())
    """)
    result = db.execute(sql, {
        "event_name":  body.event_name,
        "event_start": body.event_start,
        "event_end":   body.event_end,
        "location_id": body.location_id,
        "event_venue": body.event_venue,
        "event_type":  body.event_type,
        "status":      body.status,
        "notes":       body.notes,
        "ops_notes":   body.ops_notes,
        "created_by":  current_user.username,
    })
    new_id = result.scalar()
    db.commit()
    return {"event_id": new_id}


# ── PUT /special-events/{event_id} — update ──────────────────────────────────
@router.put("/{event_id}")
async def update_event(
    event_id: int,
    body: EventUpdate,
    db: Session = Depends(get_db),
    current_user=Depends(require_role(MANAGE_ROLES)),
):
    # Build dynamic SET clause from non-None fields
    set_clauses = []
    params: dict = {"event_id": event_id}

    field_map = {
        "event_name":  body.event_name,
        "event_start": body.event_start,
        "event_end":   body.event_end,
        "location_id": body.location_id,
        "event_venue": body.event_venue,
        "event_type":  body.event_type,
        "status":      body.status,
        "notes":       body.notes,
        "ops_notes":   body.ops_notes,
    }
    for col, val in field_map.items():
        if val is not None:
            set_clauses.append(f"{col} = :{col}")
            params[col] = val

    if not set_clauses:
        raise HTTPException(status_code=400, detail="No fields to update.")

    set_clauses.append("updated_by = :updated_by")
    set_clauses.append("updated_at = GETDATE()")
    params["updated_by"] = current_user.username

    sql = text(f"""
        UPDATE app.special_events
        SET {', '.join(set_clauses)}
        WHERE event_id = :event_id
    """)
    result = db.execute(sql, params)
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Event not found.")
    db.commit()
    return {"ok": True}


# ── DELETE /special-events/{event_id} — delete ───────────────────────────────
@router.delete("/{event_id}", status_code=204)
async def delete_event(
    event_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(require_role(MANAGE_ROLES)),
):
    result = db.execute(
        text("DELETE FROM app.special_events WHERE event_id = :id"),
        {"id": event_id}
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Event not found.")
    db.commit()
