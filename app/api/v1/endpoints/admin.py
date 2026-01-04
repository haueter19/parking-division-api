"""
Admin Configuration Endpoints
Manage devices, settlement systems, payment methods, and device assignments
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import text, and_, or_
from typing import Optional, List
from datetime import datetime

from app.db.session import get_db
from app.api.dependencies import get_current_active_user, require_role
from app.models.database import User, UserRole
from app.models.schemas import (
    DeviceCreate, DeviceResponse,
    SettlementSystemCreate, SettlementSystemResponse,
    PaymentMethodCreate, PaymentMethodResponse,
    DeviceAssignmentCreate, DeviceAssignmentUpdate, DeviceAssignmentResponse,
    FacilityResponse, SpaceResponse, LocationResponse, ChargeCodeResponse, SpaceCreate
)

router = APIRouter(prefix="/admin", tags=["admin"])


# ============= Device Management =============

@router.post("/devices", response_model=DeviceResponse)
async def create_device(
    device: DeviceCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """Create a new device (ADMIN only)"""
    
    # Check if device_terminal_id already exists
    existing = db.execute(
        text("SELECT device_id FROM app.dim_device WHERE device_terminal_id = :terminal_id"),
        {"terminal_id": device.device_terminal_id}
    ).first()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Device with terminal ID '{device.device_terminal_id}' already exists"
        )
    
    # Insert new device
    insert_sql = text("""
        INSERT INTO app.dim_device (
            device_terminal_id, device_type, supports_cash, supports_card, supports_mobile,
            cwAssetID, SerialNumber, Brand, Model
        )
        OUTPUT INSERTED.device_id, INSERTED.device_terminal_id, INSERTED.device_type,
               INSERTED.supports_cash, INSERTED.supports_card, INSERTED.supports_mobile,
               INSERTED.cwAssetID, INSERTED.SerialNumber, INSERTED.Brand, INSERTED.Model
        VALUES (
            :terminal_id, :device_type, :supports_cash, :supports_card, :supports_mobile,
            :cwAssetID, :SerialNumber, :Brand, :Model
        )
    """)
    
    result = db.execute(insert_sql, {
        "terminal_id": device.device_terminal_id,
        "device_type": device.device_type,
        "supports_cash": device.supports_cash,
        "supports_card": device.supports_card,
        "supports_mobile": device.supports_mobile,
        "cwAssetID": device.cwAssetID,
        "SerialNumber": device.SerialNumber,
        "Brand": device.Brand,
        "Model": device.Model
    }).first()
    
    db.commit()
    
    return DeviceResponse(
        device_id=result.device_id,
        device_terminal_id=result.device_terminal_id,
        device_type=result.device_type,
        supports_cash=result.supports_cash,
        supports_card=result.supports_card,
        supports_mobile=result.supports_mobile,
        cwAssetID=result.cwAssetID,
        SerialNumber=result.SerialNumber,
        Brand=result.Brand,
        Model=result.Model
    )


@router.get("/devices", response_model=List[DeviceResponse])
async def list_devices(
    skip: int = 0,
    limit: int = 100,
    device_type: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """List all devices (ADMIN only)"""
    
    filter_clause = "WHERE device_type NOT IN ('Virtual', 'MK5')"
    if device_type:
        filter_clause = "WHERE device_type = :device_type"
    
    query = text(f"""
        SELECT device_id, device_terminal_id, device_type, supports_cash, supports_card, 
               supports_mobile, cwAssetID, SerialNumber, Brand, Model
        FROM app.dim_device
        {filter_clause}
        ORDER BY device_type, device_terminal_id
        OFFSET :skip ROWS
        FETCH NEXT :limit ROWS ONLY
    """)
    
    params = {"skip": skip, "limit": limit}
    if device_type:
        params["device_type"] = device_type
    
    results = db.execute(query, params).fetchall()
    
    return [
        DeviceResponse(
            device_id=r.device_id,
            device_terminal_id=r.device_terminal_id,
            device_type=r.device_type,
            supports_cash=r.supports_cash,
            supports_card=r.supports_card,
            supports_mobile=r.supports_mobile,
            cwAssetID=r.cwAssetID,
            SerialNumber=r.SerialNumber,
            Brand=r.Brand,
            Model=r.Model
        )
        for r in results
    ]


@router.get("/devices/{device_id}", response_model=DeviceResponse)
async def get_device(
    device_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """Get device details (ADMIN only)"""
    
    query = text("""
        SELECT device_id, device_terminal_id, device_type, supports_cash, supports_card,
               supports_mobile, cwAssetID, SerialNumber, Brand, Model
        FROM app.dim_device
        WHERE device_id = :device_id
    """)
    
    result = db.execute(query, {"device_id": device_id}).first()
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {device_id} not found"
        )
    
    return DeviceResponse(
        device_id=result.device_id,
        device_terminal_id=result.device_terminal_id,
        device_type=result.device_type,
        supports_cash=result.supports_cash,
        supports_card=result.supports_card,
        supports_mobile=result.supports_mobile,
        cwAssetID=result.cwAssetID,
        SerialNumber=result.SerialNumber,
        Brand=result.Brand,
        Model=result.Model
    )


@router.get("/metadata")
async def admin_metadata(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """
    Return comprehensive admin metadata with JOINed data for frontend caching.
    Loads ALL data once on page load for client-side filtering.
    """
    
    # Devices with current assignment info
    devices_q = text("""
        SELECT 
            d.device_id, 
            d.device_terminal_id, 
            d.device_type,
            d.supports_cash,
            d.supports_card,
            d.supports_mobile,
            d.cwAssetID,
            d.SerialNumber,
            d.Brand,
            d.Model,
            da.assignment_id,
            da.location_id,
            f.facility_name,
            s.space_number
        FROM app.dim_device d
        LEFT JOIN (
            SELECT device_id, assignment_id, location_id
            FROM app.fact_device_assignment
            WHERE end_date IS NULL
        ) da ON d.device_id = da.device_id
        LEFT JOIN app.dim_location l ON da.location_id = l.location_id
        LEFT JOIN app.dim_facility f ON l.facility_id = f.facility_id
        LEFT JOIN app.dim_space s ON l.space_id = s.space_id
        ORDER BY d.device_terminal_id
    """)
    devices = [dict(r._mapping) for r in db.execute(devices_q).fetchall()]

    # Locations with facility and space details
    locations_q = text("""
        SELECT 
            l.location_id, 
            l.facility_id, 
            l.space_id,
            f.facility_name,
            f.facility_type,
            s.space_number,
            s.space_type
        FROM app.dim_location l
        INNER JOIN app.dim_facility f ON l.facility_id = f.facility_id
        LEFT JOIN app.dim_space s ON l.space_id = s.space_id
        ORDER BY f.facility_name, s.space_number
    """)
    locations = [dict(r._mapping) for r in db.execute(locations_q).fetchall()]

    # Facilities
    facilities_q = text("""
        SELECT 
            facility_id, 
            facility_name, 
            facility_nickname,
            facility_type,
            on_off_street,
            street_area
        FROM app.dim_facility
        ORDER BY facility_name
    """)
    facilities = [dict(r._mapping) for r in db.execute(facilities_q).fetchall()]

    # Spaces (active and historical)
    spaces_q = text("""
        SELECT 
            s.space_id,
            s.space_number,
            s.space_type,
            s.facility_id,
            s.cwAssetID,
            s.start_date,
            s.end_date,
            s.space_status,
            f.facility_name
        FROM app.dim_space s
        INNER JOIN app.dim_facility f ON s.facility_id = f.facility_id
        ORDER BY f.facility_name, s.space_number, s.start_date DESC
    """)
    spaces = [dict(r._mapping) for r in db.execute(spaces_q).fetchall()]

    # Device types (distinct)
    device_types_q = text("""
        SELECT DISTINCT device_type
        FROM app.dim_device
        WHERE device_type IS NOT NULL
        ORDER BY device_type
    """)
    device_types = [r.device_type for r in db.execute(device_types_q).fetchall()]

    # Settlement systems
    settlement_q = text("""
        SELECT settlement_system_id, system_name, system_type
        FROM app.dim_settlement_system
        ORDER BY system_name
    """)
    settlement_systems = [dict(r._mapping) for r in db.execute(settlement_q).fetchall()]

    # Payment methods
    payment_q = text("""
        SELECT 
            payment_method_id, 
            payment_method_brand, 
            payment_method_type,
            is_cash, 
            is_card, 
            is_mobile, 
            is_check
        FROM app.dim_payment_method
        ORDER BY payment_method_brand
    """)
    payment_methods = [dict(r._mapping) for r in db.execute(payment_q).fetchall()]

    # Device assignments with full details
    assignments_q = text("""
        SELECT 
            da.assignment_id,
            da.device_id,
            da.location_id,
            da.assign_date,
            da.end_date,
            da.assign_by_id,
            da.end_by_id,
            da.workorder_assign_id,
            da.workorder_remove_id,
            da.notes,
            d.device_terminal_id,
            d.device_type,
            f.facility_id,
            f.facility_name,
            s.space_id,
            s.space_number
        FROM app.fact_device_assignment da
        INNER JOIN app.dim_device d ON da.device_id = d.device_id
        INNER JOIN app.dim_location l ON da.location_id = l.location_id
        INNER JOIN app.dim_facility f ON l.facility_id = f.facility_id
        LEFT JOIN app.dim_space s ON l.space_id = s.space_id
        ORDER BY da.assign_date DESC
    """)
    device_assignments = [dict(r._mapping) for r in db.execute(assignments_q).fetchall()]

    return {
        "devices": devices,
        "locations": locations,
        "facilities": facilities,
        "spaces": spaces,
        "device_types": device_types,
        "settlement_systems": settlement_systems,
        "payment_methods": payment_methods,
        "device_assignments": device_assignments
    }


# ============= Settlement System Management =============

@router.post("/settlement-systems", response_model=SettlementSystemResponse)
async def create_settlement_system(
    system: SettlementSystemCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """Create a new settlement system (ADMIN only)"""
    
    # Check if system_name already exists
    existing = db.execute(
        text("SELECT settlement_system_id FROM app.dim_settlement_system WHERE system_name = :name"),
        {"name": system.system_name}
    ).first()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Settlement system '{system.system_name}' already exists"
        )
    
    insert_sql = text("""
        INSERT INTO app.dim_settlement_system (system_name, system_type)
        OUTPUT INSERTED.settlement_system_id, INSERTED.system_name, INSERTED.system_type
        VALUES (:system_name, :system_type)
    """)
    
    result = db.execute(insert_sql, {
        "system_name": system.system_name,
        "system_type": system.system_type
    }).first()
    
    db.commit()
    
    return SettlementSystemResponse(
        settlement_system_id=result.settlement_system_id,
        system_name=result.system_name,
        system_type=result.system_type
    )


@router.get("/settlement-systems", response_model=List[SettlementSystemResponse])
async def list_settlement_systems(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """List all settlement systems (ADMIN only)"""
    
    query = text("""
        SELECT settlement_system_id, system_name, system_type
        FROM app.dim_settlement_system
        ORDER BY system_name
    """)
    
    results = db.execute(query).fetchall()
    
    return [
        SettlementSystemResponse(
            settlement_system_id=r.settlement_system_id,
            system_name=r.system_name,
            system_type=r.system_type
        )
        for r in results
    ]


# ============= Payment Method Management =============

@router.post("/payment-methods", response_model=PaymentMethodResponse)
async def create_payment_method(
    method: PaymentMethodCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """Create a new payment method (ADMIN only)"""
    
    # Check if payment_method_brand already exists
    existing = db.execute(
        text("SELECT payment_method_id FROM app.dim_payment_method WHERE payment_method_brand = :brand"),
        {"brand": method.payment_method_brand}
    ).first()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Payment method '{method.payment_method_brand}' already exists"
        )
    
    insert_sql = text("""
        INSERT INTO app.dim_payment_method (
            payment_method_brand, payment_method_type, is_cash, is_card, is_mobile, is_check
        )
        OUTPUT INSERTED.payment_method_id, INSERTED.payment_method_brand, INSERTED.payment_method_type,
               INSERTED.is_cash, INSERTED.is_card, INSERTED.is_mobile, INSERTED.is_check
        VALUES (:brand, :type, :is_cash, :is_card, :is_mobile, :is_check)
    """)
    
    result = db.execute(insert_sql, {
        "brand": method.payment_method_brand,
        "type": method.payment_method_type,
        "is_cash": method.is_cash,
        "is_card": method.is_card,
        "is_mobile": method.is_mobile,
        "is_check": method.is_check
    }).first()
    
    db.commit()
    
    return PaymentMethodResponse(
        payment_method_id=result.payment_method_id,
        payment_method_brand=result.payment_method_brand,
        payment_method_type=result.payment_method_type,
        is_cash=result.is_cash,
        is_card=result.is_card,
        is_mobile=result.is_mobile,
        is_check=result.is_check
    )


@router.get("/payment-methods", response_model=List[PaymentMethodResponse])
async def list_payment_methods(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """List all payment methods (ADMIN only)"""
    
    query = text("""
        SELECT payment_method_id, payment_method_brand, payment_method_type,
               is_cash, is_card, is_mobile, is_check
        FROM app.dim_payment_method
        ORDER BY payment_method_brand
    """)
    
    results = db.execute(query).fetchall()
    
    return [
        PaymentMethodResponse(
            payment_method_id=r.payment_method_id,
            payment_method_brand=r.payment_method_brand,
            payment_method_type=r.payment_method_type,
            is_cash=r.is_cash,
            is_card=r.is_card,
            is_mobile=r.is_mobile,
            is_check=r.is_check
        )
        for r in results
    ]


# ============= Device Assignment Management =============

@router.post("/device-assignments", response_model=DeviceAssignmentResponse)
async def create_device_assignment(
    assignment: DeviceAssignmentCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """
    Create a new device assignment (ADMIN only)
    
    This handles the complete workflow:
    1. Get/create location (facility + space combo)
    2. Create device assignment
    3. Get/create charge code for location + program
    """
    
    # Validate device exists
    device_check = db.execute(
        text("SELECT device_id FROM app.dim_device WHERE device_id = :device_id"),
        {"device_id": assignment.device_id}
    ).first()
    
    if not device_check:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {assignment.device_id} not found"
        )
    
    # Validate facility exists
    facility_check = db.execute(
        text("SELECT facility_id FROM app.dim_facility WHERE facility_id = :facility_id"),
        {"facility_id": assignment.facility_id}
    ).first()
    
    if not facility_check:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Facility {assignment.facility_id} not found"
        )
    
    # If space_id provided, validate it exists
    if assignment.space_id:
        space_check = db.execute(
            text("SELECT space_id FROM app.dim_space WHERE space_id = :space_id"),
            {"space_id": assignment.space_id}
        ).first()
        
        if not space_check:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Space {assignment.space_id} not found"
            )
    
    # Check for overlapping assignments
    overlap_check = text("""
        SELECT assignment_id
        FROM app.fact_device_assignment
        WHERE device_id = :device_id
          AND (
              -- New assignment starts during existing assignment
              (:assign_date >= assign_date AND :assign_date < COALESCE(end_date, '9999-12-31'))
              OR
              -- New assignment ends during existing assignment
              (COALESCE(:end_date, '9999-12-31') > assign_date AND COALESCE(:end_date, '9999-12-31') <= COALESCE(end_date, '9999-12-31'))
              OR
              -- New assignment completely contains existing assignment
              (:assign_date <= assign_date AND COALESCE(:end_date, '9999-12-31') >= COALESCE(end_date, '9999-12-31'))
          )
    """)
    
    overlap = db.execute(overlap_check, {
        "device_id": assignment.device_id,
        "assign_date": assignment.assign_date,
        "end_date": assignment.end_date
    }).first()
    
    if overlap:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Device assignment overlaps with existing assignment {overlap.assignment_id}"
        )
    
    # Step 1: Get or create location
    location_query = text("""
        SELECT location_id
        FROM app.dim_location
        WHERE facility_id = :facility_id
          AND (
              (space_id IS NULL AND :space_id IS NULL)
              OR (space_id = :space_id)
          )
    """)
    
    location_result = db.execute(location_query, {
        "facility_id": assignment.facility_id,
        "space_id": assignment.space_id
    }).first()
    
    if location_result:
        location_id = location_result.location_id
    else:
        # Create new location
        location_insert = text("""
            INSERT INTO app.dim_location (facility_id, space_id)
            OUTPUT INSERTED.location_id
            VALUES (:facility_id, :space_id)
        """)
        
        location_id = db.execute(location_insert, {
            "facility_id": assignment.facility_id,
            "space_id": assignment.space_id
        }).scalar()
    
    # Step 2: Create device assignment
    assignment_insert = text("""
        INSERT INTO app.fact_device_assignment (
            device_id, location_id, assign_date, end_date,
            assign_by_id, workorder_assign_id, notes
        )
        OUTPUT INSERTED.assignment_id, INSERTED.device_id, INSERTED.location_id,
               INSERTED.assign_date, INSERTED.end_date, INSERTED.assign_by_id,
               INSERTED.end_by_id, INSERTED.workorder_assign_id, INSERTED.workorder_remove_id,
               INSERTED.notes
        VALUES (
            :device_id, :location_id, :assign_date, :end_date,
            :assign_by_id, :workorder_assign_id, :notes
        )
    """)
    
    result = db.execute(assignment_insert, {
        "device_id": assignment.device_id,
        "location_id": location_id,
        "assign_date": assignment.assign_date,
        "end_date": assignment.end_date,
        "assign_by_id": current_user.id,
        "workorder_assign_id": assignment.workorder_assign_id,
        "notes": assignment.notes
    }).first()
    
    # Step 3: Get or create charge code
    program_id = assignment.program_id or 1  # Default to regular program
    
    charge_code_query = text("""
        SELECT charge_code_id
        FROM app.dim_charge_code
        WHERE location_id = :location_id
          AND program_type_id = :program_id
    """)
    
    charge_code_result = db.execute(charge_code_query, {
        "location_id": location_id,
        "program_id": program_id
    }).first()
    
    if not charge_code_result:
        # Get next charge code number
        max_code = db.execute(
            text("SELECT MAX(charge_code) FROM app.dim_charge_code")
        ).scalar() or 82000
        
        new_charge_code = max_code + 1
        
        # Create new charge code
        charge_code_insert = text("""
            INSERT INTO app.dim_charge_code (
                charge_code, location_id, program_type_id, description
            )
            VALUES (:charge_code, :location_id, :program_id, :description)
        """)
        
        db.execute(charge_code_insert, {
            "charge_code": new_charge_code,
            "location_id": location_id,
            "program_id": program_id,
            "description": f"Auto-created for location {location_id}"
        })
    
    db.commit()
    
    return DeviceAssignmentResponse(
        assignment_id=result.assignment_id,
        device_id=result.device_id,
        location_id=result.location_id,
        assign_date=result.assign_date,
        end_date=result.end_date,
        assign_by_id=result.assign_by_id,
        end_by_id=result.end_by_id,
        workorder_assign_id=result.workorder_assign_id,
        workorder_remove_id=result.workorder_remove_id,
        notes=result.notes
    )


@router.put("/device-assignments/{assignment_id}", response_model=DeviceAssignmentResponse)
async def update_device_assignment(
    assignment_id: int,
    update: DeviceAssignmentUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """Update an existing device assignment (ADMIN only)"""
    
    # Get existing assignment
    existing = db.execute(
        text("""
            SELECT assignment_id, device_id, location_id, assign_date, end_date
            FROM app.fact_device_assignment
            WHERE assignment_id = :assignment_id
        """),
        {"assignment_id": assignment_id}
    ).first()
    
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assignment {assignment_id} not found"
        )
    
    # Check for overlaps if dates are being changed
    if update.assign_date or update.end_date:
        new_assign_date = update.assign_date or existing.assign_date
        new_end_date = update.end_date if update.end_date is not None else existing.end_date
        
        overlap_check = text("""
            SELECT assignment_id
            FROM app.fact_device_assignment
            WHERE device_id = :device_id
              AND assignment_id != :assignment_id
              AND (
                  (:assign_date >= assign_date AND :assign_date < COALESCE(end_date, '9999-12-31'))
                  OR
                  (COALESCE(:end_date, '9999-12-31') > assign_date AND COALESCE(:end_date, '9999-12-31') <= COALESCE(end_date, '9999-12-31'))
                  OR
                  (:assign_date <= assign_date AND COALESCE(:end_date, '9999-12-31') >= COALESCE(end_date, '9999-12-31'))
              )
        """)
        
        overlap = db.execute(overlap_check, {
            "device_id": existing.device_id,
            "assignment_id": assignment_id,
            "assign_date": new_assign_date,
            "end_date": new_end_date
        }).first()
        
        if overlap:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Updated assignment would overlap with assignment {overlap.assignment_id}"
            )
    
    # Build update query dynamically
    update_parts = []
    params = {"assignment_id": assignment_id}
    
    if update.location_id is not None:
        update_parts.append("location_id = :location_id")
        params["location_id"] = update.location_id
    
    if update.assign_date is not None:
        update_parts.append("assign_date = :assign_date")
        params["assign_date"] = update.assign_date
    
    if update.end_date is not None:
        update_parts.append("end_date = :end_date")
        params["end_date"] = update.end_date
    
    if update.assign_by_id is not None:
        update_parts.append("assign_by_id = :assign_by_id")
        params["assign_by_id"] = update.assign_by_id
    
    if update.end_by_id is not None:
        update_parts.append("end_by_id = :end_by_id")
        params["end_by_id"] = update.end_by_id
    
    if update.workorder_assign_id is not None:
        update_parts.append("workorder_assign_id = :workorder_assign_id")
        params["workorder_assign_id"] = update.workorder_assign_id
    
    if update.workorder_remove_id is not None:
        update_parts.append("workorder_remove_id = :workorder_remove_id")
        params["workorder_remove_id"] = update.workorder_remove_id
    
    if update.notes is not None:
        update_parts.append("notes = :notes")
        params["notes"] = update.notes
    
    if not update_parts:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update"
        )
    
    update_sql = text(f"""
        UPDATE app.fact_device_assignment
        SET {', '.join(update_parts)}
        OUTPUT INSERTED.assignment_id, INSERTED.device_id, INSERTED.location_id,
               INSERTED.assign_date, INSERTED.end_date, INSERTED.assign_by_id,
               INSERTED.end_by_id, INSERTED.workorder_assign_id, INSERTED.workorder_remove_id,
               INSERTED.notes
        WHERE assignment_id = :assignment_id
    """)
    
    result = db.execute(update_sql, params).first()
    db.commit()
    
    return DeviceAssignmentResponse(
        assignment_id=result.assignment_id,
        device_id=result.device_id,
        location_id=result.location_id,
        assign_date=result.assign_date,
        end_date=result.end_date,
        assign_by_id=result.assign_by_id,
        end_by_id=result.end_by_id,
        workorder_assign_id=result.workorder_assign_id,
        workorder_remove_id=result.workorder_remove_id,
        notes=result.notes
    )


@router.post("/device-assignments/{assignment_id}/close", response_model=DeviceAssignmentResponse)
async def close_device_assignment(
    assignment_id: int,
    end_date: datetime,
    workorder_remove_id: Optional[int] = None,
    notes: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """Close a device assignment by setting end_date (ADMIN only)"""
    
    # Get existing assignment
    existing = db.execute(
        text("""
            SELECT assignment_id, end_date
            FROM app.fact_device_assignment
            WHERE assignment_id = :assignment_id
        """),
        {"assignment_id": assignment_id}
    ).first()
    
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assignment {assignment_id} not found"
        )
    
    if existing.end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Assignment {assignment_id} is already closed"
        )
    
    # Update assignment
    update_sql = text("""
        UPDATE app.fact_device_assignment
        SET end_date = :end_date,
            end_by_id = :end_by_id,
            workorder_remove_id = :workorder_remove_id,
            notes = CASE 
                WHEN :notes IS NOT NULL THEN COALESCE(notes, '') + ' | Closed: ' + :notes
                ELSE notes
            END
        OUTPUT INSERTED.assignment_id, INSERTED.device_id, INSERTED.location_id,
               INSERTED.assign_date, INSERTED.end_date, INSERTED.assign_by_id,
               INSERTED.end_by_id, INSERTED.workorder_assign_id, INSERTED.workorder_remove_id,
               INSERTED.notes
        WHERE assignment_id = :assignment_id
    """)
    
    result = db.execute(update_sql, {
        "assignment_id": assignment_id,
        "end_date": end_date,
        "end_by_id": current_user.id,
        "workorder_remove_id": workorder_remove_id,
        "notes": notes
    }).first()
    
    db.commit()
    
    return DeviceAssignmentResponse(
        assignment_id=result.assignment_id,
        device_id=result.device_id,
        location_id=result.location_id,
        assign_date=result.assign_date,
        end_date=result.end_date,
        assign_by_id=result.assign_by_id,
        end_by_id=result.end_by_id,
        workorder_assign_id=result.workorder_assign_id,
        workorder_remove_id=result.workorder_remove_id,
        notes=result.notes
    )


@router.get("/device-assignments", response_model=List[DeviceAssignmentResponse])
async def list_device_assignments(
    device_id: Optional[int] = None,
    location_id: Optional[int] = None,
    active_only: bool = False,
    skip: int = 0,
    limit: int = 10000,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """List device assignments with optional filters (ADMIN only)"""
    
    where_clauses = ['']
    params = {"skip": skip, "limit": limit}
    
    if device_id:
        where_clauses.append("device_id = :device_id")
        params["device_id"] = device_id
    
    if location_id:
        where_clauses.append("location_id = :location_id")
        params["location_id"] = location_id
    
    if active_only:
        where_clauses.append("end_date IS NULL")
    
    where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    query = text(f"""
        SELECT 
            da.assignment_id, da.device_id, da.location_id, da.assign_date, da.end_date,
            da.assign_by_id, da.end_by_id, da.workorder_assign_id, da.workorder_remove_id, da.notes--,
            --d.device_terminal_id, d.device_type
        FROM app.fact_device_assignment da
        --INNER JOIN app.dim_device d On da.device_id = d.device_id
        {where_clause}
        ORDER BY da.assign_date DESC
        OFFSET :skip ROWS
        FETCH NEXT :limit ROWS ONLY
    """)
    
    results = db.execute(query, params).fetchall()
    
    return [
        DeviceAssignmentResponse(
            assignment_id=r.assignment_id,
            device_id=r.device_id,
            location_id=r.location_id,
            assign_date=r.assign_date,
            end_date=r.end_date,
            assign_by_id=r.assign_by_id,
            end_by_id=r.end_by_id,
            workorder_assign_id=r.workorder_assign_id,
            workorder_remove_id=r.workorder_remove_id,
            notes=r.notes
        )
        for r in results
    ]


# ============= Helper Endpoints for Dropdowns =============

@router.get("/facilities", response_model=List[FacilityResponse])
async def list_facilities(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """List all facilities for dropdown (ADMIN only)"""
    
    query = text("""
        SELECT facility_id, facility_name, facility_nickname, facility_type,
               on_off_street, street_area
        FROM app.dim_facility
        ORDER BY facility_name
    """)
    
    results = db.execute(query).fetchall()
    
    return [
        FacilityResponse(
            facility_id=r.facility_id,
            facility_name=r.facility_name,
            facility_nickname=r.facility_nickname,
            facility_type=r.facility_type,
            on_off_street=r.on_off_street,
            street_area=r.street_area
        )
        for r in results
    ]



# ============= Space Management (NEW) =============

@router.post("/spaces", response_model=SpaceResponse)
async def create_space(
    space: SpaceCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """
    Create a new space (ADMIN only).
    If a space with the same space_number exists for this facility, close it first.
    Automatically creates a location entry for the new space.
    """
    
    # Check if space_number already exists for this facility (active)
    existing_q = text("""
        SELECT space_id, end_date
        FROM app.dim_space
        WHERE facility_id = :facility_id 
          AND space_number = :space_number
          AND end_date IS NULL
    """)
    
    existing = db.execute(existing_q, {
        "facility_id": space.facility_id,
        "space_number": space.space_number
    }).first()
    
    if existing:
        # Close the existing space
        close_q = text("""
            UPDATE app.dim_space
            SET end_date = :end_date
            WHERE space_id = :space_id
        """)
        
        db.execute(close_q, {
            "space_id": existing.space_id,
            "end_date": space.start_date  # New space start = old space end
        })
    
    # Create new space
    insert_q = text("""
        INSERT INTO app.dim_space (
            space_number, space_type, facility_id, cwAssetID, 
            start_date, end_date, space_status
        )
        OUTPUT INSERTED.space_id, INSERTED.space_number, INSERTED.space_type,
               INSERTED.facility_id, INSERTED.cwAssetID, INSERTED.start_date,
               INSERTED.end_date, INSERTED.space_status
        VALUES (
            :space_number, :space_type, :facility_id, :cwAssetID,
            :start_date, NULL, :space_status
        )
    """)
    
    result = db.execute(insert_q, {
        "space_number": space.space_number,
        "space_type": space.space_type,
        "facility_id": space.facility_id,
        "cwAssetID": space.cwAssetID,
        "start_date": space.start_date,
        "space_status": space.space_status
    }).first()
    
    new_space_id = result.space_id
    
    # Create location for this space
    location_insert = text("""
        INSERT INTO app.dim_location (facility_id, space_id)
        VALUES (:facility_id, :space_id)
    """)
    
    db.execute(location_insert, {
        "facility_id": space.facility_id,
        "space_id": new_space_id
    })
    
    db.commit()
    
    return SpaceResponse(
        space_id=result.space_id,
        space_number=result.space_number,
        space_type=result.space_type,
        facility_id=result.facility_id,
        cwAssetID=result.cwAssetID,
        start_date=result.start_date,
        end_date=result.end_date,
        space_status=result.space_status
    )


@router.post("/spaces/{space_id}/close", response_model=SpaceResponse)
async def close_space(
    space_id: int,
    end_date: datetime = Query(..., description="Date to close the space"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """Close a space by setting its end_date (ADMIN only)"""
    
    # Verify space exists and is not already closed
    check_q = text("""
        SELECT space_id, end_date
        FROM app.dim_space
        WHERE space_id = :space_id
    """)
    
    existing = db.execute(check_q, {"space_id": space_id}).first()
    
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Space {space_id} not found"
        )
    
    if existing.end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Space {space_id} is already closed"
        )
    
    # Close the space
    update_q = text("""
        UPDATE app.dim_space
        SET end_date = :end_date
        OUTPUT INSERTED.space_id, INSERTED.space_number, INSERTED.space_type,
               INSERTED.facility_id, INSERTED.cwAssetID, INSERTED.start_date,
               INSERTED.end_date, INSERTED.space_status
        WHERE space_id = :space_id
    """)
    
    result = db.execute(update_q, {
        "space_id": space_id,
        "end_date": end_date
    }).first()
    
    db.commit()
    
    return SpaceResponse(
        space_id=result.space_id,
        space_number=result.space_number,
        space_type=result.space_type,
        facility_id=result.facility_id,
        cwAssetID=result.cwAssetID,
        start_date=result.start_date,
        end_date=result.end_date,
        space_status=result.space_status
    )

@router.get("/spaces", response_model=List[SpaceResponse])
async def list_spaces(
    facility_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """List spaces for dropdown, optionally filtered by facility (ADMIN only)"""
    
    where_clause = "WHERE facility_id = :facility_id" if facility_id else ""
    params = {"facility_id": facility_id} if facility_id else {}
    
    query = text(f"""
        SELECT space_id, space_number, space_type, facility_id, cwAssetID,
               start_date, end_date, space_status
        FROM app.dim_space
        {where_clause}
        ORDER BY space_number
    """)
    
    results = db.execute(query, params).fetchall()
    
    return [
        SpaceResponse(
            space_id=r.space_id,
            space_number=r.space_number,
            space_type=r.space_type,
            facility_id=r.facility_id,
            cwAssetID=r.cwAssetID,
            start_date=r.start_date,
            end_date=r.end_date,
            space_status=r.space_status
        )
        for r in results
    ]


@router.get("/locations", response_model=List[LocationResponse])
async def list_locations(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """List all locations with facility/space details (ADMIN only)"""
    
    query = text("""
        SELECT l.location_id, l.facility_id, l.space_id,
               f.facility_name, s.space_number
        FROM app.dim_location l
        INNER JOIN app.dim_facility f ON l.facility_id = f.facility_id
        LEFT JOIN app.dim_space s ON l.space_id = s.space_id
        ORDER BY f.facility_name, s.space_number
    """)
    
    results = db.execute(query).fetchall()
    
    return [
        LocationResponse(
            location_id=r.location_id,
            facility_id=r.facility_id,
            space_id=r.space_id,
            facility_name=r.facility_name,
            space_number=r.space_number
        )
        for r in results
    ]


@router.get("/users", response_model=List[dict])
async def list_users_for_assignment(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """List all users for assign_by/end_by dropdowns (ADMIN only)"""
    
    query = text("""
        SELECT id, username, full_name, email
        FROM app.users
        WHERE is_active = 1
        ORDER BY full_name
    """)
    
    results = db.execute(query).fetchall()
    
    return [
        {
            "id": r.id,
            "username": r.username,
            "full_name": r.full_name,
            "email": r.email
        }
        for r in results
    ]