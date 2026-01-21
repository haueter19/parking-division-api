from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from datetime import datetime
from cityworks import CityworksSession, CityworksConfig
from cityworks.api.work_order import WorkOrderAPI
from app.db.session import get_db
from app.api.dependencies import get_current_active_user, require_role, UserProxy
from app.models.database import UserRole

router = APIRouter(prefix="/cityworks", tags=["cityworks"])


@router.get("/work-orders")
async def get_work_orders(
    status_filter: Optional[str] = None,
    submit_to: Optional[str] = None,
    initiate_date_start: Optional[str] = None,
    initiate_date_end: Optional[str] = None,
    actual_start_date_start: Optional[str] = None,
    actual_start_date_end: Optional[str] = None,
    parent_template: Optional[str] = None,
    requested_by: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(require_role([UserRole.MANAGER, UserRole.ADMIN]))
):
    """
    Return work orders with TemplateId = '217' (Update GIS).

    Query parameters:
    - status_filter: Filter by work order status
    - submit_to: Filter by assigned user/group
    - initiate_date_start/end: Filter by initiate date range (YYYY-MM-DD)
    - actual_start_date_start/end: Filter by actual start date range (YYYY-MM-DD)
    - parent_template: Filter by parent template description
    - requested_by: Filter by requesting user
    """
    # TODO: Replace with actual SQL query provided by user
    # Placeholder query structure - will be updated with actual Cityworks SQL
    query = text("""
        SELECT 
            wo.WorkOrderId, wo.WorkOrderSid, wo.Description, wo.Status, wo.Supervisor, wo.RequestedBy, wo.InitiatedBy, wo.InitiateDate, wo.SubmitTo, wo.DateSubmitTo, wo.DateSubmitToOpen, wo.WorkCompletedBy, 
            wo.Location, wo.WoAddress, wo.ProjStartDate, wo.ProjFinishDate, wo.ActualStartDate, wo.ActualFinishDate, wo.Cancel, wo.WoXCoordinate x_coord, wo.WOYCOORDINATE y_coord, 
            wo.SupervisorSid, wo.RequestedBySid, wo.InitiatedBySid, wo.SubmitToSid, wo.SubmitToOpenBySid, wo.WorkCompletedBySid,
            COALESCE(pa.DESCRIPTION, 'TE Signing') ParentTemplateDescription --, al.*
        FROM CMMS.azteca.WorkOrder wo
        left join CMMS.azteca.ActivityLink al On (wo.WorkorderId=al.DESTACTIVITYID)
        left join CMMS.azteca.WorkOrder pa On (al.SOURCEACTIVITYID=pa.WorkOrderId)
        WHERE
            wo.DomainID = 3
            AND wo.WOTEMPLATEID = '217'
            AND wo.Status IN ('OPEN', 'HOLD')
            AND (al.LINKTYPE = 'Parent' or al.LINKTYPE IS NULL)
            AND (al.SOURCEACTIVITYTYPE IN ('WorkOrder', 'ServiceRequest') Or al.SOURCEACTIVITYTYPE IS NULL)
        ORDER BY 1 DESC
    """)

    # Note: Filters will be added dynamically once the actual query is provided
    
    try:
        rows = db.execute(query).fetchall()
        
        work_orders = []
        for row in rows:
            work_orders.append({
                "work_order_id": row.WorkOrderId,
                "work_order_sid": row.WorkOrderSid,
                "description": row.Description,
                "status": row.Status,
                "submit_to": row.SubmitTo,
                "initiate_date": row.InitiateDate.isoformat() if row.InitiateDate else None,
                "actual_start_date": row.ActualStartDate.isoformat() if row.ActualStartDate else None,
                "parent_template": row.ParentTemplateDescription,
                "requested_by": row.RequestedBy
            })

        return {"work_orders": work_orders, "count": len(work_orders)}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching work orders: {str(e)}"
        )


@router.get("/work-orders/{work_order_id}")
async def get_work_order_detail(
    work_order_id: int,
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(require_role([UserRole.MANAGER, UserRole.ADMIN]))
):
    """
    Return detailed information for a specific work order.
    This endpoint will call the cityworks python package to fetch full details
    including attached assets.

    Returns:
    - Work order details
    - Attached assets (list of dicts)
    - Parent template information (for determining process type)
    """
    # TODO: Integrate with cityworks python package
    
    

    # Configure connection
    config = CityworksConfig(environment='prod')

    # Create authenticated session
    with CityworksSession(config) as session:
        session.authenticate('CITY/tndnh', 'Segneri2A') # MUST CHANGE THIS!

    # Instantiate work order API
    wo_api = WorkOrderAPI(session)

    # API call to get work order details
    work_order = wo_api.get_by_sid(work_order_id)
    work_order['assets'] = wo_api.get_entities(work_order_id)
    try:
        work_order['instructions'] = wo_api.get_instructions(work_order_id)[str(work_order_id)]
    except:
        work_order['instructions'] = {}
    work_order['comments'] = wo_api.get_comments(work_order_id)

    # Example structure that will be returned:
    """{
        "work_order": {
            "work_order_id": work_order_id,
            "work_order_sid": None,
            "description": "Placeholder - integrate with cityworks package",
            "status": None,
            "submit_to": None,
            "initiate_date": None,
            "actual_start_date": None,
            "parent_template": None,
            "requested_by": None,
            "comments": None
        },
        "assets": [
            # List of attached assets will be populated here
            # Example structure:
            # {
            #     "asset_id": "12345",
            #     "asset_type": "Meter",
            #     "space_id": "A-123",
            #     "location": "123 Main St",
            #     "status": "Active"
            # }
        ],
        "parent_template_info": {
            # Information about the parent work order template
            # This determines what processing options are available
            "template_id": None,
            "template_description": None,
            "process_type": None  # e.g., "Meter/Hood Space", "Add Asset", "Remove Asset"
        }
    }"""
    return work_order


@router.get("/filter-options")
async def get_filter_options(
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(require_role([UserRole.MANAGER, UserRole.ADMIN]))
):
    """
    Return distinct values for filter dropdowns.
    """
    filters = {}

    # TODO: Fill the filter lists with actual queries, but set up in the cache so they don't run all the time

    parent_templates = text("""select 
        distinct wo.Description
    from CMMS.azteca.WorkOrder wo
    inner join CMMS.azteca.WOTEMPLATE t On (wo.WOTEMPLATEID = t.WOTEMPLATEID)
    left join CMMS.azteca.ActivityLink al On (wo.WorkorderId=al.SOURCEACTIVITYID)
    left join CMMS.azteca.WorkOrder ch On (al.DESTACTIVITYID=ch.WorkOrderId)
    where
        wo.domainId = 3
        and al.LINKTYPE = 'Parent'
        and al.SOURCEACTIVITYTYPE = 'WorkOrder'
        AND ch.WOTEMPLATEID = '217'
    ORDER BY 1
    """)
    parent_template_results = db.execute(parent_templates).fetchall()

    result_list = [tuple(row)[0] for row in parent_template_results]

    filters['statuses'] = ['OPEN', 'HOLD', 'CLOSED', 'CANCEL', 'COMPLETE', 'FINALREV', 'FINANCE']  # Distinct work order statuses
    filters['submit_to_options'] = ["Schmitt, KILEY G", "Haueter, Daniel", "Little, Calla", "Moseson, Hannah"]  # Distinct submit to values
    filters['requested_by_options'] = ["Cox, Stefanie L", "Field Operations , Supervisors", "Hall, Glenn J", "Haueter, Daniel", "Kershner, John F", "Putnam, William H", "SCHULTZ, TRENT W", "Villarreal, Juan A", "Wolfe, Heather A"]  # Distinct requested by values
    filters['parent_templates'] = result_list #[i for i in parent_template_results] # Distinct parent template descriptions
    return filters


@router.post("/work-orders/{work_order_id}/process")
async def process_work_order(
    work_order_id: str,
    action: str,  # "out_of_service", "return_to_service", "add_asset", "remove_asset", "update_status"
    space_id: Optional[str] = None,
    new_status: Optional[str] = None,
    notes: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(require_role([UserRole.MANAGER, UserRole.ADMIN]))
):
    """
    Process a work order action. Actions depend on the parent template type.

    Actions:
    - out_of_service: Move a space to the spaces out of service layer in SDE
    - return_to_service: Return a space to active service
    - add_asset: Add a new asset to the GIS SDE
    - remove_asset: Remove an asset from the GIS SDE
    - update_status: Update the work order status

    Parameters:
    - work_order_id: The work order being processed
    - action: The action to perform
    - space_id: Space ID (required for space-related actions)
    - new_status: New status value (required for status updates)
    - notes: Optional notes for the action
    """
    # TODO: Implement actual processing logic
    # This will integrate with:
    # 1. Cityworks API (via cityworks python package) for work order updates
    # 2. GIS SDE for spatial data updates

    return {
        "success": True,
        "message": f"Action '{action}' queued for work order {work_order_id}",
        "action": action,
        "work_order_id": work_order_id,
        "processed_by": current_user.username,
        "timestamp": datetime.now().isoformat()
    }
