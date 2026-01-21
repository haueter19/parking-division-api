from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from datetime import datetime
from cityworks import CityworksSession, CityworksConfig
from cityworks.api.work_order import WorkOrderAPI
from cityworks.gis.parking import get_spaces_from_work_order
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
    
    query = text("""
        WITH wo AS (
            -- Get details about single work order
            SELECT 
                WorkOrderSid, WorkOrderId, Description, Status, Cancel, InitiateDate, InitiatedBy, InitiatedBySid, SubmitTo, SubmitToSid, SubmitToOpenBy, DateSubmitTo, 
                ProjStartDate, ProjFinishDate, ActualStartDate, ActualFinishDate, RequestedBy, Supervisor, SupervisorSid, WoTemplateId, 
                ExpenseType, Location, WoAddress, Priority, WoXCoordinate, WoYCoordinate
            FROM CMMS.azteca.WorkOrder
            WHERE DomainId = 3 AND WorkOrderSid = :work_order_sid
        ),
        assets AS (
            -- Get all entities associated with single work order
            SELECT
                WorkOrderSid, WorkOrderId, EntityUid, EntityType, EntitySid, X, Y
            FROM CMMS.azteca.WorkOrderEntity
            WHERE WorkOrderSid = :work_order_sid
        ),
        comments AS (
            -- Get all Comments
            SELECT 
                CommentId, WorkOrderSid, WorkOrderId, AuthorSid, Comments, 
                DateCreated, LastModified, LastModifiedBySid
            FROM CMMS.azteca.WorkOrderComment
            WHERE WorkOrderSid = :work_order_sid
        ),
        instructions AS (
            -- Get all instructions
            SELECT
                WorkOrderSid, WorkOrderId, SeqId, Instructions
            FROM CMMS.azteca.WoInstruction
            WHERE WorkOrderSid = :work_order_sid
        ),
        custom_fields AS (
            -- Get custom fields
            SELECT WorkOrderSid, WorkOrderId, CustFieldId, CustFieldName, CustFieldValue
            FROM CMMS.azteca.WoCustField
            WHERE WorkOrderSid = :work_order_sid
        )
        -- Combine all results with identifiers
        SELECT 
            'work_order' as result_type,
            (SELECT * FROM wo FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) as json_data
        UNION ALL
        SELECT 
            'assets' as result_type,
            (SELECT * FROM assets FOR JSON PATH) as json_data
        UNION ALL
        SELECT 
            'comments' as result_type,
            (SELECT * FROM comments FOR JSON PATH) as json_data
        UNION ALL
        SELECT 
            'instructions' as result_type,
            (SELECT * FROM instructions FOR JSON PATH) as json_data
        UNION ALL
        SELECT 
            'custom_fields' as result_type,
            (SELECT * FROM custom_fields FOR JSON PATH) as json_data
        """)
    
    try:
        result = db.execute(query, {"work_order_sid": work_order_id})
        rows = result.fetchall()

        # Initialize response structure
        response = {
            "work_order": {},
            "assets": [],
            "comments": [],
            "instructions": [],
            "custom_fields": {}
        }

        # Process each result type
        for row in rows:
            result_type = row.result_type
            json_data = row.json_data
            
            if json_data:
                import json
                parsed_data = json.loads(json_data)
                
                if result_type == 'work_order':
                    response['work_order'] = parsed_data
                elif result_type == 'assets':
                    response['assets'] = parsed_data if isinstance(parsed_data, list) else []
                elif result_type == 'comments':
                    response['comments'] = parsed_data if isinstance(parsed_data, list) else []
                elif result_type == 'instructions':
                    response['instructions'] = parsed_data if isinstance(parsed_data, list) else []
                elif result_type == 'custom_fields':
                    # Convert array to flattened dict
                    if isinstance(parsed_data, list):
                        response['custom_fields'] = {
                            item['CustFieldName']: item['CustFieldValue'] 
                            for item in parsed_data
                        }
        
        # Check if work order was found
        if not response['work_order']:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Work order {work_order_id} not found"
            )
        
        # Check for any assets in one of the layers referencing a space, and if found, set flag to True
        spaces_flag = False
        for p in response['assets']:
            if p['EntityType'] in ['PU_METERS_ON_ST', 'PU_DISVET_ON_ST', 'PU_LZ_ON_ST', 'PU_OFF_ST_SPACES']:
                spaces_flag = True
                break
        
        # If flag is True, then use the cityworks package to get the spaces data from the SDE
        if spaces_flag:
            print('hi')
            spaces = parking.get_spaces_from_work_order(response) # Expects a dictionary with a key called 'assets'. Returns a Pandas DataFrame
            if spaces.shape[0] > 0:
                # Initialize list to hold updated assets
                updated_assets = []
                # Iterate through assets; get space data from spaces dataframe; turn required fields into dict; update asset dict with this new dict
                for asset in response['assets']:
                    asset.update(spaces[spaces['cwAssetID']==asset['EntityUid']][['Status', 'SpaceName']].to_dict(orient='records')[0])
                    updated_assets.append(asset)


        return response
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching work order details: {str(e)}"
        )



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
