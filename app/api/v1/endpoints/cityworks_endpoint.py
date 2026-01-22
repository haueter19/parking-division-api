from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel
from cityworks import CityworksSession, CityworksConfig
from cityworks.api.work_order import WorkOrderAPI
from cityworks.gis import parking
from cityworks.gis import operations as ops
from app.config import settings
from app.db.session import get_db
from app.api.dependencies import get_current_active_user, require_role, UserProxy
from app.models.database import UserRole


# ==================== Pydantic Models ====================

class AssetProcessData(BaseModel):
    entity_sid: int
    space_name: Optional[str] = None
    entity_type: str
    ada_relocation: Optional[str] = "Unknown"


class ProcessSpacesRequest(BaseModel):
    workflow_type: str  # 'out_of_service' or 'return_to_service'
    assets: List[AssetProcessData]
    # Out of service fields
    revenue_collected: Optional[str] = None
    removal_method: Optional[str] = None
    reason_removed: Optional[str] = None
    # Common fields
    notes: Optional[str] = None

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
                 WorkOrderSid, WorkOrderId, Status, Description, Supervisor, SupervisorSid, ProjStartDate, ProjFinishDate, ActualStartDate, ActualFinishDate, RequestedBy, RequestedBySid,
	    	    SubmitTo, SubmitToOpenBy, SubmitToOpenBySid, Priority, WoAddress, Location, WoxCoordinate, WoYCoordinate, WoClosedBy, ExpenseType, WoCategory, WoTemplateId, InitiateDate,
    	    	InitiatedBy, InitiatedBySid, DateWoClosed
            FROM CMMS.azteca.WorkOrder
            WHERE DomainId = 3 AND WorkOrderSid = :work_order_sid
        ),
        parent_work_order AS (
            SELECT
                p.WorkOrderSid, p.WorkOrderId, p.Description, p.Status,
                p.ProjStartDate, p.ProjFinishDate, p.ActualStartDate, p.ActualFinishDate,
                p.SubmitTo, p.DateSubmitTo, p.DateSubmitToOpen,
                p.WoClosedBy, p.DateWoClosed, p.ExpenseType,
                p.Supervisor, p.SupervisorSid, p.WoAddress, p.Location,
                p.WoTemplateId,
                al.ActivityLinkId
            FROM CMMS.azteca.ActivityLink al
            INNER JOIN CMMS.azteca.WorkOrder p ON (al.SourceActivityId = p.WorkOrderSid)
            WHERE
                p.DomainId = 3
                AND al.DestActivityId = :work_order_sid
                AND al.DestActivityType IN ('WorkOrder', 'ServiceRequest')
                AND al.LinkType = 'Parent'
        ),
        parent_instructions AS (
            -- Get parent work order's instructions
            SELECT
                i.WorkOrderSid, i.WorkOrderId, i.SeqId, i.Instructions
            FROM CMMS.azteca.WoInstruction i
            INNER JOIN parent_work_order p ON (i.WorkOrderSid = p.WorkOrderSid)
        ),
        assets AS (
            -- Get all entities associated with single work order
            SELECT
                WorkOrderSid, WorkOrderId, EntityUid, EntityType, EntitySid, X, Y
            FROM CMMS.azteca.WORKORDERENTITY
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
            'parent_work_order' as result_type,
            (SELECT * FROM parent_work_order FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) as json_data
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
            (SELECT Instructions FROM instructions ORDER BY SeqId FOR JSON PATH) as json_data
        UNION ALL
        SELECT 
            'parent_instructions' as result_type,
            (SELECT Instructions FROM parent_instructions ORDER BY SeqId FOR JSON PATH) as json_data
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
            "parent_work_order": {},
            "assets": [],
            "comments": [],
            "instructions": [],
            "parent_instructions": [],
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
                elif result_type == 'parent_work_order':
                    response['parent_work_order'] = parsed_data if isinstance(parsed_data, dict) else {}
                elif result_type == 'assets':
                    response['assets'] = parsed_data if isinstance(parsed_data, list) else []
                elif result_type == 'comments':
                    response['comments'] = "\n".join([i['Comments'] for i in parsed_data]) if isinstance(parsed_data, list) else ''
                elif result_type == 'instructions':
                    response['instructions'] = "','".join([i['Instructions'] for i in parsed_data]) if isinstance(parsed_data, list) else ''
                elif result_type == 'parent_instructions':
                    response['parent_instructions'] = "','".join([i['Instructions'] for i in parsed_data]) if isinstance(parsed_data, list) else ''
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
            spaces = parking.get_spaces_from_work_order(response) # Expects a dictionary with a key called 'assets'. Returns a Pandas DataFrame
            spaces.fillna({'Status':'', 'SpaceName':'', 'Space_Type':''}, inplace=True)
            if spaces.shape[0] > 0:
                # Initialize list to hold updated assets
                updated_assets = []
                # Iterate through assets; get space data from spaces dataframe; turn required fields into dict; update asset dict with this new dict
                for asset in response['assets']:
                    if spaces[spaces['cwAssetID']==asset['EntityUid']].shape[0] > 0:
                        asset.update(spaces[spaces['cwAssetID']==asset['EntityUid']][['Status', 'SpaceName', 'Space_Type', 'Address']].to_dict(orient='records')[0])
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


# ==================== Space Processing Endpoints ====================

# Parent template IDs for workflow detection
HOOD_SIGN_SPACE_TEMPLATE_IDS = [214, 682, 1160, 1161]  # Out of Service
HOOD_SIGN_REMOVAL_TEMPLATE_IDS = [99, 100, 101, 1162]  # Return to Service

# Space layer types that can be processed
SPACE_LAYER_TYPES = ['PU_METERS_ON_ST', 'PU_OFF_ST_SPACES', 'PU_LZ_ON_ST', 'PU_DISVET_ON_ST']


@router.get("/work-orders/{work_order_id}/validate-assets")
async def validate_work_order_assets(
    work_order_id: int,
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(require_role([UserRole.MANAGER, UserRole.ADMIN]))
):
    """
    Validate assets for a work order before processing.

    This endpoint:
    1. Fetches the work order and its assets
    2. Determines the workflow type based on parent template
    3. For each asset, checks if it's a space and validates against PU_SPACESOUTOFSERVICE
    4. Returns validation status for each asset

    Validation statuses:
    - ready: Asset can be processed
    - already_processed: A record already exists for this date
    - manual_review: Non-space asset type requires manual handling
    - error: Validation failed for this asset
    """
    
    # First, get the work order details to determine workflow type
    detail_response = await get_work_order_detail(work_order_id, db, current_user)

    parent_template_id = detail_response.get('parent_work_order', {}).get('WoTemplateId')
    parent_actual_finish_date = detail_response.get('parent_work_order', {}).get('ActualFinishDate')
    assets = detail_response.get('assets', [])

    # Determine workflow type
    if parent_template_id and int(parent_template_id) in HOOD_SIGN_SPACE_TEMPLATE_IDS:
        workflow_type = 'out_of_service'
    elif parent_template_id and int(parent_template_id) in HOOD_SIGN_REMOVAL_TEMPLATE_IDS:
        workflow_type = 'return_to_service'
    else:
        workflow_type = None

    validated_assets = []

    # Collect space names for bulk lookup
    space_assets = []
    for asset in assets:
        entity_type = asset.get('EntityType')
        if entity_type in SPACE_LAYER_TYPES:
            space_assets.append(asset)

    # Get existing out-of-service records for all spaces
    existing_records = {}
    if space_assets:
        space_names = [a.get('SpaceName') for a in space_assets if a.get('SpaceName')]
        if space_names:
            try:
                # Call the cityworks package to check existing records
                # ops.get_spaces_out_of_service returns data from PU_SPACESOUTOFSERVICE
                oos_data = ops.get_spaces_out_of_service(space_names)
                if oos_data is not None and len(oos_data) > 0:
                    # Convert to dict keyed by space name for easy lookup
                    for _, row in oos_data.iterrows():
                        space_num = row.get('SpaceNumber')
                        if space_num not in existing_records:
                            existing_records[space_num] = []
                        existing_records[space_num].append(row.to_dict())
            except Exception as e:
                # Log error but continue with empty records
                print(f"Error fetching out-of-service records: {e}")

    # Validate each asset
    for asset in assets:
        entity_type = asset.get('EntityType')
        entity_sid = asset.get('EntitySid')
        entity_uid = asset.get('EntityUid')
        space_name = asset.get('SpaceName')
        space_type = asset.get('Space_Type')
        space_block = asset.get('Address')
        current_status = asset.get('Status')

        validation_result = {
            'entity_sid': entity_sid,
            'entity_uid': entity_uid,
            'entity_type': entity_type,
            'space_name': space_name,
            'space_type': space_type,
            'space_block': space_block,
            'current_status': current_status,
            'validation_status': 'ready',
            'message': None
        }

        # Check if this is a space asset
        if entity_type not in SPACE_LAYER_TYPES:
            validation_result['validation_status'] = 'manual_review'
            validation_result['message'] = f'Non-space asset type ({entity_type}) requires manual review'
            validated_assets.append(validation_result)
            continue

        # For out_of_service workflow, check if record already exists for this date
        if workflow_type == 'out_of_service' and space_name:
            records = existing_records.get(space_name, [])

            # Check if any record has Date_Out on the same day as ActualFinishDate
            if parent_actual_finish_date and records:
                try:
                    finish_date = datetime.fromisoformat(parent_actual_finish_date.replace('Z', '+00:00'))
                    finish_date_only = finish_date.date()

                    for record in records:
                        date_out = record.get('Date_Out')
                        if date_out:
                            if isinstance(date_out, str):
                                record_date = datetime.fromisoformat(date_out.replace('Z', '+00:00')).date()
                            else:
                                record_date = date_out.date() if hasattr(date_out, 'date') else None

                            if record_date == finish_date_only:
                                validation_result['validation_status'] = 'already_processed'
                                validation_result['message'] = f'Record already exists for {finish_date_only}'
                                break
                except Exception as e:
                    print(f"Error parsing dates for {space_name}: {e}")

        # For return_to_service workflow, check if there's an open out-of-service record
        elif workflow_type == 'return_to_service' and space_name:
            records = existing_records.get(space_name, [])

            # Look for a record with Date_Returned IS NULL
            has_open_record = False
            for record in records:
                if record.get('Date_Returned') is None:
                    has_open_record = True
                    break

            if not has_open_record:
                validation_result['validation_status'] = 'already_processed'
                validation_result['message'] = 'No open out-of-service record found for this space'

        validated_assets.append(validation_result)

    return {
        'workflow_type': workflow_type,
        'parent_template_id': parent_template_id,
        'parent_actual_finish_date': parent_actual_finish_date,
        'assets': validated_assets
    }


@router.post("/work-orders/{work_order_id}/process-spaces")
async def process_work_order_spaces(
    work_order_id: int,
    request: ProcessSpacesRequest,
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(require_role([UserRole.MANAGER, UserRole.ADMIN]))
):
    """
    Process multiple spaces for a work order.

    For out_of_service workflow:
    1. Create records in PU_SPACESOUTOFSERVICE for each space
    2. Update space layer Status to "Out of Service" if this is the most recent record

    For return_to_service workflow:
    1. Update Date_Returned on the appropriate PU_SPACESOUTOFSERVICE record
    2. Update space layer Status to "In Service"

    Returns results for each processed space.
    """

    # Get work order details for dates and other info
    detail_response = await get_work_order_detail(work_order_id, db, current_user)
    parent_wo = detail_response.get('parent_work_order', {})
    parent_actual_finish_date = parent_wo.get('ActualFinishDate')
    submit_to = parent_wo.get('SubmitTo')

    results = []
    processed_count = 0

    for asset in request.assets:
        result = {
            'entity_sid': asset.entity_sid,
            'space_name': asset.space_name,
            'success': False,
            'message': None
        }

        try:
            if request.workflow_type == 'out_of_service':
                # Prepare data for PU_SPACESOUTOFSERVICE insert
                # TODO: Replace with actual implementation using parking.add_space_out_of_service()

                record_data = {
                    'Space_Number': asset.space_name,
                    'Date_Out': parent_actual_finish_date,
                    'Meter_Type': None,  # Would come from asset JSON
                    'created_user': current_user.username,
                    'last_edited_user': current_user.username,
                    'created_date': datetime.now().isoformat(),
                    'last_edited_date': datetime.now().isoformat(),
                    'Notes': request.notes,
                    'Revenue_Collected': request.revenue_collected,
                    'Removal_Method': request.removal_method,
                    'RemovedBy': submit_to,
                    'Reason_Removed': request.reason_removed,
                    'ADA_Relocation': asset.ada_relocation,
                    'entity_type': asset.entity_type  # For determining the layer
                }

                # STUB: In production, call parking.add_space_out_of_service([record_data])
                # and parking.update_space_status({'space': asset.space_name, 'layer': asset.entity_type, 'status': 'Out of Service'})

                result['success'] = True
                result['message'] = f'Space {asset.space_name} moved out of service (STUB)'
                processed_count += 1

            elif request.workflow_type == 'return_to_service':
                # TODO: Replace with actual implementation using parking.return_space_to_service()

                return_data = {
                    'Space_Number': asset.space_name,
                    'Date_Returned': parent_actual_finish_date,
                    'last_edited_user': current_user.username,
                    'last_edited_date': datetime.now().isoformat(),
                    'Notes': request.notes
                }

                # STUB: In production, call parking.return_space_to_service(return_data)
                # and parking.update_space_status({'space': asset.space_name, 'layer': asset.entity_type, 'status': 'In Service'})

                result['success'] = True
                result['message'] = f'Space {asset.space_name} returned to service (STUB)'
                processed_count += 1

            else:
                result['message'] = f'Unknown workflow type: {request.workflow_type}'

        except Exception as e:
            result['message'] = f'Error processing space: {str(e)}'

        results.append(result)

    return {
        'success': processed_count > 0,
        'processed_count': processed_count,
        'total_count': len(request.assets),
        'results': results,
        'work_order_id': work_order_id,
        'processed_by': current_user.username,
        'timestamp': datetime.now().isoformat()
    }


@router.post("/work-orders/{work_order_id}/close")
async def close_work_order_endpoint(
    work_order_id: int,
    notes: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: UserProxy = Depends(require_role([UserRole.MANAGER, UserRole.ADMIN]))
):
    """
    Close a work order after processing is complete.

    This endpoint will:
    1. Update the work order with current datetime, user, and status = 'Complete'
    2. Close the work order via Cityworks API
    """

    if current_user.username == 'tndnh':
        update_user_sid = 1629
    elif current_user.username == 'tnkgj':
        update_user_sid = 985
    else:
        update_user_sid = 1629

    print(update_user_sid)
    try:
        # First, update the work order with completion details
        update_data = {
            'Status': 'Complete',
            'ActualFinishDate': datetime.now().isoformat(),
            'CompletedBySid': update_user_sid,
            'notes': notes
        }
        print(update_data)
        # Configure connection
        config = CityworksConfig(environment='prod')
        
        password = settings.secret_password
        print(password)
        # Create authenticated session
        with CityworksSession(config) as session:
            #session.prompt_credentials()
            session.authenticate('CITY/tndnh', password)
        # Instantiate work order API
        wo_api = WorkOrderAPI(session)
        
        # Call parking.update_work_order to set the completion details
        update_response = wo_api.update_work_order(work_order_id, **update_data)

        if not update_response:
            return {
                'success': False,
                'message': f'Failed to update work order {work_order_id}',
                'work_order_id': work_order_id,
                'closed_by': current_user.username,
                'timestamp': datetime.now().isoformat()
            }
        
        print('made it passed the update')

        # Now close the work order via Cityworks API
        close_response = wo_api.close_work_order(str(work_order_id))

        if close_response and len(close_response) > 0:
            return {
                'success': True,
                'message': f'Work order {work_order_id} closed successfully',
                'work_order_id': work_order_id,
                'closed_by': current_user.username,
                'timestamp': datetime.now().isoformat()
            }
        else:
            return {
                'success': False,
                'message': f'Work order {work_order_id} updated but failed to close via API',
                'work_order_id': work_order_id,
                'closed_by': current_user.username,
                'timestamp': datetime.now().isoformat()
            }

    except Exception as e:
        return {
            'success': False,
            'message': f'Error closing work order: {str(e)}',
            'work_order_id': work_order_id,
            'closed_by': current_user.username,
            'timestamp': datetime.now().isoformat()
        }
