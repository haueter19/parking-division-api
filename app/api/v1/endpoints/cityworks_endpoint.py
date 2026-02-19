from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel
from cityworks import CityworksSession, CityworksConfig
from cityworks.api.work_order import WorkOrderAPI
from cityworks.gis import parking
from cityworks.queries.work_orders import get_work_order_details_json
from app.config import settings
from app.db.session import get_db
from app.api.dependencies import get_current_active_user, require_role, UserProxy
from app.models.database import UserRole


# ==================== Pydantic Models ====================

class AssetProcessData(BaseModel):
    entity_uid: str
    entity_sid: int
    entity_type: str
    space_name: Optional[str] = None
    space_type: Optional[str] = None
    current_sde_status: Optional[str] = None
    recent_space_out_of_service: Optional[str] = None
    ada_relocation: Optional[str] = "Unknown"
    object_id: Optional[int] = None
    event_id: Optional[int] = None
    date_returned: Optional[str] = None
    x: Optional[float] = None
    y: Optional[float] = None
    sign_start_date: Optional[str] = None
    sign_end_date: Optional[str] = None


class ProcessSpacesRequest(BaseModel):
    workflow_type: str  # 'out_of_service', 'return_to_service', or 'out_of_service_and_return'
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

    query = text("""
        SELECT 
            wo.WorkOrderId, wo.WorkOrderSid, wo.Description, wo.Status, wo.Supervisor, wo.RequestedBy, wo.InitiatedBy, wo.InitiateDate, wo.SubmitTo, wo.DateSubmitTo, wo.DateSubmitToOpen, wo.WorkCompletedBy, 
            wo.Location, wo.WoAddress, wo.ProjStartDate, wo.ProjFinishDate, wo.ActualStartDate, wo.ActualFinishDate, wo.Cancel, wo.WoXCoordinate x_coord, wo.WOYCOORDINATE y_coord, 
            wo.SupervisorSid, wo.RequestedBySid, wo.InitiatedBySid, wo.SubmitToSid, wo.SubmitToOpenBySid, wo.WorkCompletedBySid,
            CASE
                WHEN pa.Description IS NULL AND wo.WOTemplateId = '1586' THEN 'Portable CC Reader Move'
                WHEN pa.Description IS NULL AND wo.WOTemplateId = '217' THEN 'TE Signing'
                ELSE pa.Description
            END As ParentTemplateDescription
        FROM CMMS.azteca.WorkOrder wo
        left join CMMS.azteca.ActivityLink al On (wo.WorkorderId=al.DESTACTIVITYID AND al.DestActivityType!='Inspection')
        left join CMMS.azteca.WorkOrder pa On (al.SOURCEACTIVITYID=pa.WorkOrderId)
        WHERE
            wo.DomainID = 3
            AND wo.WOTEMPLATEID IN ('217', '1586')
            AND wo.Status IN ('OPEN', 'HOLD')
            AND (al.LINKTYPE = 'Parent' or al.LINKTYPE IS NULL)
            AND (al.SOURCEACTIVITYTYPE IN ('WorkOrder', 'ServiceRequest') Or al.SOURCEACTIVITYTYPE IS NULL)
        ORDER BY 1 DESC
    """)

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
    Return detailed information for a specific work order with validated assets.

    Returns work order details, attached assets enriched with SDE space data,
    per-asset workflow actions, and validation status — all in a single call.
    """

    try:
        response = get_work_order_details_json(work_order_id)

        if not response['work_order']:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Work order {work_order_id} not found"
            )

        # Enrich with workflow, SDE space data, per-asset actions, and validation
        parking.prepare_work_order(response)

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



# ==================== Space Processing Endpoints ====================

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

    # Get raw work order data (skip validation overhead)
    raw_response = get_work_order_details_json(work_order_id)
    parent_wo = raw_response.get('parent_work_order', {}) or {}
    parent_actual_finish_date = parent_wo.get('ActualFinishDate')
    submit_to = parent_wo.get('SubmitTo')

    now = datetime.now().isoformat(timespec='seconds')
    username = current_user.username.upper()

    results = []
    processed_count = 0
    for asset in request.assets:
        result = {
            'entity_uid': asset.entity_uid,
            'space_name': asset.space_name,
            'success': False,
            'message': None
        }

        try:
            if request.workflow_type == 'out_of_service':
                record_data = {
                    'space_number': asset.space_name,
                    'date_out': parent_actual_finish_date,
                    'meter_type': asset.space_type,
                    'x': asset.x,
                    'y': asset.y,
                    'created_user': username,
                    'last_edited_user': username,
                    'created_date': now,
                    'last_edited_date': now,
                    'note': request.notes,
                    'revenue_collected': request.revenue_collected,
                    'removal_method': request.removal_method,
                    'removed_by': submit_to,
                    'reason_removed': request.reason_removed,
                    'ada_relocation': asset.ada_relocation,
                }

                records_inserted = parking.add_space_out_of_service([record_data])
                print(f"Inserted {records_inserted} records for space {asset.space_name}")

                if asset.recent_space_out_of_service == 'true':
                    parking.update_space_status(
                        table=asset.entity_type, space_id=asset.entity_uid,
                        status='Out of service', last_edited_date=now, last_edited_user=username,
                    )

                result['success'] = True
                result['message'] = f'Space {asset.space_name} moved out of service'
                processed_count += records_inserted

            elif request.workflow_type == 'return_to_service':
                if asset.object_id is None or asset.event_id is None:
                    raise ValueError(f"Missing object_id or event_id for space {asset.space_name}")

                return_data = {
                    'object_id': asset.object_id,
                    'event_id': asset.event_id,
                    'date_returned': parent_actual_finish_date,
                    'last_edited_user': username,
                    'last_edited_date': now,
                }

                return_count = parking.return_space_to_service(return_data)
                print(f"Updated: {return_count} record for {asset.space_name} returned to service")

                if asset.recent_space_out_of_service == 'true':
                    parking.update_space_status(
                        table=asset.entity_type, space_id=asset.entity_uid,
                        status='In service', last_edited_date=now, last_edited_user=username,
                    )

                result['success'] = True
                result['message'] = f'Space {asset.space_name} returned to service'
                processed_count += 1

            elif request.workflow_type == 'out_of_service_and_return':
                # Temporary No Parking Sign: single INSERT with both dates
                if not asset.sign_start_date:
                    raise ValueError(f"Missing sign_start_date for space {asset.space_name}")

                record_data = {
                    'space_number': asset.space_name,
                    'date_out': asset.sign_start_date,
                    'date_returned': asset.sign_end_date,
                    'meter_type': asset.space_type,
                    'x': asset.x,
                    'y': asset.y,
                    'created_user': username,
                    'last_edited_user': username,
                    'created_date': now,
                    'last_edited_date': now,
                    'note': request.notes,
                    'revenue_collected': request.revenue_collected,
                    'removal_method': request.removal_method or 'Signed',
                    'removed_by': submit_to,
                    'reason_removed': request.reason_removed,
                    'ada_relocation': asset.ada_relocation,
                }

                records_inserted = parking.add_space_out_of_service([record_data])
                print(f"Inserted {records_inserted} sign OOS records for space {asset.space_name}")

                # Determine SDE status: if sign end is past → In service, else Out of service
                if asset.recent_space_out_of_service == 'true':
                    target_status = 'Out of service'
                    if asset.sign_end_date:
                        end_dt = datetime.fromisoformat(asset.sign_end_date.replace('Z', '+00:00'))
                        if end_dt < datetime.now(end_dt.tzinfo):
                            target_status = 'In service'

                    parking.update_space_status(
                        table=asset.entity_type, space_id=asset.entity_uid,
                        status=target_status, last_edited_date=now, last_edited_user=username,
                    )

                result['success'] = True
                result['message'] = f'Space {asset.space_name} sign OOS record created'
                processed_count += records_inserted

            else:
                result['message'] = f'Unknown workflow type: {request.workflow_type}'
                print(f"Unknown workflow type: {request.workflow_type}")

        except Exception as e:
            result['message'] = f'Error processing space: {str(e)}'
            print(f"Error processing {asset.space_name}: {str(e)}")

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
        
        # Get password from settings
        password = settings.secret_password

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
