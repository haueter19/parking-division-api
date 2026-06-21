from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from collections import defaultdict
from app.db.session import get_aims_db
from app.api.dependencies import require_role, UserProxy
from app.models.database import UserRole

router = APIRouter(prefix="/enforcement", tags=["enforcement"])


@router.get("/stats")
async def get_enforcement_stats(
    aims_db: Session = Depends(get_aims_db),
    current_user: UserProxy = Depends(require_role([UserRole.ENFORCEMENT, UserRole.MANAGER, UserRole.ADMIN]))
):
    """
    Return yesterday's citation stats for the Enforcement landing page.

    All aggregation is done in Python from a single DB query.
    """

    citations_sql = text("""
        SELECT
            t.ticketid,
            t.IssueDate,
            t.FirstViolationDesc,
            t.StatusDesc,
            t.BadgeNumber,
            t.BadgeLastName,
            t.Amount
        FROM dbo.VT_Tickets t
        WHERE CAST(t.IssueDate AS DATE) = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)
        ORDER BY t.IssueDate
    """)

    try:
        rows = aims_db.execute(citations_sql).fetchall()
        print(f"Fetched {len(rows)} citations from AIMS for enforcement stats")
    except Exception as e:
        print(f"Error fetching enforcement stats from AIMS: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching enforcement stats: {str(e)}"
        )

    if not rows:
        from datetime import date, timedelta
        report_date = (date.today() - timedelta(days=1)).isoformat()
        return {
            "report_date": report_date,
            "summary": {"citations": 0, "total_amount": 0.0, "warnings": 0, "tows": 0, "voids": 0},
            "by_hour": [],
            "by_violation": [],
            "by_status": [],
            "by_badge": [],
        }

    # Aggregate entirely in Python
    total_citations = 0
    total_amount = 0.0
    warnings = 0
    tows = 0
    voids = 0

    by_hour: dict[int, int] = defaultdict(int)
    by_violation: dict[str, int] = defaultdict(int)
    by_status: dict[str, int] = defaultdict(int)
    by_badge: dict[str, int] = defaultdict(int)

    report_date_val = None

    seen_tickets: set[int] = set()

    for row in rows:
        ticket_id = row.ticketid
        if ticket_id in seen_tickets:
            continue
        seen_tickets.add(ticket_id)

        total_citations += 1

        amount = float(row.Amount or 0)
        total_amount += amount

        status_raw = (row.StatusDesc or "").strip()
        status_upper = status_raw.upper()

        if "WARNING" in status_upper:
            warnings += 1
        if row.FirstViolationDesc == "TOW FEE":
            tows += 1
        if row.StatusDesc == 'Void Approved':
            voids += 1
            total_citations -= 1
            total_amount -= amount

        issued = row.IssueDate
        if issued is not None:
            hour = issued.hour
            by_hour[hour] += 1
            if report_date_val is None:
                from datetime import timedelta
                report_date_val = (issued.date() ).isoformat()

        violation = (row.FirstViolationDesc or "Unknown").strip()
        by_violation[violation] += 1

        by_status[status_raw or "Unknown"] += 1

        badge = str(row.BadgeNumber or "Unknown").strip()
        by_badge[badge] += 1

    if report_date_val is None:
        from datetime import date, timedelta
        report_date_val = (date.today() - timedelta(days=1)).isoformat()

    # Build hour series: fill all 0-23 hours so chart is continuous
    by_hour_list = [{"hour": h, "count": by_hour.get(h, 0)} for h in range(24)]

    by_violation_list = sorted(
        [{"violation": k, "count": v} for k, v in by_violation.items()],
        key=lambda x: x["count"], reverse=True
    )

    by_status_list = sorted(
        [{"status": k, "count": v} for k, v in by_status.items()],
        key=lambda x: x["count"], reverse=True
    )

    by_badge_list = sorted(
        [{"badge": k, "count": v} for k, v in by_badge.items()],
        key=lambda x: x["count"], reverse=True
    )

    return {
        "report_date": report_date_val,
        "summary": {
            "citations": total_citations,
            "total_amount": round(total_amount, 2),
            "warnings": warnings,
            "tows": tows,
            "voids": voids,
        },
        "by_hour": by_hour_list,
        "by_violation": by_violation_list,
        "by_status": by_status_list,
        "by_badge": by_badge_list,
    }
