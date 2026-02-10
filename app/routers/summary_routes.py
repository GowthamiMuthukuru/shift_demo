"""
Client shift summary API routes.

This module defines endpoints for retrieving summarized client-level
shift and allowance data for a given month. The summary can optionally
be filtered by account manager and is intended for reporting and
dashboard use cases.
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user
from services.summary_service import get_client_shift_summary
from schemas.displayschema import ClientSummary

router = APIRouter(prefix="/summary")


@router.get(
    "/client-shift-summary",
    response_model=dict[str, list[ClientSummary]],
    responses={404: {"description": "No records found"}}
)
def client_shift_summary(
    duration_month: str | None = Query(None, description="Format YYYY-MM"),
    account_manager: str | None = Query(None, description="Account manager name"),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """
    Fetch client-wise shift summary for a given month.

    This endpoint returns aggregated shift data grouped by client and
    account manager for the specified duration month. If no month is
    provided, the service automatically selects the most recent month
    with available data.

    Args:
        duration_month (str | None): Target month in YYYY-MM format.
        account_manager (str | None): Optional account manager filter.
        db (Session): Active database session.
        _current_user: Authenticated user context.

    Returns:
        dict[str, list[ClientSummary]]: Mapping of duration month to
        client-level shift summary records.

    Raises:
        HTTPException:
            - 400 for invalid input parameters.
            - 404 if no matching records are found.
    """

    summary = get_client_shift_summary(db, duration_month, account_manager)

    return summary
