"""
Routes for client summary data retrieval.
Supports month, quarter, range, employee, and account manager filters.
"""

from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import Session

from db import get_db
from utils.dependencies import get_current_user
from services.client_summary_service import client_summary_service

router = APIRouter(
    prefix="/client-summary",
    tags=["Client Summary"],
)


@router.post(
    "",
    summary="Get client summary",
    description=(
        "Returns client summary based on filters like clients, "
        "date range, employee, and account manager."
    ),
)
def client_summary(
    payload: dict = Body(
        default={},
        example={
            "emp_id": ["IN01804611"],
            "account_manager": ["John Doe"],
            "clients": "ALL",
            "selected_year": "YYYY",
            "selected_months": ["01", "02"],
            "selected_quarters": ["Q1"],
            "start_month": "YYYY-MM",
            "end_month": "YYYY-MM",
        },
    ),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """
    Return client summary based on provided filters.

    Notes:
    - `clients` can be `"ALL"` or `{ "Client": ["Dept1", "Dept2"] }`
    - If no date filter is provided, the latest available month is returned
    - `emp_ids` filters specific employees
    - `account_managers` filters by account manager names

    Delegates all data retrieval and aggregation to
    `client_summary_service`.
    """
    return client_summary_service(db=db, payload=payload)
