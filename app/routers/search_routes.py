"""Routes for fetching employee shift details."""

from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import Session
from db import get_db
from services.search_service import export_filtered_excel
from utils.dependencies import get_current_user

router = APIRouter(
    prefix="/employee-details",
    tags=["Search Details"]
)


@router.post("/search")
def fetch_employee_details(
    payload: dict = Body(..., example={
        "emp_id": "IN01804611",
        "account_manager": "John Doe",
        "clients": "ALL",
        "department": "Infra - IT Operations",
        "start_month": "YYYY-MM",
        "end_month": "YYYY-MM",
        "start": 0,
        "limit": 10,
        "selected_year": "YYYY",
        "selected_months": ["01", "02"],
        "selected_quarters": ["Q1"]
    }),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """
    Fetch employee shift details using the provided request body filters.

    Filters supported:
    - Employee ID
    - Account Manager
    - Single Client (clients)
    - Optional Department (department)
    - Start and End Month
    - Selected Year, Selected Months, Selected Quarters
    - Pagination (start, limit)

    All validation, filtering, and aggregation is handled by
    the service layer `export_filtered_excel`.

    Returns:
        dict: Employee shift details including shift-wise allowances and totals.
    """
    return export_filtered_excel(
        db=db,
        emp_id=payload.get("emp_id"),
        account_manager=payload.get("account_manager"),
        start_month=payload.get("start_month"),
        end_month=payload.get("end_month"),
        start=payload.get("start", 0),
        limit=payload.get("limit", 10),
        clients=payload.get("client"),
        department=payload.get("department"),
        selected_year=payload.get("selected_year"),
        selected_months=payload.get("selected_months", []),
        selected_quarters=payload.get("selected_quarters", [])
    )
