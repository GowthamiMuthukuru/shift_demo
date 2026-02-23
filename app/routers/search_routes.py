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
    payload: dict = Body(
        ...,
        example={
            "emp_id": "IN01804611",
            "client_partner": "John Doe",

            # Clients/Departments can be "ALL", a single string, or a list of strings
            "clients": "ALL",
            "departments": "ALL",

            # Multi-year and multi-month selection (no start_month/end_month).
            # If neither provided -> current month (or fallback to latest within last 12 months).
            # If months provided without years -> current year.
            "years": [2025],
            "months": [1, 2, 3],

            # Shifts can be "ALL", a string or list[str] (e.g. ["US_INDIA","PST_MST"])
            "shifts": "ALL",

            # Headcount filter by range(s). Applies per department if departments != "ALL", else per client.
            # Accepts "ALL", a single string "1-10", or list like ["1-10","11-20"].
            "headcounts": "ALL",

            # NEW: Allowance filter (inclusive). Accepts "min-max" or ["a-b","c-d"] (OR semantics)
             "allowance": "5000-20000",
            # "allowance": ["0-5000", "10000-15000"],

            # Pagination
            "start": 0,
            "limit": 10,

            # Employee list sorting (no summary)
            "sort_by": "total_allowance",      # "client" | "client_partner" | "departments" | "headcount" | "total_allowance"
            "sort_order": "default"            # "default" | "asc" | "desc"
        }
    ),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """
    Fetch employee shift details using the provided request body filters.
    """
    # Backward-compat support for legacy keys if present
    clients_payload = payload.get("clients", payload.get("client", "ALL"))
    departments_payload = payload.get("departments", payload.get("department", "ALL"))

    return export_filtered_excel(
        db=db,
        emp_id=payload.get("emp_id"),
        client_partner=payload.get("client_partner"),
        start=payload.get("start", 0),
        limit=payload.get("limit", 10),
        clients=clients_payload,
        departments=departments_payload,
        years=payload.get("years"),
        months=payload.get("months"),
        shifts=payload.get("shifts", "ALL"),
        headcounts=payload.get("headcounts", "ALL"),
        sort_by=payload.get("sort_by", "total_allowance"),
        sort_order=payload.get("sort_order", "default"),
        
        allowance=payload.get("allowance"),
    )