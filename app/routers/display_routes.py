"""
Routes for displaying, updating, and downloading shift allowance data.
(Future-proof: supports dynamic shift keys via config)
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import distinct

from db import get_db
from models.models import ShiftAllowances
from schemas.displayschema import ShiftUpdateRequest, ShiftUpdateResponse
from services.display_service import (
    update_shift_service,
    fetch_shift_record,
    generate_employee_shift_excel,
    fetch_shift_data,
)
from utils.dependencies import get_current_user
from utils.client_enums import Company, generate_unique_colors

router = APIRouter(prefix="/display")


@router.get("/")
def get_all_data(
    start: int = Query(0, ge=0),
    limit: int = Query(10, gt=0),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """Return paginated shift data."""
    selected_month, total_records, data, message = fetch_shift_data(db, start, limit)

    return {
        "selected_month": selected_month,
        "message": message,
        "total_records": total_records,
        "data": data,
    }


@router.get("/details")
def get_employee_shift_details(
    emp_id: str,
    duration_month: str,
    payroll_month: str,
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """Return shift details for a specific employee."""
    return fetch_shift_record(emp_id, duration_month, payroll_month, db)


@router.get("/details/download")
def download_shift_details(
    emp_id: str,
    duration_month: str,
    payroll_month: str,
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """Download shift details as an Excel file."""
    return generate_employee_shift_excel(emp_id, duration_month, payroll_month, db)



@router.put("/update", response_model=ShiftUpdateResponse)
def update_shift_detail(
    req: ShiftUpdateRequest,
    emp_id: str,
    payroll_month: str,
    duration_month: str,
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """
    Update shift allowance details (dynamic shift keys).

    Request body example:
    {
      "shifts": {
        "PST_MST": 2,
        "ANZ": 1.5
      }
    }
    """
    #  IMPORTANT: pass only the inner dict (not {"shifts": {...}})
    updates = req.shifts

    return update_shift_service(
        db=db,
        emp_id=emp_id,
        payroll_month=payroll_month,
        updates=updates,
        duration_month=duration_month,
    )


@router.get("/client-partner")
def display_client_partner(
    name: str,
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """Return matching client partner names."""
    client_partners = (
        db.query(distinct(ShiftAllowances.client_partner))
        .filter(
            ShiftAllowances.client_partner.isnot(None),
            ShiftAllowances.client_partner.ilike(f"%{name}%"),
        )
        .order_by(ShiftAllowances.client_partner)
        .all()
    )

    names = [row[0] for row in client_partners]
    return {"client_partners": names}


COLOR_MAP = generate_unique_colors(Company)


@router.get("/client-enum")
def get_client_enum(_current_user=Depends(get_current_user)):
    """Return client enum values with unique colors."""
    return {
        company.value: {
            "value": company.name.replace("_", " "),
            "hexcode": COLOR_MAP[company],
        }
        for company in Company
    }