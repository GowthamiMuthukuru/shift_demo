from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session,joinedload
from db import get_db
from models.models import ShiftAllowances,ShiftMapping
from utils.dependencies import get_current_user
from schemas.displayschema import PaginatedShiftResponse,EmployeeResponse,ShiftUpdateRequest,ShiftUpdateResponse
from services.display_service import update_shift_service,fetch_shift_record,generate_employee_shift_excel,fetch_shift_data
from sqlalchemy import func,distinct
import pandas as pd
from io import BytesIO
from fastapi.responses import StreamingResponse
from utils.client_enums import Company

router = APIRouter(prefix="/display")

@router.get("/")
def get_all_data(
    start: int = Query(0, ge=0),
    limit: int = Query(10, gt=0),
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
):
    selected_month, total_records, data, message = fetch_shift_data(db, start, limit)
 
    return {
        "selected_month": selected_month,
        "message": message,
        "total_records": total_records,
        "data": data
    }

@router.get("/details")
def get_employee_shift_details(
    emp_id: str,
    duration_month: str,
    payroll_month: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    return fetch_shift_record(emp_id, duration_month, payroll_month, db)

@router.get("/details/download")
def download_shift_details(
    emp_id: str,
    duration_month: str,
    payroll_month: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    return generate_employee_shift_excel(emp_id, duration_month, payroll_month, db)




@router.put("/update", response_model=ShiftUpdateResponse)
def update_shift_detail(
    req: ShiftUpdateRequest,
    emp_id: str,
    payroll_month: str,
    duration_month: str,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    updates = req.model_dump(exclude_unset=True)
    return update_shift_service(
        db=db,
        emp_id=emp_id,
        payroll_month=payroll_month,
        updates=updates,
        duration_month=duration_month
    )

@router.get("/account-manager")
def display_account_manger(
    name: str,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    account_managers = (db.query(distinct(ShiftAllowances.account_manager))
    .filter(ShiftAllowances.account_manager.isnot(None),
        ShiftAllowances.account_manager.ilike(f'%{name}%'))
    .order_by(ShiftAllowances.account_manager)
    .all())
    names = [name[0] for name in account_managers]
    return {"account_managers":names}

@router.get("/client-enum")
def get_client_enum(current_user = Depends(get_current_user)):
                    return{company.value:company.name.replace("_"," ") for company in Company}