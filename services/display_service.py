from fastapi import HTTPException
from sqlalchemy.orm import Session,joinedload
from sqlalchemy import extract,func
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from datetime import datetime
from typing import Optional
import pandas as pd
from io import BytesIO
from fastapi.responses import StreamingResponse
from utils.client_enums import Company

def fetch_shift_data(db: Session, start: int, limit: int):
    current_month = datetime.now().strftime("%Y-%m")

    has_current_month = (
        db.query(ShiftAllowances)
        .filter(func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == current_month)
        .first()
    )

    if has_current_month:
        selected_month = current_month
        message = None
    else:
        latest_month = (
            db.query(func.to_char(ShiftAllowances.duration_month, "YYYY-MM"))
            .order_by(func.to_char(ShiftAllowances.duration_month, "YYYY-MM").desc())
            .first()
        )

        if not latest_month:
            raise HTTPException(status_code=404, detail="No shift data is available.")

        selected_month = latest_month[0]
        message = f"No data found for current month {current_month}"

    query = (
        db.query(
            ShiftAllowances.id.label("id"),
            ShiftAllowances.emp_id.label("emp_id"),
            ShiftAllowances.emp_name.label("emp_name"),
            ShiftAllowances.department.label("department"),
            func.to_char(ShiftAllowances.payroll_month, "YYYY-MM").label("payroll_month"),
            ShiftAllowances.client.label("client"),
            ShiftAllowances.account_manager.label("account_manager"),
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM").label("duration_month")
        )
        .filter(func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == selected_month)
        .group_by(ShiftAllowances.id, ShiftAllowances.emp_id, ShiftAllowances.emp_name,
                  ShiftAllowances.department, ShiftAllowances.payroll_month,
                  ShiftAllowances.client, ShiftAllowances.account_manager,
                  ShiftAllowances.duration_month)
    )

    total_records = query.count()
    rows = query.order_by(ShiftAllowances.id.asc()).offset(start).limit(limit).all()
    SHIFT_LABELS = {
    "A": "A(9PM to 6AM)",
    "B": "B(4PM to 1AM)",
    "C": "C(6AM to 3PM)",
    "PRIME": "PRIME(12AM to 9AM)"
    }


    result = []
    for row in rows:
        mappings = (
            db.query(ShiftMapping.shift_type, ShiftMapping.days)
            .filter(ShiftMapping.shiftallowance_id == row.id)
            .all()
        )

        shift_output = {}
        for m in mappings:
            days = float(m.days)
            if days > 0:
                label = SHIFT_LABELS.get(m.shift_type, m.shift_type)
                shift_output[label] = days
        client_name = row.client
        abbr = next((c.name for c in Company if c.value == client_name), None)
        if abbr:
            client_name = abbr

        result.append({
            **row._asdict(),
            "client": client_name,
            "shift_details": shift_output
        })
    return selected_month, total_records, result, message

def parse_shift_value(value: str):
    """Convert input to float and validate shift value."""
    if value is None or str(value).strip() == "":
        return 0
    try:
        num = float(str(value).strip())
    except:
        raise HTTPException(status_code=400, detail=f"Invalid shift value '{value}'. Only numbers allowed.")
    if num < 0:
        raise HTTPException(status_code=400, detail=f"Negative values not allowed: '{value}'.")
    return num
 
def update_shift_service(
    db: Session,
    emp_id: str,
    payroll_month: str,
    updates: dict,
    duration_month: Optional[str] = None
):
    """Update shift days for a given employee, payroll month, and optional duration month."""
 
    allowed_fields = ["shift_a", "shift_b", "shift_c", "prime"]
    extra_fields = [k for k in updates if k not in allowed_fields]
    if extra_fields:
        raise HTTPException(status_code=400, detail=f"Invalid fields: {extra_fields}. Only {allowed_fields} allowed.")
 
    numeric_updates = {k: parse_shift_value(v) for k, v in updates.items()}
    shift_map = {"shift_a": "A", "shift_b": "B", "shift_c": "C", "prime": "PRIME"}
    mapped_updates = {shift_map[k]: numeric_updates[k] for k in numeric_updates if numeric_updates[k] >= 0}
 
    if not mapped_updates:
        raise HTTPException(status_code=400, detail="No valid shift values provided.")
 
    # Parse payroll_month
    try:
        payroll_date = datetime.strptime(payroll_month, "%Y-%m")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payroll_month format. Use YYYY-MM")
 
    # Parse duration_month if provided
    duration_date = None
    if duration_month:
        try:
            duration_date = datetime.strptime(duration_month, "%Y-%m")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid duration_month format. Use YYYY-MM")
 
    # Get record filtered by emp_id + payroll_month + optional duration_month
    query = db.query(ShiftAllowances).filter(
        ShiftAllowances.emp_id == emp_id,
        extract("year", ShiftAllowances.payroll_month) == payroll_date.year,
        extract("month", ShiftAllowances.payroll_month) == payroll_date.month
    )
 
    if duration_date:
        query = query.filter(
            extract("year", ShiftAllowances.duration_month) == duration_date.year,
            extract("month", ShiftAllowances.duration_month) == duration_date.month
        )
 
    record = query.first()
    if not record:
        raise HTTPException(
            status_code=404,
            detail=f"No shift record found for employee {emp_id}, month {payroll_month}, duration {duration_month}"
        )
 
    # Get shift rates
    rate_rows = db.query(ShiftsAmount).all()
    rates = {r.shift_type.upper(): float(r.amount) for r in rate_rows}
    for stype in mapped_updates:
        if stype not in rates:
            raise HTTPException(status_code=400, detail=f"Missing rate for shift '{stype}'.")
 
    existing = {m.shift_type: m for m in record.shift_mappings}
 
    # Apply updates
    for stype, days in mapped_updates.items():
        if stype in existing:
            existing[stype].days = days
        else:
            temp = ShiftMapping(shiftallowance_id=record.id, shift_type=stype, days=days)
            existing[stype] = temp
 
    # Validate total days
    # total_days_temp = float(sum(float(m.days) for m in existing.values()))
    # if total_days_temp > 22:
    #     db.rollback()
    #     raise HTTPException(status_code=400, detail=f"Total days cannot exceed 22 in a month. Current total = {total_days_temp}")
 
    # Commit updates
    for stype, days in mapped_updates.items():
        if stype not in [m.shift_type for m in record.shift_mappings]:
            db.add(existing[stype])
    db.commit()
    db.refresh(record)
 
    # Prepare response
    shift_details = [{"shift": m.shift_type, "days": float(m.days)} for m in record.shift_mappings if m.shift_type in mapped_updates]
    total_days = float(sum(float(m.days) for m in record.shift_mappings))
    total_allowance = float(sum(float(m.days) * rates[m.shift_type] for m in record.shift_mappings))
 
    return {
        "message": "Shift updated successfully",
        "updated_fields": list(mapped_updates.keys()),
        "total_days": total_days,
        "total_allowance": total_allowance,
        "shift_details": shift_details,
    }

def fetch_shift_record(emp_id: str, duration_month: str, payroll_month: str, db: Session):


    try:
        duration_dt = datetime.strptime(duration_month + "-01", "%Y-%m-%d").date()
        payroll_dt = datetime.strptime(payroll_month + "-01", "%Y-%m-%d").date()
    except:
        raise HTTPException(status_code=400, detail="Invalid month format. Expected YYYY-MM")

    record = (
        db.query(ShiftAllowances)
        .options(joinedload(ShiftAllowances.shift_mappings))
        .filter(
            ShiftAllowances.emp_id == emp_id,
            ShiftAllowances.duration_month == duration_dt,
            ShiftAllowances.payroll_month == payroll_dt,
        )
        .first()
    )

    if not record:
        raise HTTPException(status_code=404, detail="Record not found")

    result = {
        "id": record.id,
        "emp_id": record.emp_id,
        "emp_name": record.emp_name,
        "grade": record.grade,
        "department": record.department,
        "client": next((c.name for c in Company if c.value == record.client), record.client),
        "project": record.project,
        "project_code": record.project_code,
        "account_manager": record.account_manager,
        "practice_lead": record.practice_lead,
        "delivery_manager": record.delivery_manager,
        "duration_month": record.duration_month.strftime("%Y-%m"),
        "payroll_month": record.payroll_month.strftime("%Y-%m"),
        "billability_status": record.billability_status,
        "practice_remarks": record.practice_remarks,
        "rmg_comments": record.rmg_comments,

        "created_at": record.created_at.strftime("%Y-%m-%d") if record.created_at else None,
        "updated_at": record.updated_at.strftime("%Y-%m-%d") if record.updated_at else None,

        "A": 0,
        "B": 0,
        "C": 0,
        "PRIME": 0
    }

    for m in record.shift_mappings:
        stype = m.shift_type.strip().upper()
        if stype in ("A", "B", "C", "PRIME"):
            result[stype] = float(m.days)

    return result

def generate_employee_shift_excel(emp_id: str, duration_month: str, payroll_month: str, db: Session):
    record = fetch_shift_record(emp_id, duration_month, payroll_month, db)

    if not record:
        raise HTTPException(status_code=404, detail="Record not found")

    # Convert YYYY-MM to Feb'25 format
    duration_fmt = datetime.strptime(record["duration_month"], "%Y-%m").strftime("%b'%y")
    payroll_fmt = datetime.strptime(record["payroll_month"], "%Y-%m").strftime("%b'%y")

    record["duration_month"] = duration_fmt
    record["payroll_month"] = payroll_fmt

    df = pd.DataFrame([record])

    ordered_columns = [
        "id","emp_id","emp_name","grade","department","client","project","project_code",
        "account_manager","practice_lead","delivery_manager","duration_month","payroll_month",
        "billability_status","practice_remarks","rmg_comments","created_at","updated_at",
        "A","B","C","PRIME"
    ]
    df = df[ordered_columns]

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Shift Details")
    output.seek(0)

    filename = f"{emp_id}_{duration_month}_{payroll_month}_shift_details.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
