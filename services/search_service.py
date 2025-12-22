from datetime import datetime, date
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
import re

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from utils.client_enums import Company


def validate_not_future_month(month_str: str, field_name: str):
    

    if not re.fullmatch(r"\d{4}-\d{2}", month_str):
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} must be in YYYY-MM format"
        )

   
    try:
        month_date = datetime.strptime(month_str, "%Y-%m").date().replace(day=1)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {field_name} value"
        )

   
    today = date.today().replace(day=1)
    if month_date > today:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} cannot be a future month"
        )

def export_filtered_excel(
    db: Session,
    emp_id: str | None = None,
    account_manager: str | None = None,
    department: str | None = None,
    client: str | None = None,
    start_month: str | None = None,
    end_month: str | None = None,
    start: int = 0,
    limit: int = 10,
):

   
    rates = {
        r.shift_type.upper(): float(r.amount or 0)
        for r in db.query(ShiftsAmount).all()
        if r.shift_type
    }

   
    if not start_month and not end_month:
        today = datetime.now().replace(day=1)

        for i in range(12):
            year, month = today.year, today.month - i
            if month <= 0:
                month += 12
                year -= 1

            month_str = f"{year:04d}-{month:02d}"

            exists = db.query(ShiftAllowances.id).filter(
                func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == month_str
            ).first()

            if exists:
                start_month = month_str
                break

        if not start_month:
            raise HTTPException(404, "No data found in last 12 months")

    if end_month and not start_month:
        raise HTTPException(400, "start_month is required when end_month is provided")

    if start_month:
        validate_not_future_month(start_month, "start_month")

    if end_month:
        validate_not_future_month(end_month, "end_month")

    if start_month and end_month and start_month > end_month:
        raise HTTPException(400, "start_month cannot be greater than end_month")

  
    base = db.query(
        ShiftAllowances.id,
        ShiftAllowances.emp_id,
        ShiftAllowances.emp_name,
        ShiftAllowances.grade,
        ShiftAllowances.department,
        ShiftAllowances.client,
        ShiftAllowances.project,
        ShiftAllowances.account_manager,
        func.to_char(ShiftAllowances.duration_month, "YYYY-MM").label("duration_month"),
        func.to_char(ShiftAllowances.payroll_month, "YYYY-MM").label("payroll_month"),
    )

    if start_month and end_month:
        base = base.filter(
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") >= start_month,
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") <= end_month,
        )
    else:
        base = base.filter(
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == start_month
        )


    if emp_id:
        base = base.filter(func.upper(ShiftAllowances.emp_id).like(f"%{emp_id.upper()}%"))
    if account_manager:
        base = base.filter(func.upper(ShiftAllowances.account_manager).like(f"%{account_manager.upper()}%"))
    if department:
        base = base.filter(func.upper(ShiftAllowances.department).like(f"%{department.upper()}%"))
    if client:
        base = base.filter(func.upper(ShiftAllowances.client).like(f"%{client.upper()}%"))

    total_records = base.count()
    if total_records == 0:
        raise HTTPException(404, "No data found")

    subq = base.subquery()

    head_count = (
        db.query(func.count(func.distinct(subq.c.emp_id)))
        .scalar()
    )

    all_rows = base.all()

    paginated_rows = (
        base.order_by(
            ShiftAllowances.duration_month.desc(),
            ShiftAllowances.emp_id.asc(),
        )
        .offset(start)
        .limit(limit)
        .all()
    )

    SHIFT_LABELS = {
        "A": "A(9PM to 6AM)",
        "B": "B(4PM to 1AM)",
        "C": "C(6AM to 3PM)",
        "PRIME": "PRIME(12AM to 9AM)",
    }

    
    overall_shift_details = {v: 0.0 for v in SHIFT_LABELS.values()}
    overall_total_allowance = 0.0

    for row in all_rows:
        mappings = db.query(ShiftMapping).filter(
            ShiftMapping.shiftallowance_id == row.id
        ).all()

        for m in mappings:
            days = float(m.days or 0)
            if days <= 0:
                continue

            rate = rates.get(m.shift_type.upper(), 0)
            overall_total_allowance += days * rate

            label = SHIFT_LABELS.get(m.shift_type.upper(), m.shift_type)
            overall_shift_details[label] += days

 
    employees = []

    for row in paginated_rows:
        d = row._asdict()
        sid = d.pop("id")

        emp_shift_details = {}
        emp_total = 0.0

        mappings = db.query(ShiftMapping).filter(
            ShiftMapping.shiftallowance_id == sid
        ).all()

        for m in mappings:
            days = float(m.days or 0)
            if days <= 0:
                continue

            st = m.shift_type.upper()
            rate = rates.get(st, 0)
            emp_total += days * rate

            label = SHIFT_LABELS.get(st, st)
            emp_shift_details[label] = emp_shift_details.get(label, 0) + days

        d["shift_details"] = {
            k: int(v) if v.is_integer() else v
            for k, v in emp_shift_details.items()
        }
        d["total_allowance"] = round(emp_total, 2)

        abbr = next((c.name for c in Company if c.value == d["client"]), None)
        if abbr:
            d["client"] = abbr

        employees.append(d)

    return {
        "total_records": total_records,
        "shift_details": {
            **{
                k: int(v) if v.is_integer() else v
                for k, v in overall_shift_details.items()
                if v > 0
            },
            "head_count": head_count,
            "total_allowance": round(overall_total_allowance, 2),
        },
        "data": {
            "employees": employees
        }
    }
