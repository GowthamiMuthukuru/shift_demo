import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import func
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from fastapi import HTTPException
from datetime import datetime, date
from dateutil.relativedelta import relativedelta


def export_filtered_excel(
    db: Session,
    emp_id: str | None = None,
    account_manager: str | None = None,
    start_month: str | None = None,
    end_month: str | None = None,
    department: str | None = None,
    client: str | None = None
):

    SHIFT_LABELS = {"A": "A", "B": "B", "C": "C", "PRIME": "PRIME"}

    base_query = (
        db.query(
            ShiftAllowances.id,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.grade,
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.project,
            ShiftAllowances.project_code,
            ShiftAllowances.account_manager,
            ShiftAllowances.delivery_manager,
            ShiftAllowances.practice_lead,
            ShiftAllowances.billability_status,
            ShiftAllowances.practice_remarks,
            ShiftAllowances.rmg_comments,
            ShiftAllowances.duration_month,
            ShiftAllowances.payroll_month
        )
    )

   
    if emp_id:
        base_query = base_query.filter(
            func.trim(ShiftAllowances.emp_id) == emp_id.strip()
        )

    if account_manager:
        base_query = base_query.filter(
            func.lower(func.trim(ShiftAllowances.account_manager)) ==
            account_manager.strip().lower()
        )

    if department:
        base_query = base_query.filter(
            func.lower(func.trim(ShiftAllowances.department)) ==
            department.strip().lower()
        )

    if client:
        base_query = base_query.filter(
            func.lower(func.trim(ShiftAllowances.client)) ==
            client.strip().lower()
        )

 
    today = date.today()
    current_month = today.replace(day=1)

    query = None

   
    if start_month or end_month:
        if not start_month:
            raise HTTPException(400, "start_month is required when end_month is provided")

        try:
            start_date = datetime.strptime(start_month, "%Y-%m").date().replace(day=1)
        except ValueError:
            raise HTTPException(400, "start_month must be YYYY-MM")

        if end_month:
            try:
                end_date = datetime.strptime(end_month, "%Y-%m").date().replace(day=1)
            except ValueError:
                raise HTTPException(400, "end_month must be YYYY-MM")

            if start_date > end_date:
                raise HTTPException(400, "start_month cannot be after end_month")

            query = base_query.filter(
                func.date_trunc("month", ShiftAllowances.duration_month) >= start_date,
                func.date_trunc("month", ShiftAllowances.duration_month) <= end_date,
            )
        else:
            query = base_query.filter(
                func.date_trunc("month", ShiftAllowances.duration_month) == start_date
            )

    else:
        found = False

        for i in range(12):
            check_month = current_month - relativedelta(months=i)

            temp_query = base_query.filter(
                func.date_trunc("month", ShiftAllowances.duration_month) == check_month
            )

            if temp_query.first():
                query = temp_query
                found = True
                break

        if not found:
            raise HTTPException(
                status_code=404,
                detail="No data found in last 12 months"
            )


    rows = query.all()
    if not rows:
        raise HTTPException(404, "No records found for given filters")

   
    shift_amounts = db.query(ShiftsAmount).all()
    ALLOWANCE_MAP = {
        item.shift_type.upper(): float(item.amount or 0)
        for item in shift_amounts
    }

    final_data = []

    for row in rows:
        mappings = (
            db.query(ShiftMapping.shift_type, ShiftMapping.days)
              .filter(ShiftMapping.shiftallowance_id == row.id)
              .all()
        )

        shift_entries = []
        total_allowance = 0.0

        for m in mappings:
            days = float(m.days or 0)
            if days > 0:
                label = SHIFT_LABELS.get(m.shift_type.upper(), m.shift_type.upper())
                shift_entries.append(f"{label}-{int(days)}")
                total_allowance += ALLOWANCE_MAP.get(m.shift_type.upper(), 0) * days

        final_data.append({
            "emp_id": row.emp_id,
            "emp_name": row.emp_name,
            "grade": row.grade,
            "department": row.department,
            "client": row.client,
            "project": row.project,
            "project_code": row.project_code,
            "account_manager": row.account_manager,
            "shift_details": ", ".join(shift_entries) if shift_entries else None,
            "delivery_manager": row.delivery_manager,
            "practice_lead": row.practice_lead,
            "billability_status": row.billability_status,
            "practice_remarks": row.practice_remarks,
            "rmg_comments": row.rmg_comments,
            "duration_month": row.duration_month.strftime("%Y-%m") if row.duration_month else None,
            "payroll_month": row.payroll_month.strftime("%Y-%m") if row.payroll_month else None,
            "total_allowance": f"₹ {total_allowance:,.2f}",
        })

    return pd.DataFrame(final_data)
