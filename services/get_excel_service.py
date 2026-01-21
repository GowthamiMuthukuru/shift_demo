"""
Service for exporting filtered shift allowance data as a Pandas DataFrame.
"""

from typing import Optional
from datetime import datetime, date

import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount


def _parse_month(month: str, field_name: str) -> date:
    """
    Convert a YYYY-MM string into a date object representing
    the first day of the month.
    """
    try:
        return datetime.strptime(month, "%Y-%m").date().replace(day=1)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} must be YYYY-MM",
        ) from exc


def _resolve_latest_month(base_query, current_month: date):
    """
    Find the latest available month in the last 12 months
    from the given query.
    """
    for i in range(12):
        check_month = current_month - relativedelta(months=i)
        query = base_query.filter(
            func.date_trunc("month", ShiftAllowances.duration_month) == check_month
        )
        if query.first():
            return query

    raise HTTPException(
        status_code=404,
        detail="No data found in last 12 months",
    )


def _calculate_shift_allowances(db, row, shift_labels, allowance_map):
    """
    Compute shift entries and total allowance for a single row.
    """
    mappings = (
        db.query(ShiftMapping.shift_type, ShiftMapping.days)
        .filter(ShiftMapping.shiftallowance_id == row.id)
        .all()
    )

    shift_entries = []
    total_allowance = 0.0

    for mapping in mappings:
        days = float(mapping.days or 0)
        if days <= 0:
            continue

        shift_type = mapping.shift_type.upper()
        label = shift_labels.get(shift_type, shift_type)
        rate = allowance_map.get(shift_type, 0)
        shift_total = rate * days

        total_allowance += shift_total
        shift_entries.append(
            f"{label}-{int(days)}*{int(rate):,}=₹{int(shift_total):,}"
        )

    return shift_entries, total_allowance


def export_filtered_excel(
    db: Session,
    emp_id: Optional[str] = None,
    account_manager: Optional[str] = None,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
    department: Optional[str] = None,
    client: Optional[str] = None,
):
    """
    Export shift allowance records as a Pandas DataFrame with optional filters.

    Filters:
    - emp_id
    - account_manager
    - department
    - client
    - start_month / end_month (YYYY-MM)

    Returns the latest available month if no month is specified.
    """
    shift_labels = {"A": "A", "B": "B", "C": "C", "PRIME": "PRIME"}

    base_query = db.query(
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
        ShiftAllowances.payroll_month,
    )

    if emp_id:
        base_query = base_query.filter(
            func.trim(ShiftAllowances.emp_id) == emp_id.strip()
        )
    if account_manager:
        base_query = base_query.filter(
            func.lower(func.trim(ShiftAllowances.account_manager))
            == account_manager.strip().lower()
        )
    if department:
        base_query = base_query.filter(
            func.lower(func.trim(ShiftAllowances.department))
            == department.strip().lower()
        )
    if client:
        base_query = base_query.filter(
            func.lower(func.trim(ShiftAllowances.client)) == client.strip().lower()
        )

    current_month = date.today().replace(day=1)

    # Apply date filters
    if start_month or end_month:
        if not start_month:
            raise HTTPException(
                status_code=400,
                detail="start_month is required when end_month is provided",
            )
        start_date = _parse_month(start_month, "start_month")
        if end_month:
            end_date = _parse_month(end_month, "end_month")
            if start_date > end_date:
                raise HTTPException(
                    status_code=400,
                    detail="start_month cannot be after end_month",
                )
            query = base_query.filter(
                func.date_trunc("month", ShiftAllowances.duration_month) >= start_date,
                func.date_trunc("month", ShiftAllowances.duration_month) <= end_date,
            )
        else:
            query = base_query.filter(
                func.date_trunc("month", ShiftAllowances.duration_month) == start_date
            )
    else:
        query = _resolve_latest_month(base_query, current_month)

    rows = query.all()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No records found for given filters",
        )

    allowance_map = {
        item.shift_type.upper(): float(item.amount or 0)
        for item in db.query(ShiftsAmount).all()
    }

    final_data = []
    for row in rows:
        shift_entries, total_allowance = _calculate_shift_allowances(
            db, row, shift_labels, allowance_map
        )

        final_data.append(
            {
                "emp_id": row.emp_id,
                "emp_name": row.emp_name,
                "department": row.department,
                "client": row.client,
                "project": row.project,
                "project_code": row.project_code,
                "client_partner": row.account_manager,
                "shift_details": ", ".join(shift_entries) if shift_entries else None,
                "delivery_manager": row.delivery_manager,
                "practice_lead": row.practice_lead,
                "billability_status": row.billability_status,
                "practice_remarks": row.practice_remarks,
                "rmg_comments": row.rmg_comments,
                "duration_month": (
                    row.duration_month.strftime("%Y-%m")
                    if row.duration_month else None
                ),
                "payroll_month": (
                    row.payroll_month.strftime("%Y-%m")
                    if row.payroll_month else None
                ),
                "total_allowance": f"₹ {total_allowance:,.2f}",
            }
        )

    return pd.DataFrame(final_data)
