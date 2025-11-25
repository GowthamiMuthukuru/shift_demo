import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import func
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from fastapi import HTTPException


def export_filtered_excel(db: Session, emp_id: str | None, account_manager: str | None):

    
    query = (
        db.query(
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.grade,
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.project,
            ShiftAllowances.project_code,
            ShiftAllowances.account_manager,
            func.array_agg(ShiftMapping.shift_type).label("shift_type"),
            ShiftAllowances.delivery_manager,
            ShiftAllowances.practice_lead,
            ShiftAllowances.billability_status,
            ShiftAllowances.practice_remarks,
            ShiftAllowances.rmg_comments,
            ShiftAllowances.duration_month,
            ShiftAllowances.payroll_month
        )
        .outerjoin(ShiftMapping, ShiftAllowances.id == ShiftMapping.shiftallowance_id)
        .group_by(ShiftAllowances.id)
    )

    # Case 1: No filters → full data
    if not emp_id and not account_manager:
        rows = query.all()

    else:
        # Apply filters
        if emp_id:
            query = query.filter(ShiftAllowances.emp_id == emp_id)
        if account_manager:
            query = query.filter(ShiftAllowances.account_manager == account_manager)

        rows = query.all()

        # Case 2: Filter applied but no results → return FULL data again
        if not rows:
            rows = query.all()

        # Case 3: STILL empty → raise HTTP
        if not rows:
            raise HTTPException(
                status_code=404,
                detail="No data found for the given emp_id or account_manager"
            )

    shift_amounts = db.query(ShiftsAmount).all()

    # If table empty → STOP and throw HTTP error
    if not shift_amounts:
        raise HTTPException(
            status_code=404,
            detail="Shift Allowance Amount master table (shifts_amount) is empty. Please configure shift amounts."
        )

    # Convert to dict → {"A": 500, "B": 350, ...}
    ALLOWANCE_MAP = {
        item.shift_type.upper(): float(item.amount)
        for item in shift_amounts
    }

    final_data = []

    for row in rows:

        shift_list = row.shift_type if row.shift_type else []
        shift_list = [s.upper() for s in shift_list]

        # Calculate TOTAL ALLOWANCES from DB values
        total_allowances = sum(ALLOWANCE_MAP.get(s, 0) for s in shift_list)

        row_data = row._asdict()
        row_data["total_allowances"] = total_allowances

        final_data.append(row_data)

    # Return DataFrame to FastAPI route
    return pd.DataFrame(final_data)
