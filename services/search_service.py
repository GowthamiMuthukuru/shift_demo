import pandas as pd
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount


def export_filtered_excel(db: Session, emp_id: str | None, account_manager: str | None):

    # Base query
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

    # If no filters → fetch all
    if not emp_id and not account_manager:
        rows = query.all()
    else:
        if emp_id:
            query = query.filter(ShiftAllowances.emp_id == emp_id)

        if account_manager:
            query = query.filter(ShiftAllowances.account_manager == account_manager)

        rows = query.all()

        if not rows:
            raise HTTPException(
                status_code=404,
                detail="No data found for the given emp_id or account_manager"
            )

    shift_rates = db.query(ShiftsAmount).all()

    # NEW: If empty → raise HTTP 404
    if not shift_rates:
        raise HTTPException(
            status_code=404,
            detail="Shift allowance amount table (shifts_amount) is empty. Please configure the shift rates."
        )

    allowance_map = {
        rate.shift_type.upper(): float(rate.amount)
        for rate in shift_rates
    }

    final_data = []

    for row in rows:

        shift_list = row.shift_type or []
        shift_list = [str(s).upper() for s in shift_list]

        # Calculate total allowances
        total_allowances = sum(allowance_map.get(s, 0) for s in shift_list)

        row_dict = row._asdict()

        # Convert numpy values
        clean_row = {}
        for key, val in row_dict.items():
            if hasattr(val, "item"):
                clean_row[key] = val.item()
            else:
                clean_row[key] = val

        clean_row["total_allowances"] = total_allowances

        final_data.append(clean_row)

    df = pd.DataFrame(final_data)
    return df.to_dict(orient="records")
