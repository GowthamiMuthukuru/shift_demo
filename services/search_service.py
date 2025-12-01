from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from fastapi.encoders import jsonable_encoder
from models.models import ShiftAllowances, ShiftMapping

SHIFT_LABELS = {
    "A": "A(9PM to 6AM)",
    "B": "B(4PM to 1AM)",
    "C": "C(6AM to 3PM)",
    "PRIME": "PRIME(12AM to 9AM)"
}

def export_filtered_excel(db: Session, emp_id: str | None = None, account_manager: str | None = None):

    query = (
        db.query(
            ShiftAllowances.id,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.grade,
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.project,
            ShiftAllowances.account_manager,
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM").label("duration_month"),
            func.to_char(ShiftAllowances.payroll_month, "YYYY-MM").label("payroll_month")
        )
    )

    if emp_id:
        query = query.filter(ShiftAllowances.emp_id.ilike(f"%{emp_id}%"))

    if account_manager:
        query = query.filter(ShiftAllowances.account_manager.ilike(f"%{account_manager}%"))

    rows = query.all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No data found for the given emp_id or account_manager"
        )

    final_data = []
    for row in rows:
        base = row._asdict()
        shiftallowance_id = base.pop("id")  # remove internal id from output

        # fetch shift mapping
        mappings = (
            db.query(ShiftMapping.shift_type, ShiftMapping.days)
            .filter(ShiftMapping.shiftallowance_id == shiftallowance_id)
            .all()
        )

        shift_output = {}
        for m in mappings:
            if m.days and float(m.days) > 0:
                label = SHIFT_LABELS.get(m.shift_type, m.shift_type)
                shift_output[label] = float(m.days)

        # merge base + shifts and remove None
        record = {k: v for k, v in {**base, **shift_output}.items() if v is not None}
        final_data.append(record)

    return jsonable_encoder(final_data)
