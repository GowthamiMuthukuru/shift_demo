from sqlalchemy.orm import Session
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from sqlalchemy import extract
from decimal import Decimal


def get_client_shift_summary(db: Session, payroll_month: str):
    """Fetch shift summary filtered by payroll_month (YYYY-MM) including total allowances."""

    year, month = payroll_month.split("-")

    records = (
        db.query(ShiftAllowances)
        .filter(
            extract("year", ShiftAllowances.payroll_month) == int(year),
            extract("month", ShiftAllowances.payroll_month) == int(month)
        )
        .all()
    )

    if not records:
        return []

    summary = {}

    for row in records:
        client = row.client or "Unknown"

        if client not in summary:
            summary[client] = {
                "employees": set(),
                "shift_a": Decimal(0),
                "shift_b": Decimal(0),
                "shift_c": Decimal(0),
                "prime": Decimal(0),
                "total_allowances": Decimal(0)
            }

        summary[client]["employees"].add(row.emp_id)

        for mapping in row.shift_mappings:
            shift_type = mapping.shift_type.strip().upper()
            days = Decimal(mapping.days or 0)

            # add days
            if shift_type == "A":
                summary[client]["shift_a"] += days
            elif shift_type == "B":
                summary[client]["shift_b"] += days
            elif shift_type == "C":
                summary[client]["shift_c"] += days
            elif shift_type == "PRIME":
                summary[client]["prime"] += days

            # fetch rate for year
            rate = (
                db.query(ShiftsAmount.amount)
                .filter(ShiftsAmount.shift_type == shift_type)
                .filter(ShiftsAmount.payroll_year == year)
                .scalar()
            ) or 0

            rate = Decimal(str(rate))
            summary[client]["total_allowances"] += days * rate

    result = [
        {
            "client": client,
            "total_employees": len(info["employees"]),
            "shift_a_days": float(info["shift_a"]),
            "shift_b_days": float(info["shift_b"]),
            "shift_c_days": float(info["shift_c"]),
            "prime_days": float(info["prime"]),
            "total_allowances": float(info["total_allowances"])
        }
        for client, info in summary.items()
    ]

    return result
