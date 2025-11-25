from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import Session
from models.models import ShiftAllowances, ShiftsAmount
from decimal import Decimal


def get_interval_summary_service(start_month: str, end_month: str, db: Session):
    try:
        start = datetime.strptime(start_month + "-01", "%Y-%m-%d").date()
        end = datetime.strptime(end_month + "-01", "%Y-%m-%d").date()
    except:
        raise ValueError("Invalid input month format. Expected YYYY-MM")

    if start > end:
        raise ValueError("start_month must be <= end_month")

    current = start
    summary = {}

    while current <= end:
        allowances = db.query(ShiftAllowances).filter(
            ShiftAllowances.payroll_month == current
        ).all()

        if allowances:
            for allowance in allowances:
                client = allowance.client or "Unknown Client"

                if client not in summary:
                    summary[client] = {
                        "A": Decimal(0),
                        "B": Decimal(0),
                        "C": Decimal(0),
                        "PRIME": Decimal(0),
                        "total_amount": Decimal(0)
                    }

                for mapping in allowance.shift_mappings:
                    stype = mapping.shift_type.strip().upper()
                    days = Decimal(mapping.days or 0)
                    if stype in summary[client]:
                        summary[client][stype] += days

                payroll_year = str(current.year)
                shift_amount_rows = db.query(ShiftsAmount).filter(
                    ShiftsAmount.payroll_year == payroll_year
                ).all()

                amount_map = {
                    sa.shift_type.strip().upper(): Decimal(str(sa.amount))
                    for sa in shift_amount_rows
                }

                for stype in ("A", "B", "C", "PRIME"):
                    if stype in amount_map:
                        summary[client]["total_amount"] += summary[client][stype] * amount_map[stype]

        current += relativedelta(months=1)

    # Convert Decimal to float for final response
    result = {
        client: {
            "A": float(info["A"]),
            "B": float(info["B"]),
            "C": float(info["C"]),
            "PRIME": float(info["PRIME"]),
            "total_amount": float(info["total_amount"])
        }
        for client, info in summary.items()
    }

    return result
