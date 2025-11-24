from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import Session
from models.models import ShiftAllowances, ShiftsAmount


def get_interval_summary_service(start_month: str, end_month: str, db: Session):
    try:
        start = datetime.strptime(start_month + "-01", "%Y-%m-%d").date()
        end = datetime.strptime(end_month + "-01", "%Y-%m-%d").date()
    except:
        raise ValueError("Invalid input month format. Expected YYYY-MM")

    if start > end:
        raise ValueError("start_month must be less than or equal to end_month")

    current = start
    report = {}

    while current <= end:
        month_key = current.strftime("%Y-%m")
        month_summary = {}

        allowances = db.query(ShiftAllowances).filter(
            ShiftAllowances.payroll_month == current
        ).all()

        if not allowances:
            report[month_key] = "no info available"
            current += relativedelta(months=1)
            continue

        for allowance in allowances:
            client = allowance.client or "Unknown Client"
            if client not in month_summary:
                month_summary[client] = {
                    "shift_days": {"A": 0, "B": 0, "C": 0, "PRIME": 0},
                    "total_amount": 0
                }

            for mapping in allowance.shift_mappings:
                stype = mapping.shift_type.strip().upper()
                if stype in month_summary[client]["shift_days"]:
                    month_summary[client]["shift_days"][stype] += mapping.days

        payroll_year = str(current.year)
        shift_amount_rows = db.query(ShiftsAmount).filter(
            ShiftsAmount.payroll_year == payroll_year
        ).all()

        shift_amount_map = {
            sa.shift_type.strip().upper(): float(sa.amount)
            for sa in shift_amount_rows
        }

        for client, entry in month_summary.items():
            for stype, days in entry["shift_days"].items():
                if stype in shift_amount_map:
                    entry["total_amount"] += days * shift_amount_map[stype]

        report[month_key] = month_summary
        current += relativedelta(months=1)

    return report
