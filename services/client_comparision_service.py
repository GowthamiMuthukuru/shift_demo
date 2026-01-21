"""Services for client comparison, totals, and department summaries."""

from datetime import datetime, date
from calendar import monthrange
from typing import Optional, Dict, Any
from decimal import Decimal
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from dateutil.relativedelta import relativedelta
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount

def parse_yyyy_mm(value: str) -> date:
    try:
        dt = datetime.strptime(value, "%Y-%m")
        return date(dt.year, dt.month, 1)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid month format '{value}'. Expected 'YYYY-MM'."
        )

def month_key_from_date(d: date) -> str:
    return d.strftime("%Y-%m")

def last_day_of_month(d: date) -> date:
    _, last_day = monthrange(d.year, d.month)
    return date(d.year, d.month, last_day)

# pylint: disable=too-many-locals,too-many-branches,too-many-statements
def client_comparison_service(
    db: Session,
    client_name: str,
    start_month: Optional[str],
    end_month: Optional[str],
    account_manager: Optional[str] = None,
):
    """Return month-wise and department-wise client allowance comparison."""
    if end_month and not start_month:
        raise HTTPException(
            status_code=400,
            detail="end_month cannot be provided without start_month.",
        )

    if not start_month and not end_month:
        latest_date = (
            db.query(func.max(ShiftAllowances.duration_month))
            .filter(ShiftAllowances.client == client_name)
            .scalar()
        )
        if not latest_date:
            raise HTTPException(
                status_code=404,
                detail=f"No records found for client '{client_name}'.",
            )
        start_date = date(latest_date.year, latest_date.month, 1)
        end_date = last_day_of_month(latest_date)
    else:
        start_date = parse_yyyy_mm(start_month)
        if end_month:
            end_date_raw = parse_yyyy_mm(end_month)
            if (end_date_raw.year, end_date_raw.month) < (start_date.year, start_date.month):
                raise HTTPException(
                    status_code=400,
                    detail="end_month must be greater than or equal to start_month.",
                )
            end_date = last_day_of_month(end_date_raw)
        else:
            end_date = last_day_of_month(start_date)

    current_month = date.today().replace(day=1)

    if (start_date.year, start_date.month) > (current_month.year, current_month.month):
        raise HTTPException(
            status_code=400,
            detail=f"start_month cannot be greater than current month ({current_month.strftime('%Y-%m')})."
        )

    if (end_date.year, end_date.month) > (current_month.year, current_month.month):
        raise HTTPException(
            status_code=400,
            detail=f"end_month cannot be greater than current month ({current_month.strftime('%Y-%m')})."
        )

    q = (
        db.query(
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.account_manager,
            ShiftAllowances.duration_month,
            ShiftAllowances.payroll_month,
            ShiftMapping.shift_type,
            ShiftMapping.days,
            ShiftsAmount.amount,
        )
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .join(
            ShiftsAmount,
            and_(
                ShiftsAmount.shift_type == ShiftMapping.shift_type,
                ShiftsAmount.payroll_year == func.to_char(ShiftAllowances.payroll_month, "YYYY"),
            ),
        )
        .filter(ShiftAllowances.client == client_name)
        .filter(
            ShiftAllowances.duration_month >= start_date,
            ShiftAllowances.duration_month <= end_date,
        )
    )

    if account_manager:
        q = q.filter(ShiftAllowances.account_manager == account_manager)

    rows = q.all()
    data: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for (
        emp_id,
        emp_name,
        department,
        client,
        account_manager_value,
        duration_month,
        payroll_month,
        shift_type,
        days,
        amount,
    ) in rows:
        if duration_month is None:
            continue

        month_key = month_key_from_date(duration_month)
        dept_key = department or "UNKNOWN"
        payroll_month_key = payroll_month.strftime("%Y-%m") if payroll_month else None

        month_bucket = data.setdefault(month_key, {})
        dept_bucket = month_bucket.setdefault(
            dept_key,
            {
                "total_allowance": 0.0,
                "dept_total_A": 0.0,
                "dept_total_B": 0.0,
                "dept_total_C": 0.0,
                "dept_total_PRIME": 0.0,
                "head_count_set": set(),
                "diff": 0.0,
                "emp": {},
            },
        )

        days_val = float(days or 0)
        amount_val = float(amount or 0)
        shift_allowance = days_val * amount_val

        emp_key = f"{emp_id}|{payroll_month_key or ''}"
        emp_bucket = dept_bucket["emp"].setdefault(
            emp_key,
            {
                "emp_id": emp_id,
                "emp_name": emp_name,
                "duration_month": month_key,
                "payroll_month": payroll_month_key,
                "account_manager": account_manager_value,
                "A": 0.0,
                "B": 0.0,
                "C": 0.0,
                "PRIME": 0.0,
                "total_allowance": 0.0,
            },
        )

        if shift_type in ("A", "B", "C", "PRIME"):
            emp_bucket[shift_type] += shift_allowance

        emp_bucket["total_allowance"] += shift_allowance
        dept_bucket["total_allowance"] += shift_allowance

        if shift_type == "A":
            dept_bucket["dept_total_A"] += shift_allowance
        elif shift_type == "B":
            dept_bucket["dept_total_B"] += shift_allowance
        elif shift_type == "C":
            dept_bucket["dept_total_C"] += shift_allowance
        elif shift_type == "PRIME":
            dept_bucket["dept_total_PRIME"] += shift_allowance

        dept_bucket["head_count_set"].add(emp_id)

    for month_key, month_bucket in data.items():
        for dept_key, dept_bucket in month_bucket.items():
            dept_bucket["head_count"] = len(dept_bucket["head_count_set"])
            del dept_bucket["head_count_set"]
            dept_bucket["emp"] = list(dept_bucket["emp"].values())

    sorted_months = sorted(data.keys())

    for idx in range(1, len(sorted_months)):
        prev_month_key = sorted_months[idx - 1]
        curr_month_key = sorted_months[idx]
        prev_month_bucket = data[prev_month_key]
        curr_month_bucket = data[curr_month_key]

        for dept_key, curr_dept_bucket in curr_month_bucket.items():
            if dept_key not in prev_month_bucket:
                continue
            prev_total = float(prev_month_bucket[dept_key]["total_allowance"])
            curr_total = float(curr_dept_bucket["total_allowance"])
            curr_dept_bucket["diff"] = curr_total - prev_total

    for month_key, month_bucket in data.items():
        total_allowance_month = 0.0
        emp_ids_month = set()

        for dept_key, dept_bucket in month_bucket.items():
            total_allowance_month += float(dept_bucket["total_allowance"])
            for emp in dept_bucket["emp"]:
                emp_ids_month.add(emp["emp_id"])

        month_bucket["vertical_total"] = {
            "total_allowance": total_allowance_month,
            "total_A": sum(float(month_bucket[d]["dept_total_A"]) for d in month_bucket if d != "vertical_total"),
            "total_B": sum(float(month_bucket[d]["dept_total_B"]) for d in month_bucket if d != "vertical_total"),
            "total_C": sum(float(month_bucket[d]["dept_total_C"]) for d in month_bucket if d != "vertical_total"),
            "total_PRIME": sum(float(month_bucket[d]["dept_total_PRIME"]) for d in month_bucket if d != "vertical_total"),
            "head_count": len(emp_ids_month),
        }

    sorted_months = sorted(data.keys())
    for idx in range(len(sorted_months)):
        curr_month_key = sorted_months[idx]
        curr_total = float(data[curr_month_key]["vertical_total"]["total_allowance"])

        y, m = map(int, curr_month_key.split("-"))
        prev_y = y if m > 1 else y - 1
        prev_m = m - 1 if m > 1 else 12
        prev_month_seq = f"{prev_y:04d}-{prev_m:02d}"

        if prev_month_seq not in data:
            data[curr_month_key]["vertical_total"]["month_total_diff"] = 0.0
        else:
            prev_total = float(data[prev_month_seq]["vertical_total"]["total_allowance"])
            data[curr_month_key]["vertical_total"]["month_total_diff"] = curr_total - prev_total

    horizontal_total: Dict[str, Dict[str, Any]] = {}

    for month_key, month_bucket in data.items():
        for dept_key, dept_bucket in month_bucket.items():
            if dept_key == "vertical_total":
                continue
            h_bucket = horizontal_total.setdefault(
                dept_key,
                {"total_allowance": 0.0, "emp_ids": set()},
            )
            h_bucket["total_allowance"] += float(dept_bucket["total_allowance"])
            for emp in dept_bucket["emp"]:
                h_bucket["emp_ids"].add(emp["emp_id"])

    for dept_key, h_bucket in horizontal_total.items():
        h_bucket["head_count"] = len(h_bucket["emp_ids"])
        del h_bucket["emp_ids"]

    all_months = []
    cur = start_date
    while cur <= end_date:
        all_months.append(cur.strftime("%Y-%m"))
        cur = date(cur.year + 1, 1, 1) if cur.month == 12 else date(cur.year, cur.month + 1, 1)

    final_result: Dict[str, Any] = {}

    for month_key in all_months:
        if month_key in data:
            final_result[month_key] = data[month_key]
        else:
            final_result[month_key] = {
                "message": "No data found",
                "vertical_total": {
                    "total_allowance": 0.0,
                    "total_A": 0.0,
                    "total_B": 0.0,
                    "total_C": 0.0,
                    "total_PRIME": 0.0,
                    "head_count": 0
                }
            }

    final_result["horizontal_total"] = horizontal_total

    return final_result


def get_client_total_allowances(db: Session, start_month: str | None, end_month: str | None, top: str | None):


    if top is None or str(top).strip().lower() == "all":
        top_int = None
    else:
        if not str(top).isdigit():
            raise HTTPException(status_code=400, detail="top must be a positive integer or 'all'")
        top_int = int(top)
        if top_int <= 0:
            raise HTTPException(status_code=400, detail="top must be greater than 0")

    def validate_month(m: str):
        try:
            datetime.strptime(m, "%Y-%m")
            return True
        except ValueError:
            return False

    def generate_months(start_m: str, end_m: str):
        result = []
        cur = datetime.strptime(start_m, "%Y-%m")
        end = datetime.strptime(end_m, "%Y-%m")
        while cur <= end:
            result.append(cur.strftime("%Y-%m"))
            cur += relativedelta(months=1)
        return result

    if end_month and not start_month:
        raise HTTPException(
            status_code=400,
            detail="start_month is required when end_month is provided"
        )

    if not start_month and not end_month:
        check_month = datetime.now().strftime("%Y-%m")
        months = []
        for _ in range(12):
            exists = (
                db.query(ShiftAllowances)
                .filter(func.to_char(ShiftAllowances.duration_month, 'YYYY-MM') == check_month)
                .first()
            )
            if exists:
                months.append(check_month)

            check_month = (
                datetime.strptime(check_month, "%Y-%m") - relativedelta(months=1)
            ).strftime("%Y-%m")

        if not months:
            return [{"message": "No data found for last 12 months"}]

    elif start_month and not end_month:
        if not validate_month(start_month):
            raise HTTPException(status_code=400, detail="start_month must be in YYYY-MM format")
        months = [start_month]

    else:
        if not validate_month(start_month):
            raise HTTPException(status_code=400, detail="start_month must be in YYYY-MM format")

        if not validate_month(end_month):
            raise HTTPException(status_code=400, detail="end_month must be in YYYY-MM format")

        if end_month < start_month:
            raise HTTPException(status_code=400, detail="end_month must be >= start_month")

        months = generate_months(start_month, end_month)

    rate_rows = db.query(ShiftsAmount).all()
    rates = {r.shift_type.upper(): Decimal(r.amount) for r in rate_rows}

    summary = {}

    for month in months:
        rows = (
            db.query(ShiftAllowances)
            .filter(func.to_char(ShiftAllowances.duration_month, 'YYYY-MM') == month)
            .all()
        )
        for row in rows:
            client = row.client or "Unknown"
            if client not in summary:
                summary[client] = Decimal(0)

            for mapping in row.shift_mappings:
                stype = mapping.shift_type.upper()
                days = Decimal(mapping.days or 0)
                summary[client] += days * rates.get(stype, Decimal(0))

    if not summary:
        raise HTTPException(
            status_code=404,
            detail="No shift allowance data found for the selected month(s)"
        )
    result = sorted(
        [{"client": c, "total_allowances": float(v)} for c, v in summary.items()],
        key=lambda x: x["total_allowances"],
        reverse=True
    )

    if top_int is not None:
        result = result[:top_int]

    return result


def get_client_departments_service(db: Session, client: str | None):

    if client is not None:
        client = client.strip()

        if not isinstance(client, str):
            raise HTTPException(status_code=400, detail="Client name must be a string")

        if client == "":
            raise HTTPException(status_code=400, detail="Client name cannot be empty")

        if client.isdigit():
            raise HTTPException(status_code=400, detail="Numbers are not allowed, only strings")

  
    if client:
        rows = (
            db.query(ShiftAllowances.department)
            .filter(
                ShiftAllowances.client == client,
                ShiftAllowances.client.isnot(None)
            )
            .all()
        )

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"Client '{client}' not found"
            )

        departments = sorted({r[0] for r in rows if r[0]})

        return [{
            "client": client,
            "departments": departments
        }]

 
    rows = (
        db.query(
            ShiftAllowances.client,
            ShiftAllowances.department
        )
        .filter(ShiftAllowances.client.isnot(None))
        .all()
    )

    result = {}

    for client_name, dept in rows:
        if not client_name:
            continue

        result.setdefault(client_name, set())

        if dept:
            result[client_name].add(dept)


    return [
        {
            "client": c,
            "departments": sorted(result[c])
        }
        for c in sorted(result.keys())
    ]
