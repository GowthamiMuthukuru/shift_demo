from datetime import datetime, date
from calendar import monthrange
from typing import Optional, Dict, Any
from collections import defaultdict
from utils.client_enums import Company
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount


def parse_yyyy_mm(value: str) -> date:
    try:
        dt = datetime.strptime(value, "%Y-%m")
        return date(dt.year, dt.month, 1)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid month format '{value}'. Expected YYYY-MM")


def last_day_of_month(d: date) -> date:
    _, last = monthrange(d.year, d.month)
    return date(d.year, d.month, last)


def _build_summary(rows: list) -> Dict[str, Any]:
    clients: Dict[str, Any] = {}
    global_emp_set = set()
    global_totals = {"A": 0.0, "B": 0.0, "C": 0.0, "PRIME": 0.0}

    for client_name, emp_id, emp_name, dept, account_manager, shift_type, days, amount in rows:
        dept = dept or "UNKNOWN"
        shift_allowance = float(days) * float(amount)

        global_emp_set.add(emp_id)
        global_totals[shift_type] += shift_allowance
        abbr = next((c.name for c in Company if c.value == client_name), None)
        client_key = abbr if abbr else client_name

        client_bucket = clients.setdefault(
            client_key,
            {
                "client_head_set": set(),
                "client_A": 0.0,
                "client_B": 0.0,
                "client_C": 0.0,
                "client_PRIME": 0.0,
                "departments": {},
            },
        )

        client_bucket["client_head_set"].add(emp_id)
        client_bucket[f"client_{shift_type}"] += shift_allowance

        dept_bucket = client_bucket["departments"].setdefault(
            dept,
            {
                "dept_head_set": set(),
                "dept_A": 0.0,
                "dept_B": 0.0,
                "dept_C": 0.0,
                "dept_PRIME": 0.0,
                "dept_total": 0.0,
                "employees": {},
            },
        )

        dept_bucket["dept_head_set"].add(emp_id)
        dept_bucket[f"dept_{shift_type}"] += shift_allowance
        dept_bucket["dept_total"] += shift_allowance

        emp_bucket = dept_bucket["employees"].setdefault(
            emp_id,
            {
                "emp_id": emp_id,
                "emp_name": emp_name,
                "account_manager": account_manager,
                "A": 0.0,
                "B": 0.0,
                "C": 0.0,
                "PRIME": 0.0,
                "total": 0.0,
            },
        )

        emp_bucket[shift_type] += shift_allowance
        emp_bucket["total"] += shift_allowance

    for client_key, client_bucket in clients.items():
        client_bucket["client_head_count"] = len(client_bucket["client_head_set"])
        del client_bucket["client_head_set"]
        client_bucket["client_total"] = (
            client_bucket["client_A"]
            + client_bucket["client_B"]
            + client_bucket["client_C"]
            + client_bucket["client_PRIME"]
        )

        for dept_key, dept_bucket in client_bucket["departments"].items():
            dept_bucket["dept_head_count"] = len(dept_bucket["dept_head_set"])
            del dept_bucket["dept_head_set"]
            dept_bucket["employees"] = list(dept_bucket["employees"].values())

    month_total = {
        "total_head_count": len(global_emp_set),
        "A": global_totals["A"],
        "B": global_totals["B"],
        "C": global_totals["C"],
        "PRIME": global_totals["PRIME"],
        "total_allowance": (
            global_totals["A"]
            + global_totals["B"]
            + global_totals["C"]
            + global_totals["PRIME"]
        ),
    }

    return {"clients": clients, "month_total": month_total}


def _generate_month_keys(start_date: date, end_date: date) -> list[str]:
    months = []
    current = date(start_date.year, start_date.month, 1)
    end_marker = date(end_date.year, end_date.month, 1)
    while current <= end_marker:
        months.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def client_summary_service(
    db: Session,
    client: Optional[str],
    account_manager: Optional[str],
    start_month: Optional[str],
    end_month: Optional[str]
):
    if not client and not account_manager:
        raise HTTPException(status_code=400, detail="Either client or account_manager must be provided.")

    if end_month and not start_month:
        raise HTTPException(status_code=400, detail="end_month cannot be provided without start_month.")

    # UPDATED LOGIC: pick latest month containing relevant data
    if not start_month and not end_month:
        latest_query = db.query(func.max(ShiftAllowances.duration_month))
        if client and client.upper() != "ALL":
            latest_query = latest_query.filter(ShiftAllowances.client == client)
        if account_manager:
            latest_query = latest_query.filter(ShiftAllowances.account_manager == account_manager)

        latest_date = latest_query.scalar()
        if not latest_date:
            return {}

        start_date = date(latest_date.year, latest_date.month, 1)
        end_date = last_day_of_month(latest_date)
    else:
        start_date = parse_yyyy_mm(start_month)
        if end_month:
            end_date_raw = parse_yyyy_mm(end_month)
            if (end_date_raw.year, end_date_raw.month) < (start_date.year, start_date.month):
                raise HTTPException(status_code=400, detail="end_month must be >= start_month.")
            end_date = last_day_of_month(end_date_raw)
        else:
            end_date = last_day_of_month(start_date)

    current_month = date.today().replace(day=1)
    if (start_date.year, start_date.month) > (current_month.year, current_month.month):
        raise HTTPException(status_code=400, detail="start_month cannot be greater than current month.")
    if (end_date.year, end_date.month) > (current_month.year, current_month.month):
        raise HTTPException(status_code=400, detail="end_month cannot be greater than current month.")

    q = (
        db.query(
            ShiftAllowances.duration_month,
            ShiftAllowances.client,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.department,
            ShiftAllowances.account_manager,
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
        .filter(
            ShiftAllowances.duration_month >= start_date,
            ShiftAllowances.duration_month <= end_date,
        )
    )

    if client and client.upper() != "ALL":
        q = q.filter(ShiftAllowances.client == client)

    if account_manager:
        q = q.filter(ShiftAllowances.account_manager == account_manager)

    rows = q.all()
    month_keys = _generate_month_keys(start_date, end_date)

    if not rows:
        if len(month_keys) == 1:
            return {month_keys[0]: {"message": "No data found"}}
        response: Dict[str, Any] = {}
        for mk in month_keys:
            response[mk] = {"message": "No data found"}
        response["total"] = {"clients": {}, "month_total": {}}
        return response

    month_rows: Dict[str, list] = defaultdict(list)
    all_rows: list = []

    for dm, client_name, emp_id, emp_name, dept, am, st, days, amt in rows:
        rec = (client_name, emp_id, emp_name, dept, am, st, days, amt)
        key = dm.strftime("%Y-%m")
        month_rows[key].append(rec)
        all_rows.append(rec)

    if len(month_keys) == 1:
        mk = month_keys[0]
        month_data = month_rows.get(mk, [])
        if not month_data:
            return {mk: {"message": "No data found"}}
        return {mk: _build_summary(month_data)}

    interval_summary = _build_summary(all_rows)
    response: Dict[str, Any] = {}

    for mk in month_keys:
        month_data = month_rows.get(mk)
        if month_data:
            response[mk] = _build_summary(month_data)
        else:
            response[mk] = {"message": "No data found"}

    response["total"] = interval_summary
    return response
