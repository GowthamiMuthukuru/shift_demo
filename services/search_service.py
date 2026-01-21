"""Shift allowance export service.

Fetches and aggregates employee shift allowance data. Filtering is done
using duration_month only, while payroll_month is shown for display.

Future years, months, or quarters are not allowed. If a quarter is
selected, it must have started and have complete data for all months.

If no filters are selected, the latest month available in the database
is returned. Results are sorted by month, include shift-wise totals,
and support pagination and employee-level summaries.

Client filtering supports single client with optional department:
- client="ALL" → all clients
- client="DZS Inc", department=None → filter by client only
- client="DZS Inc", department="Infra - IT Operations" → filter by client + dept
"""

import re
from datetime import datetime, date
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, extract

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from utils.client_enums import Company


def validate_year(year: str):
    """Validate selected year is a 4-digit integer and not in future."""
    if not year.isdigit() or len(year) != 4:
        raise HTTPException(400, "selected_year must be a 4-digit year (YYYY)")
    year_int = int(year)
    if year_int > date.today().year:
        raise HTTPException(400, "Future year cannot be selected")
    return year_int


def validate_month(month: str):
    """Validate month string is 01-12."""
    if not month.isdigit() or not 1 <= int(month) <= 12:
        raise HTTPException(400, "selected_months must be between 01 and 12")
    return int(month)


def validate_quarters(quarters):
    """Ensure selected quarters are one of Q1-Q4."""
    valid = {"Q1", "Q2", "Q3", "Q4"}
    for q in quarters:
        if q.upper() not in valid:
            raise HTTPException(400, "selected_quarters must be one of Q1, Q2, Q3, Q4")


def normalize_company_name(client: str | None):
    """Normalize client name using Company enum if exists."""
    if not client:
        return None
    for company in Company:
        if company.name == client.upper():
            return company.value
    return client


def apply_client_department_filters(query, client=None, department=None):
    """
    Search-based filtering for client and/or department.
    Works independently or together.
    """
    conditions = []

    if client and client.strip().upper() != "ALL":
        client_norm = normalize_company_name(client)
        conditions.append(
            func.upper(ShiftAllowances.client).like(
                f"%{client_norm.strip().upper()}%"
            )
        )

    if department:
        conditions.append(
            func.upper(ShiftAllowances.department).like(
                f"%{department.strip().upper()}%"
            )
        )

    if conditions:
        return query.filter(and_(*conditions))

    return query


def get_quarter_months(q):
    """Return list of months for given quarter string."""
    return {
        "Q1": [1, 2, 3],
        "Q2": [4, 5, 6],
        "Q3": [7, 8, 9],
        "Q4": [10, 11, 12],
    }[q]


def get_default_start_month(db: Session) -> str:
    """Get the most recent month with data in the last 12 months."""
    today = datetime.now().replace(day=1)
    for i in range(12):
        y = today.year
        m = today.month - i
        if m <= 0:
            m += 12
            y -= 1
        month_str = f"{y:04d}-{m:02d}"
        exists = db.query(ShiftAllowances.id).filter(
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == month_str
        ).first()
        if exists:
            return month_str
    raise HTTPException(404, "No data found in the last 12 months")


def aggregate_shift_details(db, rows, rates, labels):
    """Aggregate shift allowance amounts by shift type."""
    overall = {v: 0.0 for v in labels.values()}
    total = 0.0
    for row in rows:
        mappings = db.query(ShiftMapping).filter(
            ShiftMapping.shiftallowance_id == row.id
        ).all()
        for m in mappings:
            days = float(m.days or 0)
            if days <= 0:
                continue
            rate = rates.get(m.shift_type.upper(), 0)
            amount = days * rate
            label = labels.get(m.shift_type.upper(), m.shift_type)
            overall[label] += amount
            total += amount
    return overall, total


def prepare_employee_data(db, rows, rates, labels):
    """Prepare employee-wise shift and allowance details."""
    employees = []
    for row in rows:
        rec = row._asdict()
        shift_id = rec.pop("id")
        emp_shift = {}
        total = 0.0
        mappings = db.query(ShiftMapping).filter(
            ShiftMapping.shiftallowance_id == shift_id
        ).all()
        for m in mappings:
            days = float(m.days or 0)
            if days <= 0:
                continue
            rate = rates.get(m.shift_type.upper(), 0)
            total += days * rate
            label = labels.get(m.shift_type.upper(), m.shift_type)
            emp_shift[label] = emp_shift.get(label, 0) + days
        rec["shift_details"] = {k: int(v) for k, v in emp_shift.items()}
        rec["total_allowance"] = round(total, 2)
        employees.append(rec)
    return employees


def export_filtered_excel(
    db: Session,
    emp_id=None,
    account_manager=None,
    start_month=None,
    end_month=None,
    start=0,
    limit=10,
    clients="ALL",
    department=None,
    selected_year=None,
    selected_months=None,
    selected_quarters=None,
):
    """Export shift allowances filtered by month, year, quarter, client, or dept."""
    today = date.today()
    allowed_months = set()
    year = None

    if selected_year:
        year = validate_year(selected_year)
        if selected_months:
            for m in selected_months:
                month_int = validate_month(m)
                if year == today.year and month_int > today.month:
                    raise HTTPException(
                        400, f"Future month {month_int:02d} is not allowed"
                    )
                allowed_months.add(month_int)
        if selected_quarters:
            validate_quarters(selected_quarters)
            for q in selected_quarters:
                q_months = get_quarter_months(q.upper())
                if not any(
                    year < today.year or (year == today.year and m <= today.month)
                    for m in q_months
                ):
                    raise HTTPException(
                        400, f"{q.upper()} has not started yet and cannot be selected"
                    )
                for m in q_months:
                    if year < today.year or (year == today.year and m <= today.month):
                        allowed_months.add(m)
        if allowed_months:
            start_month = f"{year}-{min(allowed_months):02d}"
            end_month = f"{year}-{max(allowed_months):02d}"

    if not start_month or not end_month:
        latest_month = get_default_start_month(db)
        start_month = start_month or latest_month
        end_month = end_month or latest_month

    if not re.fullmatch(r"\d{4}-\d{2}", start_month):
        raise HTTPException(400, "start_month must be in YYYY-MM format")
    if not re.fullmatch(r"\d{4}-\d{2}", end_month):
        raise HTTPException(400, "end_month must be in YYYY-MM format")

    start_dt = datetime.strptime(start_month, "%Y-%m")
    end_dt = datetime.strptime(end_month, "%Y-%m")
    if end_dt.date() > date(today.year, today.month, 1):
        raise HTTPException(400, "Future months are not allowed in date range")

    base = db.query(
        ShiftAllowances.id,
        ShiftAllowances.emp_id,
        ShiftAllowances.emp_name,
        ShiftAllowances.department,
        ShiftAllowances.client,
        ShiftAllowances.project,
        ShiftAllowances.account_manager,
        func.to_char(ShiftAllowances.duration_month, "YYYY-MM").label("duration_month"),
        func.to_char(ShiftAllowances.payroll_month, "YYYY-MM").label("payroll_month"),
    )

    if allowed_months:
        base = base.filter(
            extract("year", ShiftAllowances.duration_month) == year,
            extract("month", ShiftAllowances.duration_month).in_(allowed_months)
        )
    else:
        base = base.filter(
            ShiftAllowances.duration_month >= start_dt,
            ShiftAllowances.duration_month <= end_dt
        )

    if emp_id:
        base = base.filter(func.upper(ShiftAllowances.emp_id).like(f"%{emp_id.upper()}%"))
    if account_manager:
        base = base.filter(func.upper(ShiftAllowances.account_manager).like(f"%{account_manager.upper()}%"))

    base = apply_client_department_filters(base, clients, department)

    base = base.order_by(
        extract("year", ShiftAllowances.duration_month).asc(),
        extract("month", ShiftAllowances.duration_month).asc(),
    )

    all_rows = base.all()

    if selected_quarters:
        expected_months = {f"{year}-{m:02d}" for m in sorted(allowed_months)}
        available_months = {r.duration_month for r in all_rows}
        if available_months != expected_months:
            raise HTTPException(
                404, "No data found for the selected quarter period"
            )

    if not all_rows:
        raise HTTPException(404, "No data found for the selected period")

    total_records = len(all_rows)
    headcount = len({r.emp_id for r in all_rows})

    labels = {
        "A": "A(9PM to 6AM)",
        "B": "B(4PM to 1AM)",
        "C": "C(6AM to 3PM)",
        "PRIME": "PRIME(12AM to 9AM)",
    }

    rates = {r.shift_type.upper(): float(r.amount or 0) for r in db.query(ShiftsAmount).all()}

    overall_shift, overall_total = aggregate_shift_details(db, all_rows, rates, labels)

    paginated_rows = all_rows[start:start + limit]
    employees = prepare_employee_data(db, paginated_rows, rates, labels)

    return {
        "total_records": total_records,
        "shift_details": {**{k: v for k, v in overall_shift.items() if v > 0},
                          "headcount": headcount,
                          "total_allowance": round(overall_total, 2)},
        "data": {"employees": employees},
    }
