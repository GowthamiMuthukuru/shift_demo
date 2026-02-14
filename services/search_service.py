"""Shift allowance export service with proper headcount and shift filtering."""

import re
from datetime import datetime, date
from typing import List, Union, Optional, Dict, Any, Tuple
from collections import Counter

from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, extract
from sqlalchemy.sql import exists

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from utils.client_enums import Company
from utils.shift_config import get_all_shift_keys, get_allowance_columns,get_shift_string


def _validate_year_int(y: int, today: Optional[date] = None) -> int:
    today = today or date.today()
    if not (1000 <= y <= 9999):
        raise HTTPException(400, "Years must be 4-digit integers (YYYY)")
    if y > today.year:
        raise HTTPException(400, f"Future year {y} cannot be selected")
    return y

def _validate_month_int(m: int) -> int:
    if not (1 <= m <= 12):
        raise HTTPException(400, "Months must be integers between 1 and 12")
    return m

def normalize_company_name(client: str | None):
    if not client:
        return None
    for company in Company:
        if company.name == client.upper():
            return company.value
    return client

def _normalize_to_list(value: Union[str, List[str], None]) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, list):
        vals = [str(v).strip() for v in value if str(v).strip()]
        return vals or None
    s = str(value).strip()
    return [s] if s and s.upper() != "ALL" else None

def apply_client_department_filters(query, clients=None, departments=None):
    client_values = _normalize_to_list(clients)
    dept_values = _normalize_to_list(departments)

    conditions = []
    if client_values:
        like_terms = []
        for c in client_values:
            norm = normalize_company_name(c) or c
            like_terms.append(func.upper(ShiftAllowances.client).like(f"%{norm.strip().upper()}%"))
        conditions.append(or_(*like_terms))

    if dept_values:
        like_terms = [func.upper(ShiftAllowances.department).like(f"%{d.strip().upper()}%") for d in dept_values]
        conditions.append(or_(*like_terms))

    if conditions:
        return query.filter(and_(*conditions))
    return query

def get_default_start_month(db: Session) -> str:
    today = datetime.now().replace(day=1)
    for i in range(12):
        y = today.year
        m = today.month - i
        if m <= 0:
            m += 12
            y -= 1
        ym = f"{y:04d}-{m:02d}"
        exists_row = db.query(ShiftAllowances.id).filter(
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == ym
        ).first()
        if exists_row:
            return ym
    raise HTTPException(404, "No data found in the last 12 months")

def _resolve_periods_with_meta(
    db: Session,
    years: Optional[List[int]],
    months: Optional[List[int]]
) -> Tuple[List[Tuple[int, int]], Dict[str, Any]]:
    today = date.today()
    meta: Dict[str, Any] = {
        "assumed_current_year": False,
        "current_month_attempted": None,
        "current_month_fallback_used": None,
        "excluded_future_periods": [],
    }

    if not years and not months:
        current_ym = f"{today.year:04d}-{today.month:02d}"
        meta["current_month_attempted"] = current_ym
        has_current = db.query(ShiftAllowances.id).filter(
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == current_ym
        ).first()
        if has_current:
            return [(today.year, today.month)], meta
        latest = get_default_start_month(db)
        meta["current_month_fallback_used"] = latest
        y, m = latest.split("-")
        return [(int(y), int(m))], meta

    years_norm = [_validate_year_int(int(y), today) for y in (years or [])]
    months_norm = [_validate_month_int(int(m)) for m in (months or [])]
    periods: List[Tuple[int, int]] = []

    if months_norm and not years_norm:
        meta["assumed_current_year"] = True
        y = today.year
        for m in months_norm:
            if m > today.month:
                meta["excluded_future_periods"].append(f"{y:04d}-{m:02d}")
                continue
            periods.append((y, m))
    elif months_norm and years_norm:
        for y in years_norm:
            for m in months_norm:
                if y == today.year and m > today.month:
                    meta["excluded_future_periods"].append(f"{y:04d}-{m:02d}")
                    continue
                periods.append((y, m))
    elif years_norm and not months_norm:
        for y in years_norm:
            upper = today.month if y == today.year else 12
            for m in range(1, upper + 1):
                periods.append((y, m))

    periods = sorted(set(periods))
    if not periods:
        fut = meta["excluded_future_periods"]
        if fut:
            raise HTTPException(400, f"All requested periods are in the future: {', '.join(fut)}")
        raise HTTPException(404, "No valid (year, month) periods to query")

    return periods, meta

_HEADCOUNT_RANGE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")
_SINGLE_NUM_RE = re.compile(r"^\s*(\d+)\s*$")

def _parse_headcount_ranges(headcounts: Union[str, List[str], None]) -> Optional[List[Tuple[int, int]]]:
    vals = _normalize_to_list(headcounts)
    if not vals:
        return None
    ranges: List[Tuple[int, int]] = []
    for token in vals:
        for part in token.split(","):
            part = part.strip()
            m = _HEADCOUNT_RANGE_RE.match(part)
            if m:
                a, b = int(m.group(1)), int(m.group(2))
                if a <= 0 or b <= 0:
                    raise HTTPException(400, "Headcount range values must be positive integers")
                if a > b:
                    a, b = b, a
                ranges.append((a, b))
                continue
            n = _SINGLE_NUM_RE.match(part)
            if n:
                v = int(n.group(1))
                if v <= 0:
                    raise HTTPException(400, "Headcount value must be a positive integer")
                ranges.append((v, v))
                continue
            raise HTTPException(
                400,
                f"Invalid headcount range value: '{part}'. Use numeric ranges like '1-5' or single numbers like '3'."
            )
    return ranges

def _apply_headcount_filter(unique_employees: List[Dict[str, Any]], group_key: Optional[str], ranges: Optional[List[Tuple[int, int]]]) -> List[Dict[str, Any]]:
    """Filters employees based on headcount.
    - If group_key is None, treat as individual employees and select by position in list.
    - If group_key is provided, filter groups by employee count in range.
    """
    if not ranges:
        return unique_employees

    if group_key is None:
       
        allowed_indices = set()
        for lo, hi in ranges:
            allowed_indices.update(range(lo, hi+1))
        filtered = [e for i, e in enumerate(unique_employees, start=1) if i in allowed_indices]
        return filtered

    
    group_vals = [str(emp.get(group_key) or "UNKNOWN").upper() for emp in unique_employees]
    counts = Counter(group_vals)
    allowed_groups = set()
    for grp, cnt in counts.items():
        for lo, hi in ranges:
            if lo <= cnt <= hi:
                allowed_groups.add(grp.upper())
                break
    return [emp for emp in unique_employees if str(emp.get(group_key) or "UNKNOWN").upper() in allowed_groups]

def _compute_row_totals(db: Session, row, rates: Dict[str, float]):
    shift_days: Dict[str, float] = {}
    shift_amount: Dict[str, float] = {}
    total = 0.0

    mappings = db.query(ShiftMapping).filter(
        ShiftMapping.shiftallowance_id == row.id
    ).all()

    for m in mappings:
        days = float(m.days or 0)
        if days <= 0:
            continue

        shift_key = (m.shift_type or "").upper().strip()
        rate = float(rates.get(shift_key, 0.0))
        amount = days * rate

        shift_days[shift_key] = shift_days.get(shift_key, 0.0) + days

     
        shift_amount[shift_key] = shift_amount.get(shift_key, 0.0) + amount

        total += amount

    return shift_days, shift_amount, total

def _aggregate_unique_employees(db: Session, rows, rates: Dict[str, float]) -> List[Dict[str, Any]]:

    def _ym_to_key(ym: str) -> Tuple[int, int]:
        y, m = ym.split("-")
        return int(y), int(m)

    agg: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        row_shift_days, row_shift_amount, row_total = _compute_row_totals(db, row, rates)

        emp_id = row.emp_id
        latest_ym = row.duration_month

        if emp_id not in agg:
            agg[emp_id] = {
                "emp_id": row.emp_id,
                "emp_name": row.emp_name,
                "department": row.department or "UNKNOWN",
                "client": row.client or "UNKNOWN",
                "project": row.project,
                "client_partner": row.client_partner,
                "duration_month": row.duration_month,
                "payroll_month": row.payroll_month,

             
                "shift_days": dict(row_shift_days),

              
                "shift_details": dict(row_shift_amount),

                "total_allowance": float(row_total),
                "_latest_key": _ym_to_key(latest_ym),
            }
        else:
            cur = agg[emp_id]

          
            for k, v in row_shift_days.items():
                cur["shift_days"][k] = cur["shift_days"].get(k, 0.0) + v

            for k, v in row_shift_amount.items():
                cur["shift_details"][k] = cur["shift_details"].get(k, 0.0) + v

            cur["total_allowance"] += float(row_total)

            if _ym_to_key(latest_ym) > cur["_latest_key"]:
                cur["_latest_key"] = _ym_to_key(latest_ym)
                cur["department"] = row.department or "UNKNOWN"
                cur["client"] = row.client or "UNKNOWN"
                cur["project"] = row.project
                cur["client_partner"] = row.client_partner
                cur["duration_month"] = row.duration_month
                cur["payroll_month"] = row.payroll_month

    unique_employees: List[Dict[str, Any]] = []

    for emp in agg.values():
        emp["shift_days"] = {k: round(v, 2) for k, v in emp["shift_days"].items()}
        emp["shift_details"] = {k: round(v, 2) for k, v in emp["shift_details"].items()}
        emp["total_allowance"] = round(float(emp["total_allowance"]), 2)
        emp.pop("_latest_key", None)
        unique_employees.append(emp)

    return unique_employees

def aggregate_shift_details(db, rows, rates):
    overall = {k: 0.0 for k in get_all_shift_keys()}
    total = 0.0
    for row in rows:
        mappings = db.query(ShiftMapping).filter(ShiftMapping.shiftallowance_id == row.id).all()
        for m in mappings:
            days = float(m.days or 0)
            if days <= 0:
                continue
            shift_key = (m.shift_type or "").upper().strip()
            rate = float(rates.get(shift_key, 0.0))
            amount = days * rate
            overall[shift_key] += amount
            total += amount
    return overall, total
def export_filtered_excel(
    db: Session,
    emp_id: Optional[str] = None,
    client_partner: Optional[str] = None,
    start: int = 0,
    limit: int = 10,
    clients: Union[str, List[str]] = "ALL",
    departments: Union[str, List[str]] = "ALL",
    years: Optional[List[int]] = None,
    months: Optional[List[int]] = None,
    shifts: Union[str, List[str]] = "ALL",
    headcounts: Union[str, List[str]] = "ALL",
    sort_by: str = "total_allowance",
    sort_order: str = "default",
):

    # Resolve periods (years/months) and capture meta info
    periods, meta = _resolve_periods_with_meta(db, years, months)

    messages: List[str] = []

    if meta.get("assumed_current_year"):
        messages.append(
            f"Months provided without years; assumed current year {date.today().year}."
        )

    if meta.get("excluded_future_periods"):
        messages.append(
            f"Excluded future period(s): {', '.join(meta['excluded_future_periods'])}."
        )

    if meta.get("current_month_attempted") and meta.get("current_month_fallback_used"):
        cm = meta["current_month_attempted"]
        fb = meta["current_month_fallback_used"]
        messages.append(f"No data for current month {cm}; fell back to {fb}.")

    # Normalize shifts
    shift_values = _normalize_to_list(shifts)
    if shift_values:
        allowed = {s.upper().strip() for s in get_all_shift_keys()}
        invalid = [
            s.upper().strip() for s in shift_values if s.upper().strip() not in allowed
        ]
        if invalid:
            raise HTTPException(
                400,
                f"Invalid shift type(s): {', '.join(invalid)}. Allowed: {', '.join(sorted(allowed))}.",
            )

    # Base query
    def _build_base_query():
        q = db.query(
            ShiftAllowances.id,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.project,
            ShiftAllowances.client_partner,
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM").label("duration_month"),
            func.to_char(ShiftAllowances.payroll_month, "YYYY-MM").label("payroll_month"),
        )

        clauses = [
            and_(
                extract("year", ShiftAllowances.duration_month) == y,
                extract("month", ShiftAllowances.duration_month) == m,
            )
            for (y, m) in periods
        ]

        return q.filter(or_(*clauses))

    # Apply filters
    def _apply_filters(q):
        if emp_id:
            q = q.filter(func.upper(ShiftAllowances.emp_id).like(f"%{emp_id.upper()}%"))

        if client_partner:
            q = q.filter(
                func.upper(ShiftAllowances.client_partner).like(f"%{client_partner.upper()}%")
            )

        q = apply_client_department_filters(q, clients=clients, departments=departments)

        if shift_values:
            shift_values_up = [s.upper().strip() for s in shift_values]

            q = q.filter(
                exists().where(
                    and_(
                        ShiftMapping.shiftallowance_id == ShiftAllowances.id,
                        func.upper(func.trim(ShiftMapping.shift_type)).in_(shift_values_up),
                        or_(ShiftMapping.days.is_(None), ShiftMapping.days > 0),
                    )
                )
            )

        q = q.order_by(
            extract("year", ShiftAllowances.duration_month).asc(),
            extract("month", ShiftAllowances.duration_month).asc(),
            ShiftAllowances.emp_id.asc(),
        )

        return q

    all_rows = _apply_filters(_build_base_query()).all()

    if not all_rows:
        extra = (" " + " ".join(messages)) if messages else ""
        raise HTTPException(404, f"No data found for selected period/filters.{extra}")

    # Load shift rates
    rates = {(r.shift_type or "").upper().strip(): float(r.amount or 0) for r in db.query(ShiftsAmount).all()}

    # Aggregate unique employees with shift totals
    unique_employees = _aggregate_unique_employees(db, all_rows, rates)

    # Parse headcount ranges
    headcount_ranges = _parse_headcount_ranges(headcounts)

    dept_vals = _normalize_to_list(departments)
    client_vals = _normalize_to_list(clients)

    group_key = None
    if dept_vals:
        group_key = "department"
    elif client_vals:
        group_key = "client"

    # Apply headcount filter
    filtered_employees = _apply_headcount_filter(unique_employees, group_key, headcount_ranges)

    if not filtered_employees:
        extra = (" " + " ".join(messages)) if messages else ""
        raise HTTPException(404, f"No employees match the requested headcount range(s).{extra}")

    filtered_emp_ids = {emp["emp_id"] for emp in filtered_employees}
    filtered_rows = [r for r in all_rows if r.emp_id in filtered_emp_ids]

    # Aggregate shift details
    overall_shift, overall_total = aggregate_shift_details(db, filtered_rows, rates)
    overall_shift["headcount"] = len(filtered_employees)

    sort_by_key = (sort_by or "total_allowance").strip().lower()
    sort_order_in = (sort_order or "default").strip().lower()

    valid_sort = {"client", "client_partner", "departments", "total_allowance", "headcount"}

    if sort_by_key not in valid_sort:
        raise HTTPException(400, f"sort_by must be one of {', '.join(sorted(valid_sort))}")

    if sort_order_in not in {"default", "asc", "desc"}:
        raise HTTPException(400, "sort_order must be 'default', 'asc', or 'desc'")

    direction = sort_order_in if sort_order_in != "default" else ("desc" if sort_by_key in {"total_allowance", "headcount"} else "asc")
    reverse = direction == "desc"

    if sort_by_key == "total_allowance":
        filtered_employees.sort(
            key=lambda e: (e.get("total_allowance", 0.0), e.get("emp_id", "")),
            reverse=reverse,
        )
    elif sort_by_key == "headcount":
 
        counts = Counter()
        key_field = "department" if dept_vals else "client"
        for emp in filtered_employees:
            counts[emp.get(key_field) or "UNKNOWN"] += 1

        filtered_employees.sort(
            key=lambda e: (counts.get(e.get(key_field) or "UNKNOWN", 0), e.get("emp_id", "")),
            reverse=reverse,
        )
    elif sort_by_key in {"client", "client_partner"}:
        filtered_employees.sort(
            key=lambda e: (str(e.get(sort_by_key) or "").upper(), e.get("emp_id", "")),
            reverse=reverse,
        )
    elif sort_by_key == "departments":
        filtered_employees.sort(
            key=lambda e: (str(e.get("department") or "").upper(), e.get("emp_id", "")),
            reverse=reverse,
        )

    # Pagination
    total_unique = len(filtered_employees)
    employees_page = filtered_employees[start : start + limit]

    # Format shift summary for response
    formatted_shift_summary = {}
    for shift_key, amount in overall_shift.items():
        if amount > 0:
            label = get_shift_string(shift_key) or shift_key
            formatted_shift_summary[label.replace("\n", " ")] = round(amount, 2)

    # Prepare response
    response = {
        "total_records": total_unique,
        "shift_details": [
            formatted_shift_summary,
            {"total_allowance": round(overall_total, 2)},
        ],
        "data": {"employees": employees_page},
    }

    if messages:
        response["message"] = " ".join(messages)

    return response
