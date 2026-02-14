"""Services for client comparison, totals, and department summaries."""
from __future__ import annotations
from datetime import datetime, date,timedelta
from calendar import monthrange

from typing import Optional, Dict, Any, Union,List,Tuple,Set
from decimal import Decimal,InvalidOperation
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from dateutil.relativedelta import relativedelta
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from utils.shift_config import get_all_shift_keys
from schemas.dashboardschema import ClientTotalAllowanceFilter
from collections import OrderedDict

import re
from collections import defaultdict
from sqlalchemy import func, and_, or_, cast, Integer, extract


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


def _safe_int(v, default=None) -> Optional[int]:
    if v is None:
        return default
    try:
        return int(Decimal(str(v)))
    except (InvalidOperation, ValueError, TypeError):
        return default


def _normalize_years(years: List[int]) -> List[int]:
    seen, result = set(), []
    for y in years or []:
        yi = _safe_int(y)
        if yi is not None and yi not in seen:
            seen.add(yi)
            result.append(yi)
    return result


def _normalize_months(months: List[int]) -> List[int]:
    seen, result = set(), []
    for m in months or []:
        mi = _safe_int(m)
        if mi is not None and 1 <= mi <= 12 and mi not in seen:
            seen.add(mi)
            result.append(mi)
    return result

def _parse_headcount_filter(filter_value):
    """
    Accepts: None, "ALL", [0] -> None
             "n" (e.g., "5") -> eq rule
             "a-b" (e.g., "1-10") -> range rule
             list of the above -> multiple rules (OR)
    Returns: list of ("range", a, b) | ("eq", n) or None
    """
    if filter_value in (None, "ALL", [0]):
        return None

    rules = []
    values = filter_value if isinstance(filter_value, list) else [filter_value]

    for item in values:
        s = str(item).strip()

        if "-" in s:
            parts = s.split("-")
            if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
                raise HTTPException(
                    400,
                    f"Invalid headcount range: {item}. Use format like 1-10."
                )

            start, end = int(parts[0]), int(parts[1])
            if start > end:
                raise HTTPException(
                    400,
                    f"Invalid headcount range (start > end): {item}"
                )

            rules.append(("range", start, end))

        elif s.isdigit():
            rules.append(("eq", int(s)))

        else:
            raise HTTPException(
                400,
                f"Invalid headcount format: {item}. Use 5 or 1-10."
            )

    return rules


def _headcount_matches(value: Optional[int], rules) -> bool:
    if rules is None:
        return True
    if value is None:
        return False

    for rule in rules:
        if rule[0] == "range":
            _, start, end = rule
            if start <= value <= end:
                return True
        elif rule[0] == "eq":
            _, expected = rule
            if value == expected:
                return True
    return False

def _normalize_shifts_filter(shifts: Union[str, List[str], None]) -> Optional[Set[str]]:
    """
    Returns a set of uppercase shift keys or None if "ALL"/None.
    """
    if shifts is None or (isinstance(shifts, str) and shifts.strip().upper() == "ALL"):
        return None
    if isinstance(shifts, str):
        return {shifts.strip().upper()}
    # list
    cleaned = {str(s).strip().upper() for s in shifts if str(s).strip()}
    return cleaned or None

def get_client_total_allowances(db: Session, filters):
    today = date.today()
    current_year = today.year
    current_month = today.month
    messages: List[str] = []

    raw_years = filters.years or []
    raw_months = filters.months or []

    if raw_years == [0]:
        raw_years = []
    if raw_months == [0]:
        raw_months = []

    years = _normalize_years(raw_years)
    months = _normalize_months(raw_months)

    base_query = db.query(ShiftAllowances)

    if filters.clients != "ALL":
        if isinstance(filters.clients, list):
            base_query = base_query.filter(ShiftAllowances.client.in_(filters.clients))
        else:
            base_query = base_query.filter(ShiftAllowances.client == filters.clients)

    if filters.departments != "ALL":
        if isinstance(filters.departments, list):
            base_query = base_query.filter(ShiftAllowances.department.in_(filters.departments))
        else:
            base_query = base_query.filter(ShiftAllowances.department == filters.departments)

    # -----------------------------
    # Determine years/months
    # -----------------------------
    if not years and not months:
        current_exists = base_query.filter(
            func.extract("year", ShiftAllowances.duration_month) == current_year,
            func.extract("month", ShiftAllowances.duration_month) == current_month
        ).first()

        if current_exists:
            years = [current_year]
            months = [current_month]
        else:
            cutoff = today.replace(day=1) - relativedelta(months=12)
            latest = (
                base_query
                .filter(ShiftAllowances.duration_month >= cutoff)
                .order_by(ShiftAllowances.duration_month.desc())
                .first()
            )
            if latest and latest.duration_month:
                latest_date = latest.duration_month
                years = [latest_date.year]
                months = [latest_date.month]
                messages.append(
                    f"No data for current month. Showing latest available month: "
                    f"{latest_date.month}-{latest_date.year}"
                )
            else:
                years = [current_year]
                months = [current_month]
    elif months and not years:
        years = [current_year]
    elif years and not months:
        months = list(range(1, 13))

    q = base_query.filter(
        func.extract("year", ShiftAllowances.duration_month).in_(years)
    ).filter(
        func.extract("month", ShiftAllowances.duration_month).in_(months)
    )

    if getattr(filters, "emp_id", None):
        emp_list = filters.emp_id if isinstance(filters.emp_id, list) else [filters.emp_id]
        q = q.filter(or_(*[func.upper(ShiftAllowances.emp_id) == str(e).upper() for e in emp_list]))

    if getattr(filters, "client_partner", None):
        cp_list = filters.client_partner if isinstance(filters.client_partner, list) else [filters.client_partner]
        q = q.filter(
            or_(*[func.upper(ShiftAllowances.client_partner).like(f"%{str(cp).upper()}%") for cp in cp_list])
        )

    rows = q.all()

    rate_rows = db.query(ShiftsAmount).all()
    rates = {str(r.shift_type).upper(): Decimal(r.amount or 0) for r in rate_rows}

    allowed_shifts: Optional[Set[str]] = _normalize_shifts_filter(getattr(filters, "shifts", None))

   
    client_totals: Dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    client_emps: Dict[str, set] = defaultdict(set)
    client_shifts: Dict[str, Dict[str, Decimal]] = defaultdict(lambda: defaultdict(lambda: Decimal(0)))
    client_depts: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(lambda: defaultdict(lambda: {
        "total": Decimal(0),
        "shifts": defaultdict(lambda: Decimal(0)),
        "emp_ids": set()
    }))

    for row in rows:
        client = row.client or "Unknown"
        dept = row.department or "Unknown"
        emp = str(row.emp_id) if row.emp_id else None

        row_contributed = False
        mappings = getattr(row, "shift_mappings", []) or []

        for mapping in mappings:
            shift_key = str(mapping.shift_type or "").upper()
            days = Decimal(mapping.days or 0)
            if days <= 0:
                continue
            if allowed_shifts and shift_key not in allowed_shifts:
                continue
            rate = rates.get(shift_key, Decimal(0))
            amount = days * rate
            if amount <= 0:
                continue

            client_totals[client] += amount
            client_shifts[client][shift_key] += amount
            client_depts[client][dept]["total"] += amount
            client_depts[client][dept]["shifts"][shift_key] += amount

            row_contributed = True

        if row_contributed and emp:
            client_emps[client].add(emp)
            client_depts[client][dept]["emp_ids"].add(emp)

   
    headcount_rules = _parse_headcount_filter(getattr(filters, "headcounts", None))
    result: List[Dict[str, Any]] = []

    for client, total in client_totals.items():
        headcount = len(client_emps.get(client, set()))
        if not _headcount_matches(headcount, headcount_rules):
            continue

        shifts_out = {k: float(v) for k, v in client_shifts.get(client, {}).items() if v > 0}
        depts_out = []

        for dept, drec in client_depts[client].items():
            d_head = len(drec["emp_ids"])
            d_total = float(drec["total"])
            d_shifts = {sk: float(av) for sk, av in drec["shifts"].items() if av > 0}
            if d_total > 0:
                depts_out.append({
                    "department": dept,
                    "headcount": d_head,
                    "total_allowance": d_total,
                    "shifts": d_shifts
                })

        result.append({
            "client": client,
            "headcount": headcount,
            "total_allowance": float(total),
            "shifts": shifts_out,
            "departments": depts_out
        })

    result.sort(key=lambda x: x["total_allowance"], reverse=True)


    top_value = getattr(filters, "top", None)
    if top_value is not None:
        if str(top_value).lower() == "all":
            pass  
        else:
            if not str(top_value).isdigit():
                raise HTTPException(
                    status_code=400,
                    detail="top must be a positive integer or ALL"
                )
            top_int = int(top_value)
            if top_int <= 0:
                raise HTTPException(
                    status_code=400,
                    detail="top must be greater than 0"
                )
            result = result[:top_int]

    if not result and not messages:
        messages.append("No data found for selected periods.")

    return {
        "selected_periods": [{"year": y, "months": months} for y in years],
        "messages": messages,
        "data": result
    }

def get_client_departments_service(db: Session):

    rows = (
        db.query(ShiftAllowances.department)
        .filter(ShiftAllowances.department.isnot(None))
        .distinct()
        .all()
    )

    departments = sorted([r[0] for r in rows if r[0]])

    return departments
try:
    from utils.shift_config import get_all_shift_keys
except Exception:
    def get_all_shift_keys() -> List[str]:
        return []

def _safe_int(v, default=None) -> Optional[int]:
    if v is None:
        return default
    try:
        return int(Decimal(str(v)))
    except (InvalidOperation, ValueError, TypeError):
        return default


def _normalize_months(months: List[int]) -> List[int]:
    out = []
    for m in months or []:
        mi = _safe_int(m, None)
        if mi is not None and 1 <= mi <= 12:
            out.append(mi)

    seen, res = set(), []
    for m in out:
        if m not in seen:
            seen.add(m)
            res.append(m)
    return res


def _normalize_years(years: List[int]) -> List[int]:
    out = []
    for y in years or []:
        yi = _safe_int(y, None)
        if yi is not None:
            out.append(yi)

    seen, res = set(), []
    for y in out:
        if y not in seen:
            seen.add(y)
            res.append(y)
    return res


def _year_month_tuple(d) -> Tuple[int, int]:
    return (d.year, d.month)


def _discover_available_pairs(db: Session, base_query) -> List[Tuple[int, int]]:
    """
    Optional helper if you want to inspect what (year, month) pairs exist.
    Currently unused by core flow.
    """
    pq = base_query.with_entities(ShiftAllowances.duration_month).distinct()
    rows = pq.all()
    return sorted(
        {_year_month_tuple(r[0]) for r in rows if r[0] is not None},
        key=lambda t: (t[0], t[1])
    )


def _group_selected_periods_from_map(months_by_year: Dict[int, List[int]]) -> List[Dict[str, Any]]:
    """Builds a summary structure for the response."""
    return [
        {"year": y, "months": sorted(set(ms))}
        for y, ms in sorted(months_by_year.items())
    ]

def _parse_headcount_filter(filter_value: Union[str, List[str]]):
    """
    Parses headcount filters into normalized rules:
      - '5'     -> ('eq', 5)
      - '1-10'  -> ('range', 1, 10)
      - '10+'   -> ('range', 10, INF)   # Note: get_client_dashboard validation forbids '+' per requirements
    Returns:
      - list of rules OR None to indicate no filtering
    """
    if filter_value in (None, "ALL", [0]):
        return None

    rules = []
    values = filter_value if isinstance(filter_value, list) else [filter_value]

    for item in values:
        s = str(item).strip()

        if s.endswith("+") and s[:-1].isdigit():
            rules.append(("range", int(s[:-1]), 10**9))
        elif "-" in s:
            a, b = s.split("-", 1)
            if a.strip().isdigit() and b.strip().isdigit():
                start, end = int(a), int(b)
                if start <= end:
                    rules.append(("range", start, end))
        elif s.isdigit():
            rules.append(("eq", int(s)))

    return rules or None


def _headcount_matches(value: Optional[int], rules) -> bool:
    if rules is None:
        return True
    if value is None:
        return False

    for rule in rules:
        if rule[0] == "range":
            _, start, end = rule
            if start <= value <= end:
                return True
        elif rule[0] == "eq":
            _, expected = rule
            if value == expected:
                return True
    return False


def _resolve_periods_and_messages(db: Session, base_query, filters, today: date):
    """
    Resolution rules:
      - If years & months both empty:
          1) Try current (Y, M)
          2) Else latest in last 12 months
          3) Else latest in whole DB
          4) Else default (current Y, M) + message
      - If only months specified: assume current year
      - If only years specified: use all 1..12 months
      - Exclude future (Y, M) pairs

    Returns:
      final_years: List[int]
      months_by_year: Dict[int, List[int]]
      messages: List[str]
    """
    messages: List[str] = []
    current_year, current_month = today.year, today.month

    raw_years = filters.years or []
    raw_months = filters.months or []

    if raw_years == [0]:
        raw_years = []
    if raw_months == [0]:
        raw_months = []

    years = _normalize_years(raw_years)
    months = _normalize_months(raw_months)

    def pair_exists(y: int, m: int) -> bool:
        return base_query.filter(
            func.extract("year", ShiftAllowances.duration_month) == y,
            func.extract("month", ShiftAllowances.duration_month) == m
        ).first() is not None

    if not years and not months:
     
        if pair_exists(current_year, current_month):
            years = [current_year]
            months = [current_month]
        else:
            
            cutoff = today.replace(day=1) - relativedelta(months=12)
            latest_row_12 = (
                base_query
                .filter(ShiftAllowances.duration_month >= cutoff)
                .order_by(ShiftAllowances.duration_month.desc())
                .first()
            )
            if latest_row_12 and latest_row_12.duration_month:
                latest_date = latest_row_12.duration_month
                years = [latest_date.year]
                months = [latest_date.month]
                messages.append(
                    f"No data for current month. "
                    f"Showing latest available month in last 12 months: "
                    f"{latest_date.month}-{latest_date.year}"
                )
            else:
                
                latest_row_any = (
                    base_query
                    .order_by(ShiftAllowances.duration_month.desc())
                    .first()
                )
                if latest_row_any and latest_row_any.duration_month:
                    latest_date = latest_row_any.duration_month
                    years = [latest_date.year]
                    months = [latest_date.month]
                    messages.append(
                        f"No data in last 12 months. "
                        f"Showing latest available month in the database: "
                        f"{latest_date.month}-{latest_date.year}"
                    )
                else:
                  
                    years = [current_year]
                    months = [current_month]
                    messages.append("No data in the database. Defaulting to current month.")

    elif months and not years:
      
        years = [current_year]

    elif years and not months:
       
        months = list(range(1, 12 + 1))


    pairs: List[Tuple[int, int]] = []
    for y in years:
        for m in months:
            if (y > current_year) or (y == current_year and m > current_month):
                continue
            pairs.append((y, m))

   
    if not pairs:
        pairs = [(y, m) for y in years for m in months]

   
    pairs = sorted(set(pairs), key=lambda t: (t[0], t[1]))

    months_by_year: Dict[int, List[int]] = {}
    for y, m in pairs:
        months_by_year.setdefault(y, []).append(m)

    final_years = sorted(months_by_year.keys())

    return final_years, months_by_year, messages


def _extract_employee_id(row: ShiftAllowances) -> Optional[str]:
    emp = getattr(row, "emp_id", None)
    if emp:
        return str(emp).strip()

    name = str(getattr(row, "emp_name", "") or "").strip()
    dept = str(getattr(row, "department", "") or "").strip()
    client = str(getattr(row, "client", "") or "").strip()

    if name or dept or client:
        return f"{name}|{dept}|{client}"

    return None



def get_client_dashboard(db: Session, filters) -> Dict[str, Any]:
    """
    Builds a client dashboard with departments count, headcount, and total allowance,
    following the corrected period resolution logic and filters.
    """
    today = date.today()

    
    if filters.years and filters.years != [0]:
        for y in filters.years:
            yi = _safe_int(y)
            if yi is None or yi < 2000 or yi > today.year + 5:
                raise HTTPException(400, f"Invalid year: {y}")

    if filters.months and filters.months != [0]:
        for m in filters.months:
            mi = _safe_int(m)
            if mi is None or mi < 1 or mi > 12:
                raise HTTPException(400, f"Invalid month: {m}")

   
    VALID_SHIFTS = set(get_all_shift_keys())
    if filters.shifts != "ALL":
        shifts_to_check = (
            filters.shifts if isinstance(filters.shifts, list)
            else [filters.shifts]
        )
        for s in shifts_to_check:
            shift_key = str(s).upper().strip()
            if VALID_SHIFTS and shift_key not in VALID_SHIFTS:
                raise HTTPException(400, f"Invalid shift type: {s}")

    
    if filters.headcounts not in (None, "ALL", [0]):
        values = filters.headcounts if isinstance(filters.headcounts, list) else [filters.headcounts]
        for item in values:
            s = str(item).strip()

            if "+" in s:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid headcount format: {item}. Use format like 1-10."
                )

            if "-" in s:
                parts = s.split("-")
                if (
                    len(parts) != 2
                    or not parts[0].isdigit()
                    or not parts[1].isdigit()
                ):
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid headcount range: {item}. Use format like 1-10."
                    )

                if int(parts[0]) > int(parts[1]):
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid headcount range (start > end): {item}"
                    )

            elif not s.isdigit():
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid headcount format: {item}. Use 5 or 1-10."
                )

    if filters.top is None or str(filters.top).lower() == "all":
        top_int = None
    else:
        if not str(filters.top).isdigit():
            raise HTTPException(400, "top must be integer or ALL")
        top_int = int(filters.top)
        if top_int <= 0:
            raise HTTPException(400, "top must be > 0")

    rate_rows = db.query(ShiftsAmount).all()
    rates = {str(r.shift_type).upper(): Decimal(r.amount) for r in rate_rows}

    
    q = db.query(ShiftAllowances)

   
    if getattr(filters, "client_starts_with", None):
        prefix = filters.client_starts_with.strip().lower()
        q = q.filter(func.lower(func.trim(ShiftAllowances.client)).like(f"{prefix}%"))
    elif getattr(filters, "clients", "ALL") != "ALL":
        if isinstance(filters.clients, list):
            norm = [c.strip().lower() for c in filters.clients]
            q = q.filter(func.lower(func.trim(ShiftAllowances.client)).in_(norm))
        else:
            norm = filters.clients.strip().lower()
            q = q.filter(func.lower(func.trim(ShiftAllowances.client)) == norm)

    years, months_by_year, messages = _resolve_periods_and_messages(db, q, filters, today)

    pair_clauses = []
    for y, ms in months_by_year.items():
        pair_clauses.append(and_(
            func.extract("year", ShiftAllowances.duration_month) == y,
            func.extract("month", ShiftAllowances.duration_month).in_(ms)
        ))

    if pair_clauses:
        q = q.filter(or_(*pair_clauses))

    rows = q.all()

    
    hc_rules = _parse_headcount_filter(filters.headcounts)

    selected_shifts: Optional[Set[str]] = None
    if filters.shifts != "ALL":
        if isinstance(filters.shifts, list):
            selected_shifts = {str(s).upper() for s in filters.shifts}
        else:
            selected_shifts = {str(filters.shifts).upper()}

    employees_by_client: Dict[str, Set[str]] = {}
    departments_by_client: Dict[str, Set[str]] = {}
    allowances_by_client: Dict[str, Decimal] = {}

    for row in rows:
        client = row.client or "Unknown"
        emp = _extract_employee_id(row)

        employees_by_client.setdefault(client, set())
        departments_by_client.setdefault(client, set())
        allowances_by_client.setdefault(client, Decimal(0))

        if emp:
            employees_by_client[client].add(emp)

        if getattr(row, "department", None):
            departments_by_client[client].add(row.department)

       
        for mapping in getattr(row, "shift_mappings", []):
            shift_key = str(mapping.shift_type).upper()
            if selected_shifts and shift_key not in selected_shifts:
                continue
            if VALID_SHIFTS and shift_key not in VALID_SHIFTS:
                continue

            days = Decimal(mapping.days or 0)
            rate = rates.get(shift_key, Decimal(0))
            allowances_by_client[client] += days * rate

    
    items = []
    for client, total in allowances_by_client.items():
        hc = len(employees_by_client.get(client, []))
        if not _headcount_matches(hc, hc_rules):
            continue

        items.append({
            "client": client,
            "departments": len(departments_by_client.get(client, [])),
            "headcount": hc,
            "total_allowance": float(total),
        })

    items.sort(key=lambda x: x["total_allowance"], reverse=True)

    if top_int:
        items = items[:top_int]

    dashboard = OrderedDict()
    for it in items:
        dashboard[it["client"]] = {
            "departments": it["departments"],
            "headcount": it["headcount"],
            "total_allowance": it["total_allowance"],
        }

    if not dashboard and not messages:
        messages.append("No data found for selected filters.")

    
    selected_periods = _group_selected_periods_from_map(months_by_year)

    return {
        "summary": {"selected_periods": selected_periods},
        "messages": messages,
        "dashboard": dashboard
    }