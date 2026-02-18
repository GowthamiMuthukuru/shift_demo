"""
Client summary service with multi-year, multi-month, shift, and headcount support.

Features:
- Filters by years, months, clients, departments, employees, client partners, shifts, headcount ranges.
- Headcount range applied at CLIENT level (unique employees across its departments).
- Validations for years, months, shifts, and headcount formats.
- Caching for latest-month requests and month resolution with filter-aware keys.

Behavior:
- Sorting: user-selected years/months are converted to int, deduped, and sorted ascending.
- If some of the explicitly selected periods have no data -> no error; response.meta.missing_periods is populated.
- If none of the explicitly selected periods have data -> no error; return empty "periods" + message in meta.
- If default/latest mode and no data -> no error; return empty "periods" + message in meta.

Latest-month resolution (default):
- If no years/months are explicitly provided, return ONLY the single latest month that has data in the DB
  (with the applied filters), without restricting to any "last 12 months" window.
"""

from __future__ import annotations
from datetime import date
from typing import List, Dict, Optional, Any, Tuple
from sqlalchemy import literal
from dateutil.relativedelta import relativedelta  # kept if you later add time windows
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, cast, Integer, extract

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from diskcache import Cache
from utils.shift_config import get_all_shift_keys

cache = Cache("./diskcache/latest_month")

# 🔄 Bump to invalidate caches generated under old latest-month behavior and zero-shift bug
CLIENT_SUMMARY_VERSION = "v4"
CACHE_TTL = 24 * 60 * 60  # 24 hours


def empty_shift_totals(shift_keys: List[str]) -> Dict[str, float]:
    """Zero-initialized shift totals keyed by configured shift keys."""
    return {k: 0.0 for k in shift_keys}


def is_default_latest_month_request(payload: dict) -> bool:
    """
    'Default' latest-month summary request: no explicit years/months, clients==ALL,
    and no emp_id or client_partner.
    """
    return (
        not payload
        or (
            (payload.get("clients") in (None, "ALL"))
            and not payload.get("years")
            and not payload.get("months")
            and not payload.get("emp_id")
            and not payload.get("client_partner")
        )
    )


def validate_year(year: int) -> None:
    """Validate year is not in the future or invalid."""
    current_year = date.today().year
    if year <= 0:
        raise HTTPException(400, "Year must be greater than 0")
    if year > current_year:
        raise HTTPException(400, "Year cannot be in the future")


def validate_months(months: List[int]) -> None:
    """Validate month integers."""
    for m in months:
        if not 1 <= int(m) <= 12:
            raise HTTPException(400, f"Invalid month: {m}")


def parse_headcount_ranges(headcounts_payload):
    """
    Parses headcount ranges.

    Returns:
      - None  -> ALL (no filtering)
      - List[(start, end)]
    """
    if headcounts_payload == "ALL":
        return None

    if isinstance(headcounts_payload, str):
        headcounts_payload = [headcounts_payload]

    ranges: List[Tuple[int, int]] = []
    for h in headcounts_payload:
        if "-" in h:
            parts = h.split("-")
            if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid headcount range format: {h}. Use '1-5'"
                )
            start = int(parts[0])
            end = int(parts[1])
            if start > end:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid headcount range: {h}"
                )
        elif str(h).isdigit():
            start = end = int(h)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid headcount range format: {h}"
            )
        ranges.append((start, end))
    return ranges


def clean_str(value: Any) -> str:
    """Normalize strings from DB and inputs."""
    if value is None:
        return ""
    s = value.strip() if isinstance(value, str) else str(value).strip()
    s = s.replace("\u200b", "").replace("\u00a0", "").strip()
    for _ in range(2):
        if len(s) >= 2 and ((s[0] == s[-1]) and s[0] in ("'", '"')):
            s = s[1:-1].strip()
    if s in ("'", "''", '"', '""'):
        return ""
    if s.upper() in ("NULL", "NONE", "NAN"):
        return ""
    return s


def get_shift_keys() -> List[str]:
    """Get configured shift keys (uppercase)."""
    return [clean_str(k).upper() for k in get_all_shift_keys()]


def build_base_query(db: Session):
    """Base SQLAlchemy query for allowances joined to mapping and shift amounts."""
    return (
        db.query(
            ShiftAllowances.duration_month,   # date
            ShiftAllowances.client,           # str
            ShiftAllowances.department,       # str
            ShiftAllowances.emp_id,           # str
            ShiftAllowances.emp_name,         # str
            ShiftAllowances.client_partner,   # str
            ShiftMapping.shift_type,          # str (e.g., PST_MST / US_INDIA / SG / ANZ)
            ShiftMapping.days,                # int
            ShiftsAmount.amount,              # numeric (rate per day/year)
        )
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .outerjoin(
            ShiftsAmount,
            and_(
                ShiftMapping.shift_type == ShiftsAmount.shift_type,
                cast(ShiftsAmount.payroll_year, Integer)
                == extract("year", ShiftAllowances.duration_month),
            ),
        )
    )


def _requested_periods_from_payload(payload: dict) -> List[str]:
    """
    Build a list of requested 'YYYY-MM' periods from the payload, sorted ascending.
    - Both years & months -> Cartesian product.
    - Years only -> all 12 months for those years.
    - Months only -> current year.
    - Neither -> empty list (default/latest mode).
    """
    years_raw = payload.get("years") or []
    months_raw = payload.get("months") or []
    years = sorted({int(y) for y in years_raw}) if years_raw else []
    months = sorted({int(m) for m in months_raw}) if months_raw else []

    periods: List[str] = []
    if years and months:
        for y in years:
            for m in months:
                periods.append(f"{int(y):04d}-{int(m):02d}")
    elif years:
        for y in years:
            for m in range(1, 13):
                periods.append(f"{int(y):04d}-{m:02d}")
    elif months:
        current_year = date.today().year
        for m in months:
            periods.append(f"{int(current_year):04d}-{int(m):02d}")
    return periods


def latest_month_cache_key(payload: dict) -> str:
    """
    Filter-aware cache key for latest-month resolution and response caching.
    """
    parts = {
        "clients": payload.get("clients", "ALL"),
        "departments": payload.get("departments", "ALL"),
        "emp_id": payload.get("emp_id"),
        "client_partner": payload.get("client_partner"),
        "shifts": payload.get("shifts", "ALL"),
        "headcounts": payload.get("headcounts", "ALL"),  # include headcount for cache correctness
    }
    return f"client_summary_latest:{CLIENT_SUMMARY_VERSION}:{str(parts)}"


def resolve_target_months(
    db: Session,
    payload: dict,
    clients_list: List[str],
    departments_list: List[str],
    emp_id: Optional[Any],
    client_partner: Optional[Any],
    allowed_shifts: Optional[set],
) -> List[date]:
    """
    Determine months to use:
    - If explicit years/months are provided -> expand to their Cartesian selection (sorted, deduped).
    - Else -> return ONLY the latest month in the database (with applied filters), regardless of how far back it is.
    """
    selected_years = payload.get("years", [])
    selected_months = payload.get("months", [])

    # 1) Explicit periods
    if selected_years or selected_months:
        periods: List[date] = []
        years = sorted({int(y) for y in selected_years}) if selected_years else []
        months = sorted({int(m) for m in selected_months}) if selected_months else []
        if years and months:
            for y in years:
                for m in months:
                    periods.append(date(y, m, 1))
        elif years:
            for y in years:
                for m in range(1, 13):
                    periods.append(date(y, m, 1))
        elif months:
            current_year = date.today().year
            for m in months:
                periods.append(date(current_year, m, 1))
        return periods

    # 2) Latest month fallback (no time window restriction)
    q = db.query(ShiftAllowances.duration_month.distinct())

    # Apply filters for correctness of "latest with data"
    if allowed_shifts:
        q = q.join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        q = q.filter(func.upper(ShiftMapping.shift_type).in_(list(allowed_shifts)))

    if clients_list:
        q = q.filter(func.lower(ShiftAllowances.client).in_([c.lower() for c in clients_list]))
    if departments_list:
        q = q.filter(func.lower(ShiftAllowances.department).in_([d.lower() for d in departments_list]))
    if emp_id:
        ids = emp_id if isinstance(emp_id, list) else [emp_id]
        q = q.filter(func.lower(ShiftAllowances.emp_id).in_([clean_str(e).lower() for e in ids]))
    if client_partner:
        col = ShiftAllowances.client_partner
        parts = [clean_str(client_partner)] if isinstance(client_partner, str) else [clean_str(p) for p in client_partner]
        like_parts = [p for p in parts if p]
        if like_parts:
            q = q.filter(or_(*[func.lower(col).like(f"%{p.lower()}%") for p in like_parts]))

    latest_dm = q.order_by(ShiftAllowances.duration_month.desc()).first()
    if latest_dm and latest_dm[0]:
        return [latest_dm[0].replace(day=1)]

    return []


def client_summary_service(db: Session, payload: dict):
    payload = payload or {}
    shift_keys = get_shift_keys()
    shift_key_set = set(shift_keys)

    clients_raw = payload.get("clients", "ALL")
    if isinstance(clients_raw, str):
        clients_list = [] if clients_raw == "ALL" else [c.strip() for c in clients_raw.split(",") if c.strip()]
    elif isinstance(clients_raw, list):
        clients_list = [c.strip() for c in clients_raw if c]
    else:
        clients_list = []

    departments_raw = payload.get("departments", "ALL")
    if isinstance(departments_raw, str):
        departments_list = [] if departments_raw == "ALL" else [departments_raw.strip()]
    elif isinstance(departments_raw, list):
        departments_list = [d.strip() for d in departments_raw if d]
    else:
        departments_list = []

    emp_id = payload.get("emp_id")
    client_partner = payload.get("client_partner")

    shifts = payload.get("shifts", "ALL")
    allowed_shifts_for_filter: set = set()
    if shifts != "ALL":
        sel = [shifts] if isinstance(shifts, str) else list(shifts)
        shifts_upper = [clean_str(s).upper() for s in sel]
        invalid_shifts = [s for s in shifts_upper if s not in shift_key_set]
        if invalid_shifts:
            raise HTTPException(400, f"Invalid shift(s): {invalid_shifts}")
        allowed_shifts_for_filter = set(shifts_upper)
    else:
        allowed_shifts_for_filter = set(shift_keys)

    headcount_ranges = parse_headcount_ranges(payload.get("headcounts", "ALL"))

    months_to_use: List[date] = resolve_target_months(
        db=db,
        payload=payload,
        clients_list=clients_list,
        departments_list=departments_list,
        emp_id=emp_id,
        client_partner=client_partner,
        allowed_shifts=allowed_shifts_for_filter,
    )

    if not months_to_use:
        return {
            "periods": {},
            "meta": {"message": "No data found for the selected filters."}
        }

    latest_style_request = not payload.get("years") and not payload.get("months")
    response_cache_key = None
    if latest_style_request:
        parts = {
            "clients": payload.get("clients", "ALL"),
            "departments": payload.get("departments", "ALL"),
            "emp_id": payload.get("emp_id"),
            "client_partner": payload.get("client_partner"),
            "shifts": payload.get("shifts", "ALL"),
            "headcounts": payload.get("headcounts", "ALL"),
            "sort_by": payload.get("sort_by", "client_total"),       
            "sort_order": payload.get("sort_order", "desc"),  
        }
        response_cache_key = f"client_summary_latest:{CLIENT_SUMMARY_VERSION}:{str(parts)}:response"
        cached_resp = cache.get(response_cache_key)
        if cached_resp:
            return cached_resp

    query = build_base_query(db)

    filter_clauses = []
    if clients_list:
        filter_clauses.append(func.lower(ShiftAllowances.client).in_([c.lower() for c in clients_list]))
    if departments_list:
        filter_clauses.append(func.lower(ShiftAllowances.department).in_([d.lower() for d in departments_list]))
    if emp_id:
        ids = emp_id if isinstance(emp_id, list) else [emp_id]
        filter_clauses.append(func.lower(ShiftAllowances.emp_id).in_([clean_str(e).lower() for e in ids]))
    if client_partner:
        cp_col = ShiftAllowances.client_partner
        parts = [clean_str(client_partner)] if isinstance(client_partner, str) else [clean_str(p) for p in client_partner]
        like_parts = [p for p in parts if p]
        if like_parts:
            filter_clauses.append(or_(*[func.lower(cp_col).like(f"%{p.lower()}%") for p in like_parts]))
    if allowed_shifts_for_filter:
        filter_clauses.append(func.upper(ShiftMapping.shift_type).in_(list(allowed_shifts_for_filter)))
    if filter_clauses:
        query = query.filter(and_(*filter_clauses))

    month_numbers = [(d.year, d.month) for d in months_to_use]
    month_filters = [
        and_(
            extract("year", ShiftAllowances.duration_month) == y,
            extract("month", ShiftAllowances.duration_month) == m
        )
        for y, m in month_numbers
    ]
    query = query.filter(or_(*month_filters))

    rows = query.all()

    aggregated: Dict[str, Any] = {}

    for row in rows:
        month_str = row.duration_month.strftime("%Y-%m")

        if month_str not in aggregated:
            month_total_template = empty_shift_totals(shift_keys)
            month_total_template.update({
                "total_head_count": 0,
                "total_allowance": 0.0
            })
            aggregated[month_str] = {
                "clients": {},
                "month_total": month_total_template
            }

        month_data = aggregated[month_str]
        client_name = clean_str(row.client)
        dept_name = clean_str(row.department)
        shift_key = clean_str(row.shift_type).upper()

        if shift_key not in shift_key_set:
            continue

        if client_name not in month_data["clients"]:
            client_template = {
                "client_name": client_name,
                "departments": {},
                "client_head_count": 0,
                "client_total": 0.0,
                "client_partner": clean_str(row.client_partner),
            }
            client_template.update(empty_shift_totals(shift_keys))
            month_data["clients"][client_name] = client_template

        client_data = month_data["clients"][client_name]

        if dept_name not in client_data["departments"]:
            dept_template = {
                "dept_head_count": 0,
                "dept_total": 0.0,
                "employees": [],
                "_emp_map": {},
            }
            dept_template.update(empty_shift_totals(shift_keys))
            client_data["departments"][dept_name] = dept_template

        dept_data = client_data["departments"][dept_name]

        try:
            days_val = float(row.days or 0.0)
        except Exception:
            days_val = 0.0
        try:
            rate_val = float(row.amount or 0.0)
        except Exception:
            rate_val = 0.0

        value = days_val * rate_val

        emp_map = dept_data["_emp_map"]
        eid = clean_str(row.emp_id)
        if eid not in emp_map:
            emp_entry = {
                "emp_id": eid,
                "emp_name": clean_str(row.emp_name),
                "client_partner": clean_str(row.client_partner),
                "total": 0.0,
            }
            emp_entry.update(empty_shift_totals(shift_keys))
            emp_map[eid] = emp_entry

        emp_entry = emp_map[eid]

        emp_entry[shift_key] += value
        emp_entry["total"] += value

        dept_data[shift_key] += value
        dept_data["dept_total"] += value

    def range_matches(hc: int, ranges: Optional[List[Tuple[int, int]]]) -> bool:
        if not ranges:
            return True
        return any(start <= hc <= end for start, end in ranges)

    for month_str, month_data in aggregated.items():

        for client_name, client_data in month_data["clients"].items():
            finalized_departments: Dict[str, Any] = {}
            for dept_name, dept_data in client_data["departments"].items():
                emp_map = dept_data.pop("_emp_map", {})
                employees = list(emp_map.values())
                dept_data["employees"] = employees
                dept_data["dept_head_count"] = len(employees)
                finalized_departments[dept_name] = dept_data

            client_data["departments"] = finalized_departments

        filtered_clients: Dict[str, Any] = {}
        for client_name, client_data in month_data["clients"].items():
            depts = client_data["departments"]

            client_total = sum(d["dept_total"] for d in depts.values())
            per_shift_totals = {k: sum(d[k] for d in depts.values()) for k in shift_keys}

            unique_emp_ids = set()
            for d in depts.values():
                for e in d.get("employees", []):
                    eid = e.get("emp_id")
                    if eid:
                        unique_emp_ids.add(eid)
            client_headcount = len(unique_emp_ids)

            if not range_matches(client_headcount, headcount_ranges):
                continue

            client_data["client_total"] = client_total
            client_data["client_head_count"] = client_headcount
            for k in shift_keys:
                client_data[k] = per_shift_totals[k]

            filtered_clients[client_name] = client_data

        month_data["clients"] = filtered_clients

      
        sort_by = payload.get("sort_by", "client_total")
        sort_order = str(payload.get("sort_order", "desc")).lower()
        reverse = sort_order != "asc"

        if sort_by == "total_allowance":
            actual_sort_field = "client_total"
        elif sort_by == "head_count":
            actual_sort_field = "client_head_count"
        elif sort_by == "client_name":
            actual_sort_field = "client_name"
        else:
            actual_sort_field = sort_by

        clients_items = list(month_data["clients"].items())

        if actual_sort_field == "client_name":
            clients_items.sort(
                key=lambda x: (x[1].get("client_name") or "").lower(),
                reverse=reverse
            )
        else:
            clients_items.sort(
                key=lambda x: x[1].get(actual_sort_field, 0),
                reverse=reverse
            )

        month_data["clients"] = dict(clients_items)

        month_total = month_data["month_total"]
        for k in shift_keys:
            month_total[k] = sum(c[k] for c in month_data["clients"].values())
        month_total["total_head_count"] = sum(c["client_head_count"] for c in month_data["clients"].values())
        month_total["total_allowance"] = sum(c["client_total"] for c in month_data["clients"].values())

    if latest_style_request and response_cache_key:
        cache.set(response_cache_key, aggregated, expire=CACHE_TTL)

    return aggregated
