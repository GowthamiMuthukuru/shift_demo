"""
Client summary service with multi-year, multi-month, shift, and headcount support.

Features:
- Filters by years, months, clients, departments, employees, client partners, shifts, headcount ranges.
- Headcount range applied at dept level if departments selected, else at client level.
- Validations for years, months, shifts, and headcount formats.
- Caching for latest-month requests and month resolution with filter-aware keys.

Behavior:
- Sorting: user-selected years/months are converted to int, deduped, and sorted ascending.
- If some of the explicitly selected periods have no data -> no error; response.meta.missing_periods is populated.
- If none of the explicitly selected periods have data -> no error; return empty "periods" + message in meta.
- If default/latest mode and no data -> no error; return empty "periods" + message in meta.
"""

from __future__ import annotations
from datetime import date
from typing import List, Dict, Optional, Any, Tuple

from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, cast, Integer, extract

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from diskcache import Cache
from utils.shift_config import get_all_shift_keys

cache = Cache("./diskcache/latest_month")

CLIENT_SUMMARY_VERSION = "v3"
CACHE_TTL = 24 * 60 * 60  # 24 hours


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


def empty_shift_totals(shift_keys: List[str]) -> Dict[str, float]:
    """Zero-initialized shift totals."""
    return {k: 0.0 for k in shift_keys}


def is_default_latest_month_request(payload: dict) -> bool:
    """
    Check if this is a 'default' latest-month summary request.
    This means: no explicit years/months, no emp_id, no client_partner, and clients == ALL.
    """
    return (
        not payload
        or (
            payload.get("clients") in (None, "ALL")
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


def build_base_query(db: Session):
    """Base SQLAlchemy query."""
    return (
        db.query(
            ShiftAllowances.duration_month,
            ShiftAllowances.client,
            ShiftAllowances.department,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.client_partner,
            ShiftMapping.shift_type,
            ShiftMapping.days,
            ShiftsAmount.amount,
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



def prev_month(d: date) -> date:
    """Return the previous month anchor (day=1)."""
    if d.month == 1:
        return date(d.year - 1, 12, 1)
    return date(d.year, d.month - 1, 1)


def has_data_for_month(
    db: Session,
    month_anchor: date,
    clients_list: List[str],
    departments_list: List[str],
    emp_id: Optional[List[str]],
    client_partner: Optional[Any],
    allowed_shifts: Optional[set],
) -> bool:
    """
    Lightweight check: is there any data for this month with the given filters?
    """
    q = (
        db.query(func.count(ShiftAllowances.id))
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .filter(
            extract("year", ShiftAllowances.duration_month) == month_anchor.year,
            extract("month", ShiftAllowances.duration_month) == month_anchor.month,
        )
    )

    # Clients + Departments filter (same logic as main query)
    if clients_list:
        client_filters = []
        if departments_list:
            depts_lower = [d.lower() for d in departments_list]
            for c in clients_list:
                client_filters.append(
                    and_(
                        func.lower(ShiftAllowances.client) == c.lower(),
                        func.lower(ShiftAllowances.department).in_(depts_lower),
                    )
                )
        else:
            for c in clients_list:
                client_filters.append(func.lower(ShiftAllowances.client) == c.lower())
        if client_filters:
            q = q.filter(or_(*client_filters))

    # emp_id filter
    if emp_id:
        ids = emp_id if isinstance(emp_id, list) else [emp_id]
        q = q.filter(func.lower(ShiftAllowances.emp_id).in_([clean_str(e).lower() for e in ids]))

    # client_partner filter
    if client_partner:
        col = ShiftAllowances.client_partner
        if isinstance(client_partner, list):
            parts = [clean_str(cp) for cp in client_partner if clean_str(cp)]
            if parts:
                q = q.filter(or_(*[func.lower(col).like(f"%{p.lower()}%") for p in parts]))
        else:
            q = q.filter(func.lower(col).like(f"%{clean_str(client_partner).lower()}%"))

    # allowed shifts filter
    if allowed_shifts and len(allowed_shifts) > 0:
        q = q.filter(func.upper(ShiftMapping.shift_type).in_(list(allowed_shifts)))

    count_val = q.scalar() or 0
    return count_val > 0


def latest_month_cache_key(payload: dict) -> str:
    """
    Builds a filter-aware cache key for latest-month resolution and response caching.
    Only includes filters that affect data presence.
    """
    parts = {
        "clients": payload.get("clients", "ALL"),
        "departments": payload.get("departments", "ALL"),
        "emp_id": payload.get("emp_id"),
        "client_partner": payload.get("client_partner"),
        "shifts": payload.get("shifts", "ALL"),
    }
    return f"client_summary_latest:{CLIENT_SUMMARY_VERSION}:{str(parts)}"


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


def resolve_target_months_with_fallback(
    db: Session,
    payload: dict,
    clients_list: List[str],
    departments_list: List[str],
    emp_id: Optional[Any],
    client_partner: Optional[Any],
    allowed_shifts: Optional[set],
    max_lookback_months: int = 12,
) -> List[date]:
    """
    Month determination logic:

    - Years & Months both -> Cartesian product (ascending).
    - Years only -> all 12 months in those years (ascending).
    - Months only -> assume current year (ascending).
    - Neither -> try current month; if no data, walk backward up to `max_lookback_months`;
      else fallback to absolute latest month in DB; else current month if DB empty.
    """
    selected_years_raw = payload.get("years", [])
    selected_months_raw = payload.get("months", [])

    # SORTING ONLY: convert to int, remove duplicates, sort ascending
    selected_years = sorted({int(y) for y in selected_years_raw}) if selected_years_raw else []
    selected_months = sorted({int(m) for m in selected_months_raw}) if selected_months_raw else []

    # Validate
    if selected_years:
        for y in selected_years:
            validate_year(int(y))
    if selected_months:
        validate_months(selected_months)

    # 1) Both provided -> CARTESIAN PRODUCT (ascending)
    if selected_years and selected_months:
        return [date(int(y), int(m), 1) for y in selected_years for m in selected_months]

    # 2) Only years provided -> all months of those years (ascending)
    if selected_years and not selected_months:
        return [date(int(y), m, 1) for y in selected_years for m in range(1, 13)]

    # 3) Only months provided -> current year (ascending)
    if selected_months and not selected_years:
        current_year = date.today().year
        return [date(current_year, int(m), 1) for m in selected_months]

    # 4) Neither provided -> resolve latest with fallback and cache (filter-aware key)
    lm_key = latest_month_cache_key(payload)
    cached_month = cache.get(lm_key)
    if cached_month:
        return [cached_month]

    current = date.today().replace(day=1)

    if has_data_for_month(
        db, current, clients_list, departments_list, emp_id, client_partner, allowed_shifts
    ):
        cache.set(lm_key, current, expire=CACHE_TTL)
        return [current]

    # Walk back up to max_lookback_months
    probe = current
    for _ in range(max_lookback_months):
        probe = prev_month(probe)
        if has_data_for_month(
            db, probe, clients_list, departments_list, emp_id, client_partner, allowed_shifts
        ):
            cache.set(lm_key, probe, expire=CACHE_TTL)
            return [probe]

    # Final fallback: absolute latest month in DB (no filters)
    latest_dm = db.query(func.max(ShiftAllowances.duration_month)).scalar()
    if latest_dm:
        month1 = latest_dm.replace(day=1)
        cache.set(lm_key, month1, expire=CACHE_TTL)
        return [month1]

    # DB empty -> return current month
    cache.set(lm_key, current, expire=CACHE_TTL)
    return [current]


def client_summary_service(db: Session, payload: dict):
    """
    Return client summary with multi-year, multi-month, shift, and headcount filters with sorting.
    For explicit month/year selections:
      - Does NOT throw when some or all selected periods are empty.
      - Returns "meta" with requested/present/missing periods and a friendly message when applicable.
    """

    payload = payload or {}
    shift_keys = get_shift_keys()
    shift_key_set = set(shift_keys)

    clients_raw = payload.get("clients", "ALL")
    departments_raw = payload.get("departments", "ALL")
    emp_id = payload.get("emp_id")
    client_partner = payload.get("client_partner")

    # SORTING ONLY: read, parse, dedupe, sort ascending
    selected_years_raw = payload.get("years", [])
    selected_months_raw = payload.get("months", [])
    selected_years = sorted({int(y) for y in selected_years_raw}) if selected_years_raw else []
    selected_months = sorted({int(m) for m in selected_months_raw}) if selected_months_raw else []

    shifts = payload.get("shifts", "ALL")
    headcounts_payload = payload.get("headcounts", "ALL")
    sort_by = payload.get("sort_by", "total_allowance")
    sort_order = payload.get("sort_order", "default")  # 'asc' | 'desc' | 'default'

    # Normalize clients
    if isinstance(clients_raw, str):
        if clients_raw == "ALL":
            clients_list: List[str] = []
        else:
            clients_list = [c.strip() for c in clients_raw.split(",") if c.strip()]
    elif isinstance(clients_raw, list):
        clients_list = [c.strip() for c in clients_raw if c]
    else:
        clients_list = []

    # Normalize departments
    if isinstance(departments_raw, str):
        departments_list = [departments_raw.strip()] if departments_raw != "ALL" else []
    elif isinstance(departments_raw, list):
        departments_list = [d.strip() for d in departments_raw if d]
    else:
        departments_list = []

    departments_selected = bool(departments_list)

    # Validate years/months (defensive)
    if selected_years:
        for y in selected_years:
            validate_year(int(y))
    if selected_months:
        validate_months(selected_months)

    # Normalize shifts; build allowed set for filtering
    allowed_shifts_for_filter: set = set()
    if shifts != "ALL":
        sel = [shifts] if isinstance(shifts, str) else list(shifts)
        shifts_upper = [clean_str(s).upper() for s in sel]
        invalid_shifts = [s for s in shifts_upper if s not in shift_key_set]
        if invalid_shifts:
            raise HTTPException(400, f"Invalid shift(s): {invalid_shifts}")
        allowed_shifts_for_filter = set(shifts_upper)
        # Limit key set to selected shifts
        shift_key_set = allowed_shifts_for_filter.copy()
    else:
        # All shifts allowed
        allowed_shifts_for_filter = set(shift_keys)

    # Parse headcount ranges
    headcount_ranges = parse_headcount_ranges(headcounts_payload)

    # Compute requested periods (for meta & partial/no-data messages)
    requested_periods = _requested_periods_from_payload(payload)

    months_to_use: List[date] = resolve_target_months_with_fallback(
        db=db,
        payload=payload,
        clients_list=clients_list,
        departments_list=departments_list,
        emp_id=emp_id,
        client_partner=client_partner,
        allowed_shifts=allowed_shifts_for_filter,
    )

    # Latest-style (no explicit months/years) for response caching
    latest_style_request = (not selected_years and not selected_months)
    response_cache_key = None
    if latest_style_request:
        response_cache_key = f"{latest_month_cache_key(payload)}:response"
        cached_resp = cache.get(response_cache_key)
        if cached_resp:
            return cached_resp

    query = build_base_query(db)

    # Filter clients/departments (same logic used in has_data_for_month)
    if clients_list:
        client_filters = []
        if departments_list:
            depts_lower = [d.lower() for d in departments_list]
            for c in clients_list:
                client_filters.append(
                    and_(
                        func.lower(ShiftAllowances.client) == c.lower(),
                        func.lower(ShiftAllowances.department).in_(depts_lower)
                    )
                )
        else:
            for c in clients_list:
                client_filters.append(func.lower(ShiftAllowances.client) == c.lower())
        if client_filters:
            query = query.filter(or_(*client_filters))

    # Filter emp_id
    if emp_id:
        ids = [emp_id] if isinstance(emp_id, str) else emp_id
        query = query.filter(
            func.lower(ShiftAllowances.emp_id).in_([clean_str(e).lower() for e in ids])
        )

    # Filter client_partner
    if client_partner:
        col = ShiftAllowances.client_partner
        if isinstance(client_partner, list):
            filters_cp = [
                func.lower(col).like(f"%{clean_str(cp).lower()}%")
                for cp in client_partner if clean_str(cp)
            ]
            if filters_cp:
                query = query.filter(or_(*filters_cp))
        else:
            query = query.filter(func.lower(col).like(f"%{clean_str(client_partner).lower()}%"))

    # Filter months
    query = query.filter(
        or_(*[
            and_(
                extract("year", ShiftAllowances.duration_month) == m.year,
                extract("month", ShiftAllowances.duration_month) == m.month
            )
            for m in months_to_use
        ])
    )

    # Optionally filter by shifts early (to reduce rows)
    if allowed_shifts_for_filter and len(allowed_shifts_for_filter) > 0:
        query = query.filter(func.upper(ShiftMapping.shift_type).in_(list(allowed_shifts_for_filter)))

    # Execute query
    rows = query.all()

    
    periods_map: Dict[str, Any] = {}
    present_periods_set = set()

    for dm, client, dept, eid, ename, cp, stype, days, amt in rows:
        stype_norm = clean_str(stype).upper()
        if stype_norm not in shift_key_set:
            continue

        period_key = dm.strftime("%Y-%m")
        present_periods_set.add(period_key)

        month_block = periods_map.setdefault(
            period_key,
            {
                "clients": {},
                "month_total": {
                    "total_head_count": 0,
                    **{k: 0.0 for k in shift_keys},
                    "total_allowance": 0.0
                }
            }
        )

        client_safe = clean_str(client)
        dept_safe = clean_str(dept)
        cp_safe = clean_str(cp)

        client_block = month_block["clients"].setdefault(
            client_safe,
            {
                "client_name": client_safe,  # for sorting/keying
                **{k: 0.0 for k in shift_keys},
                "departments": {},
                "client_head_count": 0,
                "client_total": 0.0,
                "client_partner": cp_safe or "UNKNOWN"
            }
        )

        dept_block = client_block["departments"].setdefault(
            dept_safe,
            {
                **{k: 0.0 for k in shift_keys},
                "dept_total": 0.0,
                "employees": [],
                "dept_head_count": 0
            }
        )

        # Employee aggregation with headcount range semantics
        employee = next((e for e in dept_block["employees"] if e["emp_id"] == eid), None)
        if not employee:
            prospective_dept_headcount = dept_block["dept_head_count"] + 1
            prospective_client_headcount = client_block["client_head_count"] + 1
            # Range check at dept level if departments selected; else at client level
            total_headcount_for_check = (
                prospective_dept_headcount if departments_selected else prospective_client_headcount
            )
            passes_headcount = True if headcount_ranges is None else any(
                start <= total_headcount_for_check <= end for start, end in headcount_ranges
            )
            if not passes_headcount:
                continue

            employee = {
                "emp_id": eid,
                "emp_name": ename,
                "client_partner": cp_safe or "UNKNOWN",
                **{k: 0.0 for k in shift_keys},
                "total": 0.0
            }
            dept_block["employees"].append(employee)
            dept_block["dept_head_count"] += 1
            client_block["client_head_count"] += 1
            month_block["month_total"]["total_head_count"] += 1

        # Monetary aggregation
        val = float(days or 0) * float(amt or 0)
        employee[stype_norm] += val
        employee["total"] += val
        dept_block[stype_norm] += val
        dept_block["dept_total"] += val
        client_block[stype_norm] += val
        client_block["client_total"] += val
        month_block["month_total"][stype_norm] += val
        month_block["month_total"]["total_allowance"] += val

   
    for period, pdata in periods_map.items():
        clients_map = pdata.get("clients", {})
        clients_data = list(clients_map.values())

        reverse = (sort_order == "desc")
        sort_key = (sort_by or "").lower()

        if sort_key == "head_count":
            clients_data.sort(key=lambda x: x.get("client_head_count", 0), reverse=reverse)
        elif sort_key == "client":
            clients_data.sort(key=lambda x: (x.get("client_name") or "").lower(), reverse=reverse)
        elif sort_key == "client_partner":
            clients_data.sort(key=lambda x: (x.get("client_partner") or "").lower(), reverse=reverse)
        elif sort_key == "departments":
            clients_data.sort(key=lambda x: len(x.get("departments", {})), reverse=reverse)
        elif sort_key == "total_allowance":
            clients_data.sort(key=lambda x: x.get("client_total", 0.0), reverse=reverse)
        # else: leave as-is

        # Rebuild ordered dict keyed by client_name
        pdata["clients"] = {c["client_name"]: c for c in clients_data}

  
    present_periods = sorted(present_periods_set)  # ascending lex YYYY-MM
    missing_periods: List[str] = []
    message: Optional[str] = None

    if requested_periods:
        missing_periods = [p for p in requested_periods if p not in present_periods]
        if missing_periods and present_periods:
            message = f"No data present for the following selected period(s): {', '.join(missing_periods)}"
        elif missing_periods and not present_periods:
            message = "No data present for the selected month(s)/year(s)."
    else:
        # Default/latest mode without explicit selection
        if not present_periods:
            message = "No data present."

    final_response = {
        "periods": periods_map,
        "meta": {
            "requested_periods": requested_periods,
            "present_periods": present_periods,
            "missing_periods": missing_periods,
            "message": message,
        }
    }

    # Cache the final response for latest-month-style requests
    if latest_style_request and response_cache_key:
        cache.set(response_cache_key, final_response, expire=CACHE_TTL)

    return final_response