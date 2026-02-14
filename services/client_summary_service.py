"""
Client summary service with multi-year, multi-month, shift, and headcount support.

Features:
- Filters by years, months, clients, departments, employees, client partners, shifts, headcount ranges.
- Headcount range applied at dept level if departments selected, else at client level.
- Validations for years, months, shifts, and headcount formats.
- Caching for latest-month requests.
"""

from __future__ import annotations
from datetime import date, datetime
from typing import List, Dict, Optional, Any, Tuple

from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, cast, Integer, extract

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from diskcache import Cache
from utils.shift_config import get_all_shift_keys

cache = Cache("./diskcache/latest_month")

CLIENT_SUMMARY_VERSION = "v3"
LATEST_MONTH_KEY = f"client_summary_latest:{CLIENT_SUMMARY_VERSION}"
CACHE_TTL = 24 * 60 * 60  # 24 hours


def clean_str(value: Any) -> str:
    """Normalize strings from DB"""
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
    """Get configured shift keys (uppercase)"""
    return [clean_str(k).upper() for k in get_all_shift_keys()]


def empty_shift_totals(shift_keys: List[str]) -> Dict[str, float]:
    """Zero-initialized shift totals"""
    return {k: 0.0 for k in shift_keys}


def is_default_latest_month_request(payload: dict) -> bool:
    """Check if this is default latest-month summary request"""
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
    """Validate year is not in future or invalid"""
    current_year = date.today().year
    if year <= 0:
        raise HTTPException(400, "Year must be greater than 0")
    if year > current_year:
        raise HTTPException(400, "Year cannot be in the future")


def validate_months(months: List[int]) -> None:
    """Validate month integers"""
    for m in months:
        if not 1 <= int(m) <= 12:
            raise HTTPException(400, f"Invalid month: {m}")


def parse_headcount_ranges(headcounts_payload):
    """
    Parses headcount ranges.
    Returns:
      - None  -> means ALL (no filtering)
      - List[(start, end)]
    """
    if headcounts_payload == "ALL":
        return None

    if isinstance(headcounts_payload, str):
        headcounts_payload = [headcounts_payload]

    ranges = []

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

        elif h.isdigit():
            start = end = int(h)

        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid headcount range format: {h}"
            )

        ranges.append((start, end))

    return ranges


def normalize_clients(clients_payload: Optional[Any], depts_payload: Optional[Any]) -> Tuple[Dict[str, list], Dict[str, str], dict]:
    """Normalize clients and departments"""
    normalized_clients: dict = {}
    client_name_map: dict = {}
    dept_name_map: dict = {}

    if not clients_payload or clients_payload == "ALL":
        return normalized_clients, client_name_map, dept_name_map

    if isinstance(clients_payload, str):
        clients_payload = [clients_payload]

    for client in clients_payload:
        client_clean = clean_str(client)
        client_lc = client_clean.lower()
        client_name_map[client_lc] = client_clean

        depts_list = []
        if depts_payload not in (None, "ALL"):
            if isinstance(depts_payload, str):
                depts_payload = [depts_payload]
            for d in depts_payload:
                d_clean = clean_str(d)
                dept_name_map[(client_lc, d_clean.lower())] = d_clean
                depts_list.append(d_clean.lower())
        normalized_clients[client_lc] = depts_list

    return normalized_clients, client_name_map, dept_name_map


def get_latest_month(db: Session) -> date:
    """Fetch latest month from DB"""
    latest = db.query(func.max(ShiftAllowances.duration_month)).scalar()
    if not latest:
        raise HTTPException(404, "No data available in database")
    return date(latest.year, latest.month, 1)


def build_base_query(db: Session):
    """Base SQLAlchemy query"""
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


def client_summary_service(db: Session, payload: dict):
    """Return client summary with multi-year, multi-month, shift, and headcount filters"""

    payload = payload or {}
    shift_keys = get_shift_keys()
    shift_key_set = set(shift_keys)

    emp_id = payload.get("emp_id")
    client_partner = payload.get("client_partner")
    selected_years = payload.get("years", [])
    selected_months = payload.get("months", [])
    shifts = payload.get("shifts", "ALL")
    headcounts_payload = payload.get("headcounts", "ALL")

    
    departments_selected = payload.get("departments") not in (None, "ALL")  

    if selected_years:
        for y in selected_years:
            validate_year(int(y))
    if selected_months:
        validate_months(selected_months)

    if shifts != "ALL":
        if isinstance(shifts, str):
            shifts = [shifts]
        shifts_upper = [clean_str(s).upper() for s in shifts]
        invalid_shifts = [s for s in shifts_upper if s not in shift_key_set]
        if invalid_shifts:
            raise HTTPException(400, f"Invalid shift(s): {invalid_shifts}")
        shift_key_set = set(shifts_upper)  

    headcount_ranges = parse_headcount_ranges(headcounts_payload)


    use_cache = is_default_latest_month_request(payload)
    latest_ym: Optional[str] = None
    if use_cache:
        try:
            latest_ym = get_latest_month(db).strftime("%Y-%m")
        except HTTPException:
            latest_ym = None

        if latest_ym:
            cached = cache.get(LATEST_MONTH_KEY)
            if cached and isinstance(cached, dict) and "data" in cached:
                if cached.get("_cached_month") == latest_ym:
                    return cached["data"]

    normalized_clients, client_name_map, dept_name_map = normalize_clients(
        payload.get("clients"), payload.get("departments")
    )

    months: List[date] = []
    if selected_years:
        for y in selected_years:
            if selected_months:
                months.extend([date(int(y), int(m), 1) for m in selected_months])
            else:
                months.extend([date(int(y), m, 1) for m in range(1, 13)])
    else:
        months = [get_latest_month(db)]

    response: dict = {m.strftime("%Y-%m"): {"message": f"No data found for {m.strftime('%Y-%m')}"} for m in months}

    query = build_base_query(db)

    
    if normalized_clients:
        filters = []
        for client_lc, depts_lc in normalized_clients.items():
            if depts_lc:
                filters.append(
                    and_(
                        func.lower(ShiftAllowances.client) == client_lc,
                        func.lower(ShiftAllowances.department).in_(depts_lc),
                    )
                )
            else:
                filters.append(func.lower(ShiftAllowances.client) == client_lc)
        query = query.filter(or_(*filters))
   
    if emp_id:
        if isinstance(emp_id, str):
            emp_id = [emp_id]
        query = query.filter(func.lower(ShiftAllowances.emp_id).in_([clean_str(e).lower() for e in emp_id]))

    if client_partner:
        col = ShiftAllowances.client_partner
        if isinstance(client_partner, list):
            filters = [
                func.lower(col).like(f"%{clean_str(cp).lower()}%")
                for cp in client_partner
                if isinstance(cp, str) and clean_str(cp)
            ]
            if filters:
                query = query.filter(or_(*filters))
        else:
            query = query.filter(func.lower(col).like(f"%{clean_str(client_partner).lower()}%"))

  
    query = query.filter(
        or_(
            *[
                and_(
                    extract("year", ShiftAllowances.duration_month) == m.year,
                    extract("month", ShiftAllowances.duration_month) == m.month,
                )
                for m in months
            ]
        )
    )

    rows = query.all()

    for dm, client, dept, eid, ename, cp, stype, days, amt in rows:
        stype_norm = clean_str(stype).upper()
        if stype_norm not in shift_key_set:
            continue

        period_key = dm.strftime("%Y-%m")
        if "message" in response.get(period_key, {}):
            response[period_key] = {
                "clients": {},
                "month_total": {
                    "total_head_count": 0,
                    **{k: 0.0 for k in shift_keys},
                    "total_allowance": 0.0,
                },
            }

        month_block = response[period_key]
        client_safe = clean_str(client)
        dept_safe = clean_str(dept)
        cp_safe = clean_str(cp)

        client_name = client_name_map.get(client_safe.lower(), client_safe or "UNKNOWN")
        dept_name = dept_name_map.get((client_safe.lower(), dept_safe.lower()), dept_safe or "UNKNOWN")
        cp_display = cp_safe or "UNKNOWN"

        total = float(days or 0) * float(amt or 0)

        client_block = month_block["clients"].setdefault(
            client_name,
            {
                **{f"client_{k}": 0.0 for k in shift_keys},
                "departments": {},
                "client_head_count": 0,
                "client_total": 0.0,
                "client_partner": cp_display,
            },
        )

        dept_block = client_block["departments"].setdefault(
            dept_name,
            {
                **{f"dept_{k}": 0.0 for k in shift_keys},
                "dept_total": 0.0,
                "employees": [],
                "dept_head_count": 0,
            },
        )

  
        employee = next((e for e in dept_block["employees"] if e["emp_id"] == eid), None)

        if not employee:
           
            prospective_dept_headcount = dept_block["dept_head_count"] + 1
            prospective_client_headcount = client_block["client_head_count"] + 1

            
            total_headcount_for_check = (
                prospective_dept_headcount if departments_selected else prospective_client_headcount
            )

            
            passes_headcount = True if headcount_ranges is None else any(
                start <= total_headcount_for_check <= end for start, end in headcount_ranges
            )

            if passes_headcount:
                employee = {
                    "emp_id": eid,
                    "emp_name": ename,
                    "client_partner": cp_display,
                    **{k: 0.0 for k in shift_keys},
                    "total": 0.0,
                }
                dept_block["employees"].append(employee)
                dept_block["dept_head_count"] += 1
                client_block["client_head_count"] += 1
                month_block["month_total"]["total_head_count"] += 1
            else:
               
                continue

      
        employee[stype_norm] += total
        employee["total"] += total
        dept_block[f"dept_{stype_norm}"] += total
        dept_block["dept_total"] += total
        client_block[f"client_{stype_norm}"] += total
        client_block["client_total"] += total
        month_block["month_total"][stype_norm] += total
        month_block["month_total"]["total_allowance"] += total

    if use_cache and latest_ym:
        cache.set(
            LATEST_MONTH_KEY,
            {"_cached_month": latest_ym, "data": response},
            expire=CACHE_TTL,
        )

    return response