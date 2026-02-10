"""
Client summary service for month, quarter, and range based analytics.

This module aggregates shift allowance data across clients, departments,
employees, and time periods with optional caching for latest-month queries.

Updates:
- Dynamic shift types from config (get_all_shift_keys)
- Uses ONLY client_partner (account_manager removed)
- Summary output uses dynamic keys (no A/B/C/PRIME hardcoding)
- client_partner fallback displays "UNKNOWN" instead of empty/quotes/whitespace
- Cache fix:
  * versioned cache key to avoid serving old cached responses after code update
  * validates cached month vs latest DB month and refreshes automatically
"""

from __future__ import annotations

from datetime import date, datetime
from typing import List, Dict, Optional, Tuple, Any

from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, cast, Integer, extract

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from diskcache import Cache

from utils.shift_config import get_all_shift_keys


cache = Cache("./diskcache/latest_month")


LATEST_MONTH_KEY = "client_summary:latest_month"

CACHE_TTL = 24 * 60 * 60  


def clean_str(value: Any) -> str:
    """
    Normalize strings coming from DB:
    - None -> ""
    - trims whitespace
    - removes surrounding quotes: 'ABC' or "ABC" -> ABC
    - treats quote-only values (' or '' or "" ) as empty
    - treats "NULL"/"NONE"/"NAN" as empty
    - removes common invisible characters (NBSP, zero-width space)
    """
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
    """Get configured shift keys (normalized to uppercase)."""
    return [clean_str(k).upper() for k in get_all_shift_keys()]


def empty_shift_totals(shift_keys: List[str]) -> Dict[str, float]:
    """Return zero-initialized shift totals dynamically."""
    return {k: 0.0 for k in shift_keys}


def is_default_latest_month_request(payload: dict) -> bool:
    """Check whether the request is for default latest-month summary."""
    return (
        not payload
        or (
            payload.get("clients") in (None, "ALL")
            and not payload.get("selected_year")
            and not payload.get("selected_months")
            and not payload.get("selected_quarters")
            and not payload.get("start_month")
            and not payload.get("end_month")
            and not payload.get("emp_id")
            and not payload.get("client_partner")
        )
    )


def validate_year(year: int) -> None:
    """Validate that the year is not in the future or invalid."""
    current_year = date.today().year
    if year <= 0:
        raise HTTPException(400, "selected_year must be greater than 0")
    if year > current_year:
        raise HTTPException(400, "selected_year cannot be in the future")


def parse_yyyy_mm(value: str) -> date:
    """Parse YYYY-MM string into a date object."""
    try:
        return datetime.strptime(value, "%Y-%m").date().replace(day=1)
    except Exception as exc:
        raise HTTPException(400, "Invalid month format. Expected YYYY-MM") from exc


def quarter_to_months(quarter: str) -> List[int]:
    """Convert quarter (Q1–Q4) into month numbers."""
    mapping = {
        "Q1": [1, 2, 3],
        "Q2": [4, 5, 6],
        "Q3": [7, 8, 9],
        "Q4": [10, 11, 12],
    }

    key = clean_str(quarter).upper()
    if key not in mapping:
        raise HTTPException(400, "Invalid quarter (expected Q1–Q4)")
    return mapping[key]


def month_range(start: date, end: date) -> List[date]:
    """Generate list of month-start dates between two dates."""
    if start > end:
        raise HTTPException(400, "start_month cannot be after end_month")

    months: List[date] = []
    current = start
    while current <= end:
        months.append(current)
        year = current.year + (current.month // 12)
        month = (current.month % 12) + 1
        current = current.replace(year=year, month=month)
    return months


def normalize_clients(
    clients_payload: Optional[dict],
) -> Tuple[Dict[str, List[str]], Dict[str, str], Dict[tuple, str]]:
    """Normalize client and department filters into lowercase-safe mappings."""
    normalized_clients: Dict[str, List[str]] = {}
    client_name_map: Dict[str, str] = {}
    dept_name_map: Dict[tuple, str] = {}

    if not clients_payload or clients_payload == "ALL":
        return normalized_clients, client_name_map, dept_name_map

    if not isinstance(clients_payload, dict):
        raise HTTPException(400, "clients must be 'ALL' or {client: [departments]}")

    for client, depts in clients_payload.items():
        client_clean = clean_str(client)
        client_lc = client_clean.lower()
        client_name_map[client_lc] = client_clean
        normalized_clients[client_lc] = []

        for dept in depts or []:
            dept_clean = clean_str(dept)
            dept_lc = dept_clean.lower()
            dept_name_map[(client_lc, dept_lc)] = dept_clean
            normalized_clients[client_lc].append(dept_lc)

    return normalized_clients, client_name_map, dept_name_map


def get_latest_month(db: Session) -> date:
    """Fetch the latest available duration month."""
    latest = db.query(func.max(ShiftAllowances.duration_month)).scalar()
    if not latest:
        raise HTTPException(404, "No data available in database")
    return date(latest.year, latest.month, 1)


def build_base_query(db: Session):
    """
    Build base SQLAlchemy query for client summary.

    Uses ONLY client_partner.
    """
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
    """Return client-wise shift allowance summary (dynamic shift keys)."""
    payload = payload or {}

    shift_keys = get_shift_keys()
    shift_key_set = set(shift_keys)

    emp_id = payload.get("emp_id")
    client_partner = payload.get("client_partner")

    if is_default_latest_month_request(payload):
        cached = cache.get(LATEST_MONTH_KEY)
        if cached and isinstance(cached, dict) and "data" in cached:
            
            try:
                latest_month = get_latest_month(db).strftime("%Y-%m")
                cached_month = cached.get("_cached_month")
                if cached_month == latest_month:
                    return cached["data"]
            except Exception:
                
                pass

    selected_year = payload.get("selected_year")
    selected_months = payload.get("selected_months", [])
    selected_quarters = payload.get("selected_quarters", [])
    start_month = payload.get("start_month")
    end_month = payload.get("end_month")

    normalized_clients, client_name_map, dept_name_map = normalize_clients(payload.get("clients"))

    months: List[date] = []
    quarter_map: Dict[str, List[date]] = {}

    if start_month and end_month:
        months = month_range(parse_yyyy_mm(start_month), parse_yyyy_mm(end_month))
    elif selected_months:
        validate_year(int(selected_year))
        months = [date(int(selected_year), int(m), 1) for m in selected_months]
    elif selected_quarters:
        validate_year(int(selected_year))
        for quarter in selected_quarters:
            mlist = [date(int(selected_year), m, 1) for m in quarter_to_months(quarter)]
            quarter_map[f"{mlist[0]:%Y-%m} - {mlist[-1]:%Y-%m}"] = mlist
    else:
        months = [get_latest_month(db)]

    response: Dict = {}
    periods = quarter_map.keys() if selected_quarters else [m.strftime("%Y-%m") for m in months]
    for period in periods:
        response[period] = {"message": f"No data found for {period}"}

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
        query = query.filter(func.lower(ShiftAllowances.emp_id) == clean_str(emp_id).lower())

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

    
    date_list = [m for ml in quarter_map.values() for m in ml] if selected_quarters else months
    query = query.filter(
        or_(
            *[
                and_(
                    extract("year", ShiftAllowances.duration_month) == m.year,
                    extract("month", ShiftAllowances.duration_month) == m.month,
                )
                for m in date_list
            ]
        )
    )

    rows = query.all()

    for dm, client, dept, eid, ename, cp, stype, days, amt in rows:
        period_key = next(
            (q for q, ml in quarter_map.items() if dm.replace(day=1) in ml),
            dm.strftime("%Y-%m"),
        )

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

        stype_norm = clean_str(stype).upper()
        if stype_norm not in shift_key_set:
            continue

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

        employee[stype_norm] += total
        employee["total"] += total

        dept_block[f"dept_{stype_norm}"] += total
        dept_block["dept_total"] += total

        client_block[f"client_{stype_norm}"] += total
        client_block["client_total"] += total

        month_block["month_total"][stype_norm] += total
        month_block["month_total"]["total_allowance"] += total

   
    if is_default_latest_month_request(payload):
        cache.set(
            LATEST_MONTH_KEY,
            {"_cached_month": months[0].strftime("%Y-%m"), "data": response},
            expire=CACHE_TTL,
        )

    return response