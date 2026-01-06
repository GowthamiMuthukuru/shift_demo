"""Client summary service for month, quarter, and range based analytics."""

from datetime import date, datetime
from typing import List, Dict
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, cast, Integer

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount

# ================= CACHE IMPORTS =================
from diskcache import Cache

cache = Cache("./diskcache/latest_month")
LATEST_MONTH_KEY = "client_summary:latest_month"
CACHE_TTL = 24 * 60 * 60  # 1 day
# ===============================================


# ---------------- HELPERS ----------------

def is_default_latest_month_request(payload: dict) -> bool:
    return (
        not payload
        or (
            payload.get("clients") in (None, "ALL")
            and not payload.get("selected_year")
            and not payload.get("selected_months")
            and not payload.get("selected_quarters")
            and not payload.get("start_month")
            and not payload.get("end_month")
        )
    )


def validate_year(year: int):
    current_year = date.today().year
    if year <= 0:
        raise HTTPException(400, "selected_year must be greater than 0")
    if year > current_year:
        raise HTTPException(400, "selected_year cannot be in the future")


def parse_yyyy_mm(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m").date().replace(day=1)
    except Exception:
        raise HTTPException(400, "Invalid month format. Expected YYYY-MM")


def quarter_to_months(q: str) -> List[int]:
    mapping = {"Q1": [1,2,3], "Q2": [4,5,6], "Q3": [7,8,9], "Q4": [10,11,12]}
    q = q.upper().strip()
    if q not in mapping:
        raise HTTPException(400, "Invalid quarter (expected Q1–Q4)")
    return mapping[q]


def month_range(start: date, end: date) -> List[date]:
    if start > end:
        raise HTTPException(400, "start_month cannot be after end_month")
    months = []
    cur = start
    while cur <= end:
        months.append(cur)
        year = cur.year + (cur.month // 12)
        month = (cur.month % 12) + 1
        cur = cur.replace(year=year, month=month)
    return months


def empty_shift_totals():
    return {"A": 0, "B": 0, "C": 0, "PRIME": 0}


# ---------------- MAIN SERVICE ----------------

def client_summary_service(db: Session, payload: dict):
    payload = payload or {}

    # ---------- CACHE READ (LATEST MONTH ONLY) ----------
    if is_default_latest_month_request(payload):
        cached = cache.get(LATEST_MONTH_KEY)
        if cached:
            print("CACHE HIT: latest month")
            return cached["data"]
    # ---------------------------------------------------

    selected_year = payload.get("selected_year")
    selected_months = payload.get("selected_months", [])
    selected_quarters = payload.get("selected_quarters", [])
    start_month = payload.get("start_month")
    end_month = payload.get("end_month")
    clients_payload = payload.get("clients")

    months: List[date] = []

    # ---------------- CLIENT NORMALIZATION ----------------
    if not clients_payload or clients_payload == "ALL":
        normalized_clients = {}

        if not selected_year and not selected_months and not selected_quarters and not start_month and not end_month:
            latest_month_obj = db.query(func.max(ShiftAllowances.duration_month)).scalar()
            if not latest_month_obj:
                today = date.today()
                latest_month_obj = date(today.year, today.month, 1)
            months = [date(latest_month_obj.year, latest_month_obj.month, 1)]
            selected_year = str(latest_month_obj.year)

    elif isinstance(clients_payload, dict):
        normalized_clients = {
            c.lower(): [d.lower() for d in (depts or [])]
            for c, depts in clients_payload.items()
        }

        if not selected_year and not selected_months and not selected_quarters and not start_month and not end_month:
            latest_month = db.query(func.max(ShiftAllowances.duration_month)).scalar()
            if not latest_month:
                raise HTTPException(404, "No data available")
            months = [date(latest_month.year, latest_month.month, 1)]
            selected_year = str(latest_month.year)
    else:
        raise HTTPException(400, "clients must be 'ALL' or a mapping of client -> departments")

    if (selected_months or selected_quarters) and not selected_year:
        raise HTTPException(400, "selected_year is mandatory when using selected_months or selected_quarters")

    quarter_map: Dict[str, List[date]] = {}

    if start_month and end_month:
        months = month_range(parse_yyyy_mm(start_month), parse_yyyy_mm(end_month))
    elif selected_months:
        validate_year(int(selected_year))
        y = int(selected_year)
        months = [date(y, int(m), 1) for m in selected_months]
    elif selected_quarters:
        validate_year(int(selected_year))
        y = int(selected_year)
        for q in selected_quarters:
            mlist = [date(y, m, 1) for m in quarter_to_months(q)]
            quarter_map[f"{mlist[0]:%Y-%m} - {mlist[-1]:%Y-%m}"] = mlist
    elif not months:
        raise HTTPException(400, "No valid date filter provided")

    # ---------------- RESPONSE SKELETON ----------------
    response: Dict = {}
    if selected_quarters:
        for q in quarter_map:
            response[q] = {"message": f"No data found for {q}"}
    else:
        for m in months:
            response[m.strftime("%Y-%m")] = {"message": f"No data found"}

    # ---------------- DB QUERY ----------------
    query = (
        db.query(
            ShiftAllowances.duration_month,
            ShiftAllowances.client,
            ShiftAllowances.department,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.account_manager,
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
                == func.extract("year", ShiftAllowances.duration_month),
            ),
        )
    )

    if normalized_clients:
        filters = []
        for client_name, dept_list in normalized_clients.items():
            if dept_list:
                filters.append(
                    and_(
                        func.lower(ShiftAllowances.client) == client_name,
                        func.lower(ShiftAllowances.department).in_(dept_list),
                    )
                )
            else:
                filters.append(func.lower(ShiftAllowances.client) == client_name)
        query = query.filter(or_(*filters))

    date_filters = months if months else [m for ml in quarter_map.values() for m in ml]
    query = query.filter(
        or_(*[
            and_(
                func.extract("year", ShiftAllowances.duration_month) == m.year,
                func.extract("month", ShiftAllowances.duration_month) == m.month,
            )
            for m in date_filters
        ])
    )

    rows = query.all()

    # ---------------- POPULATE RESPONSE ----------------
    for dm, client, dept, emp_id, emp_name, acc_mgr, stype, days, amt in rows:
        period_key = (
            next(q for q, mlist in quarter_map.items() if dm.replace(day=1) in mlist)
            if selected_quarters
            else dm.strftime("%Y-%m")
        )

        if "message" in response.get(period_key, {}):
            response[period_key] = {
                "clients": {},
                "month_total": {
                    "total_head_count": 0,
                    **empty_shift_totals(),
                    "total_allowance": 0,
                },
            }

        total = float(days or 0) * float(amt or 0)
        month_block = response[period_key]

        client_block = month_block["clients"].setdefault(client or "", {
            **{f"client_{k}": 0 for k in ["A","B","C","PRIME"]},
            "departments": {},
            "client_head_count": 0,
            "client_total": 0,
        })

        dept_block = client_block["departments"].setdefault(dept or "", {
            **{f"dept_{k}": 0 for k in ["A","B","C","PRIME"]},
            "dept_total": 0,
            "employees": [],
            "dept_head_count": 0,
        })

        emp = next((e for e in dept_block["employees"] if e["emp_id"] == emp_id), None)
        if not emp:
            emp = {
                "emp_id": emp_id,
                "emp_name": emp_name,
                "account_manager": acc_mgr,
                **empty_shift_totals(),
                "total": 0,
            }
            dept_block["employees"].append(emp)
            dept_block["dept_head_count"] += 1
            client_block["client_head_count"] += 1
            month_block["month_total"]["total_head_count"] += 1

        emp[stype] += total
        emp["total"] += total
        dept_block[f"dept_{stype}"] += total
        dept_block["dept_total"] += total
        client_block[f"client_{stype}"] += total
        client_block["client_total"] += total
        month_block["month_total"][stype] += total
        month_block["month_total"]["total_allowance"] += total

    # ---------- CACHE WRITE (LATEST MONTH ONLY) ----------
    if is_default_latest_month_request(payload):
        cache.set(LATEST_MONTH_KEY,{
            "_cached_month": months[0].strftime("%Y-%m"),
            "data": response
            },
            expire=CACHE_TTL)
    # ---------------------------------------------------

    return response
