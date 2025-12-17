from datetime import date, datetime
from typing import List, Dict
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount


# ---------------- HELPERS ----------------

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
    mapping = {
        "Q1": [1, 2, 3],
        "Q2": [4, 5, 6],
        "Q3": [7, 8, 9],
        "Q4": [10, 11, 12],
    }
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

    # ---------------- FILTER INPUTS ----------------
    selected_year = payload.get("selected_year")
    selected_months = payload.get("selected_months", [])
    selected_quarters = payload.get("selected_quarters", [])
    start_month = payload.get("start_month")
    end_month = payload.get("end_month")

    # Initialize months list upfront
    months: List[date] = []

    # ---------------- CLIENT NORMALIZATION ----------------
    clients_payload = payload.get("clients")

    if not clients_payload or clients_payload == "ALL":
        normalized_clients = {}

        # ---- DEFAULT: latest month from DB if no month filters ----
        if not selected_months and not selected_quarters and not start_month and not end_month:
            # get the latest month stored in DB
            latest_month_obj = db.query(func.max(ShiftAllowances.duration_month)).scalar()

            if not latest_month_obj:
                # fallback if DB empty
                today = date.today()
                latest_month_obj = date(today.year, today.month, 1)

            months = [latest_month_obj]
            selected_year = str(latest_month_obj.year)

    elif isinstance(clients_payload, dict):
        normalized_clients = {
            c.lower(): [d.lower() for d in (depts or [])]
            for c, depts in clients_payload.items()
        }
        if (
            not selected_year
            and not selected_months
            and not selected_quarters
            and not start_month
            and not end_month
        ):
            current_year = date.today().year

            latest_month = (
                db.query(func.max(ShiftAllowances.duration_month))
                .filter(func.extract("year", ShiftAllowances.duration_month) == current_year)
                .scalar()
            )

            if not latest_month:
                raise HTTPException(404, "No data available for the current year")

            months = [latest_month]
            selected_year = str(current_year)
    else:
        raise HTTPException(
            400,
            "clients must be 'ALL' or a mapping of client -> departments"
        )

    # REQUIRED VALIDATION
    if (selected_months or selected_quarters) and not selected_year:
        raise HTTPException(
            400,
            "selected_year is mandatory when using selected_months or selected_quarters"
        )

    quarter_map: Dict[str, List[date]] = {}   

    # ---- Month range ----
    if start_month and end_month:
        start_date = parse_yyyy_mm(start_month)
        end_date = parse_yyyy_mm(end_month)
        months = month_range(start_date, end_date)

    # ---- selected_months ----
    elif selected_months:
        validate_year(int(selected_year))
        year = int(selected_year)
        months = [date(year, int(m), 1) for m in selected_months]

    # ---- selected_quarters ----
    elif selected_quarters:
        validate_year(int(selected_year))
        year = int(selected_year)

        for q in selected_quarters:
            month_list = [date(year, m, 1) for m in quarter_to_months(q)]
            start_key = month_list[0].strftime("%Y-%m")
            end_key = month_list[-1].strftime("%Y-%m")
            quarter_key = f"{start_key} - {end_key}"
            quarter_map[quarter_key] = month_list

    elif not months:
        raise HTTPException(400, "No valid date filter provided")

    # ---------------- RESPONSE SKELETON ----------------
    response: Dict = {}
    if selected_quarters:
        for q_key in quarter_map:
            response[q_key] = {"message": f"No data found for {q_key}"}
    else:
        for m in months:
            key = m.strftime("%Y-%m")
            response[key] = {"message": f"No data found for {key}"}

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
        .join(
            ShiftsAmount,
            and_(
                ShiftsAmount.shift_type == ShiftMapping.shift_type,
                ShiftsAmount.payroll_year
                == func.to_char(ShiftAllowances.payroll_month, "YYYY"),
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
                filters.append(
                    func.lower(ShiftAllowances.client) == client_name
                )

        query = query.filter(or_(*filters))

    # ---- Date filtering ----
    if selected_quarters:
        all_quarter_months = []
        for mlist in quarter_map.values():
            all_quarter_months.extend(mlist)
        query = query.filter(ShiftAllowances.duration_month.in_(all_quarter_months))
    else:
        query = query.filter(ShiftAllowances.duration_month.in_(months))

    rows = query.all()

    # ---------------- POPULATE RESPONSE ----------------
    for dm, client, dept, emp_id, emp_name, acc_mgr, stype, days, amt in rows:

        if selected_quarters:
            dm_key = dm.replace(day=1)
            period_key = next(
                q for q, mlist in quarter_map.items() if dm_key in mlist
            )
        else:
            period_key = dm.strftime("%Y-%m")

        if "message" in response.get(period_key, {}):
            response[period_key] = {
                "clients": {},
                "month_total": {
                    "total_head_count": 0,
                    **empty_shift_totals(),
                    "total_allowance": 0,
                },
            }

        client_key = (client or "").strip()
        dept_key = (dept or "").strip()

        total = float(days or 0) * float(amt or 0)
        month_block = response[period_key]

        client_block = month_block["clients"].setdefault(client_key, {
            **{f"client_{k}": 0 for k in ["A", "B", "C", "PRIME"]},
            "departments": {},
            "client_head_count": 0,
            "client_total": 0,
        })

        dept_block = client_block["departments"].setdefault(dept_key, {
            **{f"dept_{k}": 0 for k in ["A", "B", "C", "PRIME"]},
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
    
    if isinstance(clients_payload, dict):
        for period_key, period_block in response.items():
            if "clients" not in period_block:
                continue

            for client_name, dept_list in clients_payload.items():
                client_block = period_block["clients"].setdefault(client_name, {
                    **{f"client_{k}": 0 for k in ["A", "B", "C", "PRIME"]},
                    "departments": {},
                    "client_head_count": 0,
                    "client_total": 0,
                })

                for dept in dept_list or []:
                    if dept not in client_block["departments"]:
                        client_block["departments"][dept] = {
                            **{f"dept_{k}": 0 for k in ["A", "B", "C", "PRIME"]},
                            "dept_total": 0,
                            "employees": [],
                            "dept_head_count": 0,
                            "error": f"No data available for {client_name} - {dept} in {period_key}",
                        } 

    return response
