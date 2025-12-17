from datetime import date
from typing import List, Dict
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
import pandas as pd
import os

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount


# ---------------- HELPERS ----------------

def validate_year(year: int):
    current_year = date.today().year
    if year <= 0:
        raise HTTPException(400, "selected_year must be greater than 0")
    if year > current_year:
        raise HTTPException(400, "selected_year cannot be in the future")


def quarter_to_months(q: str) -> List[int]:
    mapping = {
        "Q1": [1, 2, 3],
        "Q2": [4, 5, 6],
        "Q3": [7, 8, 9],
        "Q4": [10, 11, 12],
    }
    q = q.upper().strip()
    if q not in mapping:
        raise HTTPException(400, "Invalid quarter (Q1–Q4 expected)")
    return mapping[q]


def month_range(start: str, end: str) -> Dict[int, List[int]]:
    sy, sm = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))

    if (sy, sm) > (ey, em):
        raise HTTPException(400, "start_month cannot be greater than end_month")

    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    result = {}
    for y, m in months:
        result.setdefault(y, []).append(m)

    return result


# ---------------- MAIN SERVICE ----------------

def client_summary_download_service(db: Session, payload: dict) -> str:

    payload = payload or {}

    # ---------------- CLIENT FILTER ----------------
    clients_payload = payload.get("clients")

    if not clients_payload or clients_payload == "ALL":
        normalized_clients = {}
        is_all_clients = True

        # ✅ ONLY CHANGE: default latest month if no filters
        if (
            not payload.get("selected_year")
            and not payload.get("selected_months")
            and not payload.get("selected_quarters")
            and not payload.get("start_month")
            and not payload.get("end_month")
        ):
            latest_month_obj = (
                db.query(func.max(ShiftAllowances.duration_month))
                .scalar()
            )

            if not latest_month_obj:
                today = date.today()
                latest_month_obj = date(today.year, today.month, 1)

            payload["selected_year"] = str(latest_month_obj.year)
            payload["selected_months"] = [latest_month_obj.month]

    elif isinstance(clients_payload, dict):
        normalized_clients = {
            c.lower(): [d.lower() for d in (depts or [])]
            for c, depts in clients_payload.items()
        }
        is_all_clients = False
    else:
        raise HTTPException(400, "clients must be 'ALL' or client -> departments")

    start_month = payload.get("start_month")
    end_month = payload.get("end_month")
    selected_year = payload.get("selected_year")
    selected_months = payload.get("selected_months", [])
    selected_quarters = payload.get("selected_quarters", [])

    data_frames = []
    missing_months = []

    # ---------------- RANGE MODE ----------------
    if start_month and end_month:
        year_month_map = month_range(start_month, end_month)

        for year, months in year_month_map.items():
            rows = fetch_rows(db, year, months, normalized_clients)

            if not rows:
                for m in months:
                    missing_months.append(f"{year}-{m:02d}")
                continue

            data_frames.append(build_dataframe(rows))

        if missing_months:
            raise HTTPException(
                404,
                f"No data available for months: {', '.join(missing_months)}"
            )

    # ---------------- MONTH / QUARTER MODE ----------------
    else:
        if (selected_months or selected_quarters) and not selected_year:
            raise HTTPException(
                400,
                "selected_year is mandatory when using selected_months or selected_quarters"
            )

        year = int(selected_year) if selected_year else date.today().year
        validate_year(year)

        months: List[int] = []

        if selected_quarters:
            for q in selected_quarters:
                months.extend(quarter_to_months(q))
        elif selected_months:
            months = [int(m) for m in selected_months]
        else:
            latest_month = (
                db.query(func.max(func.extract("month", ShiftAllowances.duration_month)))
                .filter(func.extract("year", ShiftAllowances.duration_month) == year)
                .scalar()
            )
            if not latest_month:
                raise HTTPException(404, "No data available for the current year")
            months = [int(latest_month)]

        rows = fetch_rows(db, year, months, normalized_clients)

        if not rows:
            raise HTTPException(404, "No data available for selected filters")

        data_frames.append(build_dataframe(rows))

    # ---------------- EXPORT ----------------
    final_df = pd.concat(data_frames, ignore_index=True)

    os.makedirs("exports", exist_ok=True)
    file_path = "exports/client_summary.xlsx"

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="Client Summary")

    return file_path


# ---------------- QUERY & DF HELPERS ----------------

def fetch_rows(db, year: int, months: List[int], normalized_clients: dict):
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
        .filter(
            func.extract("year", ShiftAllowances.duration_month) == year,
            func.extract("month", ShiftAllowances.duration_month).in_(months),
        )
    )

    rows = []
    for r in query.all():
        client = (r.client or "").lower()
        dept = (r.department or "").lower()

        if normalized_clients:
            matched = next((c for c in normalized_clients if c in client), None)
            if not matched:
                continue
            allowed = normalized_clients[matched]
            if allowed and dept not in allowed:
                continue

        rows.append(r)

    return rows


def build_dataframe(rows):
    grouped = {}

    for r in rows:
        key = (r.duration_month.strftime("%Y-%m"), r.emp_id)

        if key not in grouped:
            grouped[key] = {
                "Year-Month": r.duration_month.strftime("%Y-%m"),
                "Client": r.client,
                "Department": r.department,
                "Employee ID": r.emp_id,
                "Employee Name": r.emp_name,
                "Account Manager": r.account_manager,
                "Shift Type-Days": {},
                "Total Allowance": 0,
            }

        grouped[key]["Shift Type-Days"][r.shift_type] = r.days
        grouped[key]["Total Allowance"] += float(r.days or 0) * float(r.amount or 0)

    data = []
    for g in grouped.values():
        g["Shift Type-Days"] = ",".join(
            f"{stype}-{days}" for stype, days in g.pop("Shift Type-Days").items()
        )
        data.append(g)

    return pd.DataFrame(data)
