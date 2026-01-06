"""Service for exporting client summary data as an Excel file."""

import os
from datetime import date
from typing import List, Dict
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, extract
import pandas as pd
from services.client_summary_service import client_summary_service
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount


# ---------------- HELPERS ----------------

def validate_year(year: int):
    """Validate that the selected year is not invalid or in the future."""
    current_year = date.today().year
    if year <= 0:
        raise HTTPException(400, "selected_year must be greater than 0")
    if year > current_year:
        raise HTTPException(400, "selected_year cannot be in the future")


def quarter_to_months(q: str) -> List[int]:
    """Convert quarter string (Q1–Q4) into a list of month numbers."""
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
    """Generate a year-to-months mapping between two YYYY-MM values."""
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
    """
    Generate and export client summary Excel
    using client_summary_service output.
    """

    # ✅ Reuse existing logic (cache included)
    summary_data = client_summary_service(db, payload)

    if not summary_data:
        raise HTTPException(404, "No data available")

    rows = []

    for period_key, period_data in summary_data.items():
        if "clients" not in period_data:
            continue

        for client_name, client_block in period_data["clients"].items():
            for dept_name, dept_block in client_block["departments"].items():
                for emp in dept_block.get("employees", []):
                    rows.append({
                        "Period": period_key,
                        "Client": client_name,
                        "Department": dept_name,
                        "Employee ID": emp.get("emp_id"),
                        "Employee Name": emp.get("emp_name"),
                        "Account Manager": emp.get("account_manager"),
                        "Shift A": emp.get("A", 0),
                        "Shift B": emp.get("B", 0),
                        "Shift C": emp.get("C", 0),
                        "Prime": emp.get("PRIME", 0),
                        "Total Allowance": emp.get("total", 0),
                    })

    if not rows:
        raise HTTPException(404, "No data available for export")

    df = pd.DataFrame(rows)

    os.makedirs("exports", exist_ok=True)
    file_path = "exports/client_summary.xlsx"

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Client Summary")

    return file_path
