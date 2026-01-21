"""
Service for exporting client summary data as an Excel file.

This module converts client summary analytics into a structured
Excel export with optional caching for latest-month requests.
"""

import os
from datetime import date, datetime
from typing import Dict, List

import pandas as pd
from fastapi import HTTPException
from sqlalchemy.orm import Session
from diskcache import Cache


from services.client_summary_service import (
    client_summary_service,
    is_default_latest_month_request,
    LATEST_MONTH_KEY,
    CACHE_TTL,
)


cache = Cache("./diskcache/latest_month")

EXPORT_DIR = "exports"
DEFAULT_EXPORT_FILE = "client_summary_latest.xlsx"




def _format_currency(value: float) -> str:
    """Format numeric value as INR currency."""
    return f"₹{value:,.0f}"


def _append_department_row(
    rows: List[Dict],
    period: str,
    client: str,
    partner: str,
    dept_name: str,
    dept_block: Dict,
) -> None:
    """Append a department-level row."""
    rows.append(
        {
            "Period": period,
            "Client": client,
            "Client Partner": partner,
            "Employee ID": "",
            "Department": dept_name,
            "Head Count": dept_block.get("dept_head_count", 0),
            "Shift A": _format_currency(dept_block.get("dept_A", 0)),
            "Shift B": _format_currency(dept_block.get("dept_B", 0)),
            "Shift C": _format_currency(dept_block.get("dept_C", 0)),
            "Shift PRIME": _format_currency(dept_block.get("dept_PRIME", 0)),
            "Total Allowance": _format_currency(dept_block.get("dept_total", 0)),
        }
    )


def _append_employee_row(
    rows: List[Dict],
    period: str,
    client: str,
    dept_name: str,
    emp: Dict,
    dept_block: Dict,
    partner_fallback: str,
) -> None:
    """Append an employee-level row."""
    rows.append(
        {
            "Period": period,
            "Client": client,
            "Client Partner": emp.get("account_manager", partner_fallback),
            "Employee ID": emp.get("emp_id", ""),
            "Department": dept_name,
            "Head Count": 1,
            "Shift A": _format_currency(emp.get("A", dept_block.get("dept_A", 0))),
            "Shift B": _format_currency(emp.get("B", dept_block.get("dept_B", 0))),
            "Shift C": _format_currency(emp.get("C", dept_block.get("dept_C", 0))),
            "Shift PRIME": _format_currency(
                emp.get("PRIME", dept_block.get("dept_PRIME", 0))
            ),
            "Total Allowance": _format_currency(
                emp.get("total", dept_block.get("dept_total", 0))
            ),
        }
    )


def _write_excel(df: pd.DataFrame, payload: dict) -> str:
    """Write DataFrame to Excel and return file path."""
    os.makedirs(EXPORT_DIR, exist_ok=True)

    if is_default_latest_month_request(payload):
        file_path = os.path.join(EXPORT_DIR, DEFAULT_EXPORT_FILE)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(
            EXPORT_DIR,
            f"client_summary_{timestamp}.xlsx",
        )

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Client Summary")

    return file_path




def client_summary_download_service(db: Session, payload: dict) -> str:
    """
    Generate and export client summary Excel.

    Rules:
    - Uses cache ONLY for default latest-month request
    - Preserves departments with zero employees
    - Supports employee & account manager filters
    """

    payload = payload or {}

    if is_default_latest_month_request(payload):
        cached = cache.get(f"{LATEST_MONTH_KEY}:excel")
        if cached and os.path.exists(cached["file_path"]):
            return cached["file_path"]

    emp_filter = payload.get("emp_id")
    manager_filter = payload.get("account_manager")

    summary_data = client_summary_service(db, payload)
    if not summary_data:
        raise HTTPException(404, "No data available")

    rows: List[Dict] = []

    for period_key in sorted(summary_data):
        period_data = summary_data[period_key]
        clients = period_data.get("clients")
        if not clients:
            continue

        for client_name, client_block in clients.items():
            partner_value = client_block.get("account_manager", "")
            departments = client_block.get("departments", {})

            for dept_name, dept_block in departments.items():
                employees = dept_block.get("employees", [])

                if not employees:
                    if manager_filter and manager_filter != partner_value:
                        continue

                    _append_department_row(
                        rows,
                        period_key,
                        client_name,
                        partner_value,
                        dept_name,
                        dept_block,
                    )
                    continue

                for emp in employees:
                    if emp_filter and emp_filter != emp.get("emp_id"):
                        continue

                    emp_manager = emp.get("account_manager", partner_value)
                    if manager_filter and manager_filter != emp_manager:
                        continue

                    _append_employee_row(
                        rows,
                        period_key,
                        client_name,
                        dept_name,
                        emp,
                        dept_block,
                        partner_value,
                    )

    if not rows:
        raise HTTPException(404, "No data available for export")

    df = pd.DataFrame(rows)
    df["Period"] = pd.to_datetime(df["Period"], format="%Y-%m")
    df = df.sort_values(
        by=["Period", "Client", "Department", "Employee ID"]
    )
    df["Period"] = df["Period"].dt.strftime("%Y-%m")

    file_path = _write_excel(df, payload)

    if is_default_latest_month_request(payload):
        cache.set(
            f"{LATEST_MONTH_KEY}:excel",
            {
                "_cached_month": df["Period"].iloc[0],
                "file_path": file_path,
            },
            expire=CACHE_TTL,
        )

    return file_path
