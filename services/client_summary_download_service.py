"""
Service for exporting client summary data as an Excel file.

Requirements implemented:
- NO hardcoded shift types (dynamic from get_all_shift_keys)
- Uses ONLY client_partner 
- Excel: all content centered
- Shift labels (timing + INR rate) appear ONLY in Excel headers (from config via get_shift_string)
- Caching technique: same as before (cache file_path for default latest-month request)
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, List, Any

import pandas as pd
from fastapi import HTTPException
from sqlalchemy.orm import Session
from diskcache import Cache

from utils.shift_config import get_all_shift_keys, get_shift_string

from services.client_summary_service import (
    client_summary_service,
    is_default_latest_month_request,
    LATEST_MONTH_KEY,
    CACHE_TTL,
)

cache = Cache("./diskcache/latest_month")

EXPORT_DIR = "exports"
DEFAULT_EXPORT_FILE = "client_summary_latest.xlsx"


def _shift_header(key: str) -> str:
    """Excel header label for shift key using config (may include \\n)."""
    return get_shift_string(key) or key


def _money(v: Any) -> float:
    """Coerce to float for numeric excel formatting."""
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _write_excel(df: pd.DataFrame, payload: dict, currency_cols: List[str]) -> str:
    """Write DataFrame to Excel with center alignment and currency formats."""
    os.makedirs(EXPORT_DIR, exist_ok=True)

    if is_default_latest_month_request(payload):
        file_path = os.path.join(EXPORT_DIR, DEFAULT_EXPORT_FILE)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(EXPORT_DIR, f"client_summary_{timestamp}.xlsx")

    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Client Summary")

        workbook = writer.book
        ws = writer.sheets["Client Summary"]

        header_fmt = workbook.add_format({
            "text_wrap": True,
            "align": "center",
            "valign": "vcenter",
            "bold": True,
            "border": 1,
            "bg_color": "#EDEDED",
        })

        center_fmt = workbook.add_format({
            "align": "center",
            "valign": "vcenter",
            "border": 1,
        })

        inr_fmt = workbook.add_format({
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "num_format": "₹ #,##0",
        })

      
        for c, col_name in enumerate(df.columns):
            ws.write(0, c, col_name, header_fmt)

        ws.set_row(0, 60)
        ws.freeze_panes(1, 0)

        currency_set = set(currency_cols)

        for c, col_name in enumerate(df.columns):
            lines = str(col_name).split("\n")
            longest = max((len(x) for x in lines), default=len(str(col_name)))
            width = min(max(longest + 2, 12), 45)

            if col_name in ("Client", "Client Partner", "Department"):
                width = max(width, 18)

            fmt = inr_fmt if col_name in currency_set else center_fmt
            ws.set_column(c, c, width, fmt)

    return file_path


def client_summary_download_service(db: Session, payload: dict) -> str:
    """
    Generate and export client summary Excel.

    Cache rules:
    - Uses cache ONLY for default latest-month request
    - Caches file_path and reuses it if the file exists
    """
    payload = payload or {}

   
    if is_default_latest_month_request(payload):
        cached = cache.get(f"{LATEST_MONTH_KEY}:excel")
        if cached and os.path.exists(cached.get("file_path", "")):
            return cached["file_path"]

    emp_filter = payload.get("emp_id")
    partner_filter = payload.get("client_partner")

    summary_data = client_summary_service(db, payload)
    if not summary_data:
        raise HTTPException(404, "No data available")

    shift_keys = [k.upper().strip() for k in get_all_shift_keys()]
    shift_cols = [_shift_header(k) for k in shift_keys] 

    rows: List[Dict[str, Any]] = []

    for period_key in sorted(summary_data):
        period_data = summary_data[period_key]
        clients = period_data.get("clients")
        if not clients:
            continue

        for client_name, client_block in clients.items():
            partner_value = client_block.get("client_partner", "")
            departments = client_block.get("departments", {})

            for dept_name, dept_block in departments.items():
                employees = dept_block.get("employees", [])

               
                if not employees:
                    if partner_filter and partner_filter != partner_value:
                        continue

                    row = {
                        "Period": period_key,
                        "Client": client_name,
                        "Client Partner": partner_value,
                        "Employee ID": "",
                        "Department": dept_name,
                        "Head Count": int(dept_block.get("dept_head_count", 0) or 0),
                    }

                    for k, col in zip(shift_keys, shift_cols):
                        row[col] = _money(dept_block.get(f"dept_{k}", 0))

                    row["Total Allowance"] = _money(dept_block.get("dept_total", 0))
                    rows.append(row)
                    continue

               
                for emp in employees:
                    if emp_filter and emp_filter != emp.get("emp_id"):
                        continue

                    emp_partner = emp.get("client_partner", partner_value)
                    if partner_filter and partner_filter != emp_partner:
                        continue

                    row = {
                        "Period": period_key,
                        "Client": client_name,
                        "Client Partner": emp_partner,
                        "Employee ID": emp.get("emp_id", ""),
                        "Department": dept_name,
                        "Head Count": 1,
                    }

                    for k, col in zip(shift_keys, shift_cols):
                        row[col] = _money(emp.get(k, dept_block.get(f"dept_{k}", 0)))

                    row["Total Allowance"] = _money(emp.get("total", dept_block.get("dept_total", 0)))
                    rows.append(row)

    if not rows:
        raise HTTPException(404, "No data available for export")

    df = pd.DataFrame(rows)

   
    df["Period"] = pd.to_datetime(df["Period"], format="%Y-%m", errors="coerce")
    df = df.sort_values(by=["Period", "Client", "Department", "Employee ID"])
    df["Period"] = df["Period"].dt.strftime("%Y-%m")

    
    ordered_cols = (
        ["Period", "Client", "Client Partner", "Employee ID", "Department", "Head Count"]
        + shift_cols
        + ["Total Allowance"]
    )
    df = df[[c for c in ordered_cols if c in df.columns]]

  
    currency_cols = shift_cols + ["Total Allowance"]
    file_path = _write_excel(df, payload, currency_cols=currency_cols)

    if is_default_latest_month_request(payload):
        cache.set(
            f"{LATEST_MONTH_KEY}:excel",
            {"_cached_month": df["Period"].iloc[0], "file_path": file_path},
            expire=CACHE_TTL,
        )

    return file_path