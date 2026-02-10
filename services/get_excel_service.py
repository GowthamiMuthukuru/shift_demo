"""
Service for exporting filtered shift allowance data as an Excel download (fast)
WITH file-path caching (same technique as client summary download service).

- Uses Pandas + XlsxWriter for speed on large datasets.
- Excel headers use config display strings (with '\n') and are wrapped in Excel.
- Cell "shift_details" uses shift keys only (PST_MST etc.).
- Avoids N+1 queries by fetching all ShiftMapping rows in ONE query.
- Cache technique: store file_path in diskcache ONLY for default latest-month request.
"""

from __future__ import annotations

import os
from typing import Optional, Dict, List, Tuple, Any
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session
from diskcache import Cache
from fastapi.responses import FileResponse

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from utils.shift_config import get_shift_string, get_all_shift_keys


cache = Cache("./diskcache/latest_month")

EXPORT_DIR = "exports"
DEFAULT_EXPORT_FILE = "shift_data_latest.xlsx"

LATEST_MONTH_KEY = "shift_data:latest_month"
CACHE_TTL = 20 * 60 * 60  


def is_default_latest_month_request(
    emp_id: Optional[str] = None,
    client_partner: Optional[str] = None,
    department: Optional[str] = None,
    client: Optional[str] = None,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
) -> bool:
    """Cache ONLY for latest-month request with NO filters."""
    return (
        not emp_id
        and not client_partner
        and not department
        and not client
        and not start_month
        and not end_month
    )


def invalidate_shift_excel_cache() -> None:
    """Call after latest-month data/rates update to avoid stale cached file."""
    cache.pop(f"{LATEST_MONTH_KEY}:excel", None)


def _parse_month(month: str, field_name: str) -> datetime:
    """Convert YYYY-MM to datetime at first day of month."""
    try:
        return datetime.strptime(month, "%Y-%m").replace(day=1)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} must be YYYY-MM") from exc


def _build_shift_display_map() -> Dict[str, str]:
    """SHIFT_KEY -> SHIFT_DISPLAY_LABEL (config). Used as Excel headers."""
    keys = get_all_shift_keys()
    return {k: (get_shift_string(k) or k) for k in keys}


def _latest_available_month_dt(db: Session, base_filters: List[Any], current_month: datetime) -> datetime:
    """Find latest available month within last 12 months."""
    cutoff = current_month - relativedelta(months=11)

    latest = (
        db.query(func.max(func.date_trunc("month", ShiftAllowances.duration_month)))
        .filter(*base_filters)
        .filter(func.date_trunc("month", ShiftAllowances.duration_month) >= cutoff)
        .scalar()
    )
    if not latest:
        raise HTTPException(status_code=404, detail="No data found in last 12 months")
    return latest


def _fetch_mappings_bulk(db: Session, allowance_ids: List[int]) -> Dict[int, List[Tuple[str, float]]]:
    """Fetch all ShiftMapping rows in ONE query: id -> [(shift_type, days), ...]."""
    if not allowance_ids:
        return {}
    rows = (
        db.query(ShiftMapping.shiftallowance_id, ShiftMapping.shift_type, ShiftMapping.days)
        .filter(ShiftMapping.shiftallowance_id.in_(allowance_ids))
        .all()
    )
    out: Dict[int, List[Tuple[str, float]]] = {}
    for sid, stype, days in rows:
        out.setdefault(sid, []).append(((stype or "").upper().strip(), float(days or 0.0)))
    return out


def export_filtered_excel_df(
    db: Session,
    emp_id: Optional[str] = None,
    client_partner: Optional[str] = None,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
    department: Optional[str] = None,
    client: Optional[str] = None,
) -> pd.DataFrame:
    """Return DataFrame ready for Excel export (shift headers from config)."""
    shift_keys = get_all_shift_keys()
    shift_display_map = _build_shift_display_map()
    shift_headers = [shift_display_map[k] for k in shift_keys]

    base_filters: List[Any] = []
    if emp_id:
        base_filters.append(func.trim(ShiftAllowances.emp_id) == emp_id.strip())
    if client_partner:
        base_filters.append(func.lower(func.trim(ShiftAllowances.client_partner)) == client_partner.strip().lower())
    if department:
        base_filters.append(func.lower(func.trim(ShiftAllowances.department)) == department.strip().lower())
    if client:
        base_filters.append(func.lower(func.trim(ShiftAllowances.client)) == client.strip().lower())

    current_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if start_month or end_month:
        if not start_month:
            raise HTTPException(status_code=400, detail="start_month is required when end_month is provided")

        start_dt = _parse_month(start_month, "start_month")
        if end_month:
            end_dt = _parse_month(end_month, "end_month")
            if start_dt > end_dt:
                raise HTTPException(status_code=400, detail="start_month cannot be after end_month")
            date_filters = [
                func.date_trunc("month", ShiftAllowances.duration_month) >= start_dt,
                func.date_trunc("month", ShiftAllowances.duration_month) <= end_dt,
            ]
        else:
            date_filters = [func.date_trunc("month", ShiftAllowances.duration_month) == start_dt]
    else:
        latest_month = _latest_available_month_dt(db, base_filters, current_month)
        date_filters = [func.date_trunc("month", ShiftAllowances.duration_month) == latest_month]

    rows = (
        db.query(
            ShiftAllowances.id,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.grade,
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.project,
            ShiftAllowances.project_code,
            ShiftAllowances.client_partner,
            ShiftAllowances.delivery_manager,
            ShiftAllowances.practice_lead,
            ShiftAllowances.billability_status,
            ShiftAllowances.practice_remarks,
            ShiftAllowances.rmg_comments,
            ShiftAllowances.duration_month,
            ShiftAllowances.payroll_month,
        )
        .filter(*base_filters)
        .filter(*date_filters)
        .all()
    )

    if not rows:
        raise HTTPException(status_code=404, detail="No records found for given filters")

    allowance_map = {
        (item.shift_type or "").upper().strip(): float(item.amount or 0)
        for item in db.query(ShiftsAmount).all()
    }

    mappings_by_id = _fetch_mappings_bulk(db, [r.id for r in rows])

    final_data: List[Dict[str, Any]] = []

    for r in rows:
        mappings = mappings_by_id.get(r.id, [])
        per_shift_days = {hdr: 0.0 for hdr in shift_headers}
        shift_details_parts: List[str] = []

        total_days = 0.0
        total_allowance = 0.0

        for shift_key, days in mappings:
            if days <= 0:
                continue

            rate = float(allowance_map.get(shift_key, 0.0))
            amount = days * rate

            total_days += days
            total_allowance += amount

            shift_details_parts.append(f"{shift_key}-{days:g}*{int(rate):,}=₹{int(amount):,}")

            header = shift_display_map.get(shift_key, shift_key)
            per_shift_days[header] = per_shift_days.get(header, 0.0) + days

        record = {
            "emp_id": r.emp_id,
            "emp_name": r.emp_name,
            "grade": r.grade,
            "department": r.department,
            "client": r.client,
            "project": r.project,
            "project_code": r.project_code,
            "client_partner": r.client_partner,
            "duration_month": r.duration_month.strftime("%Y-%m") if r.duration_month else None,
            "payroll_month": r.payroll_month.strftime("%Y-%m") if r.payroll_month else None,
            "shift_details": ", ".join(shift_details_parts) if shift_details_parts else None,
            "total_days": float(total_days),
            "total_allowance": float(round(total_allowance, 2)),
            "delivery_manager": r.delivery_manager,
            "practice_lead": r.practice_lead,
            "billability_status": r.billability_status,
            "practice_remarks": r.practice_remarks,
            "rmg_comments": r.rmg_comments,
        }
        record.update({hdr: float(per_shift_days.get(hdr, 0.0)) for hdr in shift_headers})
        final_data.append(record)

    df = pd.DataFrame(final_data)

    core_cols = [
        "emp_id", "emp_name", "grade", "department", "client", "project", "project_code",
        "client_partner", "duration_month", "payroll_month",
        "shift_details", "total_days", "total_allowance",
        "delivery_manager", "practice_lead", "billability_status",
        "practice_remarks", "rmg_comments",
    ]

    ordered_cols = (
        [c for c in core_cols if c in df.columns]
        + [c for c in shift_headers if c in df.columns]
        + [c for c in df.columns if c not in set(core_cols + shift_headers)]
    )
    return df[ordered_cols]


def dataframe_to_excel_file(
    df: pd.DataFrame,
    file_path: str,
    sheet_name: str = "Shift Data",
    header_row_height: int = 60,
    freeze_header: bool = True,
) -> str:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        header_fmt = workbook.add_format({
            "text_wrap": True,
            "align": "center",
            "valign": "vcenter",
            "bold": True,
            "border": 1,
            "bg_color": "#EDEDED",
        })
        cell_fmt = workbook.add_format({
            "align": "center",
            "valign": "vcenter",
            "border": 1,
        })

        for c, name in enumerate(df.columns):
            worksheet.write(0, c, name, header_fmt)

        worksheet.set_row(0, header_row_height)
        if freeze_header:
            worksheet.freeze_panes(1, 0)

        for c, name in enumerate(df.columns):
            lines = str(name).split("\n")
            longest = max((len(x) for x in lines), default=len(str(name)))
            width = min(max(longest + 2, 12), 45)
            if str(name) in ("shift_details", "practice_remarks", "rmg_comments"):
                width = 45
            worksheet.set_column(c, c, width, cell_fmt)

    return file_path


def shift_excel_download_service(
    db: Session,
    emp_id: Optional[str] = None,
    client_partner: Optional[str] = None,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
    department: Optional[str] = None,
    client: Optional[str] = None,
) -> str:
    default_latest = is_default_latest_month_request(
        emp_id=emp_id,
        client_partner=client_partner,
        department=department,
        client=client,
        start_month=start_month,
        end_month=end_month,
    )

    if default_latest:
        cached = cache.get(f"{LATEST_MONTH_KEY}:excel")
        if cached and os.path.exists(cached.get("file_path", "")):
            return cached["file_path"]

    df = export_filtered_excel_df(
        db=db,
        emp_id=emp_id,
        client_partner=client_partner,
        start_month=start_month,
        end_month=end_month,
        department=department,
        client=client,
    )

    os.makedirs(EXPORT_DIR, exist_ok=True)
    if default_latest:
        file_path = os.path.join(EXPORT_DIR, DEFAULT_EXPORT_FILE)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(EXPORT_DIR, f"shift_data_{timestamp}.xlsx")

    dataframe_to_excel_file(df, file_path=file_path, sheet_name="Shift Data")

    if default_latest:
        cached_month = None
        if "duration_month" in df.columns and not df["duration_month"].isna().all():
            cached_month = str(df["duration_month"].dropna().iloc[0])

        cache.set(
            f"{LATEST_MONTH_KEY}:excel",
            {"_cached_month": cached_month, "file_path": file_path},
            expire=CACHE_TTL,
        )

    return file_path


def build_excel_file_response(file_path: str, download_name: str = "shift_data.xlsx") -> FileResponse:
    """Return FileResponse for saved Excel file."""
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=download_name,
    )