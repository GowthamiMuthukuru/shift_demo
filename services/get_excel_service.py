# services/get_excel_service.py
"""
Clean rewritten version WITHOUT schemas.
Fixes:
- emp_id & client_partner payload filtering
- correct default-latest caching
- unified years/months filtering
- stable Excel formatting
- legacy params still work but not used by router
"""

from __future__ import annotations

import os
import re
import tempfile
from typing import Optional, Dict, List, Tuple, Any

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session
from diskcache import Cache

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from utils.shift_config import get_shift_string, get_all_shift_keys

cache = Cache("./diskcache/latest_month")

EXPORT_DIR = "exports"
DEFAULT_EXPORT_FILE = "shift_data_latest.xlsx"
LATEST_MONTH_KEY = "shift_data:latest_month"
CACHE_TTL = 24 * 60 * 60  # 24 hours



def invalidate_shift_excel_cache() -> None:
    cache.pop(f"{LATEST_MONTH_KEY}:excel", None)



def _get_db_latest_ym(db: Session) -> Optional[str]:
    dt = db.query(func.max(func.date_trunc("month", ShiftAllowances.duration_month))).scalar()
    return dt.strftime("%Y-%m") if dt else None


def _parse_month(month: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(month, "%Y-%m").replace(day=1)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"{field_name} must be YYYY-MM")


def _build_shift_display_map() -> Dict[str, str]:
    keys = get_all_shift_keys()
    return {k: (get_shift_string(k) or k) for k in keys}


def _latest_available_month_dt(db: Session, base_filters: List[Any], current_month: datetime) -> datetime:
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


def _normalize_multi(val) -> Optional[List[str]]:
    """Returns list[str] or None if 'ALL'/empty"""
    if val is None:
        return None

    if isinstance(val, str):
        if val.strip().upper() == "ALL":
            return None
        parts = [p.strip() for p in val.split(",") if p.strip()]
        return [p.lower() for p in parts] or None

    if isinstance(val, list):
        cleaned = [str(v).strip() for v in val if str(v).strip()]
        cleaned = [v for v in cleaned if v.upper() != "ALL"]
        return [v.lower() for v in cleaned] or None

    return None


def _months_from_years_months(years, months) -> List[datetime]:
    if not years and not months:
        return []
    if years and not months:
        months = list(range(1, 12 + 1))
    years = years or []
    months = months or []
    out: List[datetime] = []
    for y in years:
        for m in months:
            if 1 <= m <= 12:
                out.append(datetime(y, m, 1))
            else:
                raise HTTPException(status_code=400, detail=f"Invalid month: {m}")
    return out


def _validate_payload(payload: dict) -> None:
    # Validate shifts if provided
    shifts = payload.get("shifts")
    if shifts is None:
        return
    if isinstance(shifts, str) and shifts.strip().upper() == "ALL":
        return

    if isinstance(shifts, str):
        lst = [s.strip() for s in shifts.split(",") if s.strip()]
    elif isinstance(shifts, list):
        lst = [str(s).strip() for s in shifts if str(s).strip()]
    else:
        raise HTTPException(status_code=400, detail="Invalid 'shifts' format")

    valid = {k.upper() for k in get_all_shift_keys()}
    invalid = [s for s in lst if s.upper() not in valid]
    if invalid:
        raise HTTPException(status_code=400, detail=f"Unknown shifts: {invalid}")


def _is_default_cache_request(db: Session, payload: dict) -> bool:
    """Cache only when ALL filters are empty and the month is latest DB month."""

    # If any filter is applied → NOT cacheable
    for fkey in ["clients", "departments", "shifts", "emp_id", "client_partner"]:
        if _normalize_multi(payload.get(fkey)):
            return False

    # Check month == latest DB month
    latest_ym = _get_db_latest_ym(db)
    if not latest_ym:
        return False

    years = payload.get("years")
    months = payload.get("months")

    if not years and not months:
        return True

    selected = _months_from_years_months(years, months)
    if len(selected) != 1:
        return False

    return selected[0].strftime("%Y-%m") == latest_ym


def _fetch_mappings_bulk(db: Session, allowance_ids: List[int]) -> Dict[int, List[Tuple[str, float]]]:
    """
    Fetch all ShiftMapping rows in ONE query.
    Returns dict: allowance_id -> [(shift_type, days), ...]
    """
    if not allowance_ids:
        return {}

    # ensure all IDs are ints
    allowance_ids = [int(i) for i in allowance_ids if i is not None]

    rows = (
        db.query(
            
            ShiftMapping.shiftallowance_id,
            ShiftMapping.shift_type,
            ShiftMapping.days
        )
        .filter(ShiftMapping.shiftallowance_id.in_(allowance_ids))  
        .all()
    )

    out: Dict[int, List[Tuple[str, float]]] = {}
    for sid, stype, days in rows:
        try:
            sid_int = int(sid)
        except (TypeError, ValueError):
            continue
        out.setdefault(sid_int, []).append(
            ((stype or "").upper().strip(), float(days or 0.0))
        )
    return out


def export_filtered_excel_df(
    db: Session,
    emp_id=None,
    client_partner=None,
    start_month=None,
    end_month=None,
    department=None,
    client=None,
    payload=None,
):
    shift_keys = get_all_shift_keys()
    shift_header_map = _build_shift_display_map()
    shift_headers = [shift_header_map[k] for k in shift_keys]

    base_filters: List[Any] = []
    join_shift_mapping = False
    shift_values_upper: List[str] = []
    date_filter_clause = None

    sort_by = "total_allowance"
    sort_order = "default"

  
    if payload:
        _validate_payload(payload)

        # Multi filters
        clients = _normalize_multi(payload.get("clients"))
        departments = _normalize_multi(payload.get("departments"))
        shifts = _normalize_multi(payload.get("shifts"))
        emp_ids = _normalize_multi(payload.get("emp_id"))
        client_partners = _normalize_multi(payload.get("client_partner"))

        if clients:
            base_filters.append(func.lower(func.trim(ShiftAllowances.client)).in_(clients))

        if departments:
            base_filters.append(func.lower(func.trim(ShiftAllowances.department)).in_(departments))

        if emp_ids:
            base_filters.append(func.lower(func.trim(ShiftAllowances.emp_id)).in_(emp_ids))

        if client_partners:
            base_filters.append(func.lower(func.trim(ShiftAllowances.client_partner)).in_(client_partners))

        if shifts:
            join_shift_mapping = True
            shift_values_upper = [s.upper() for s in shifts]

        # Month selection
        month_list = _months_from_years_months(payload.get("years"), payload.get("months"))

        if month_list:
            date_filter_clause = func.date_trunc("month", ShiftAllowances.duration_month).in_(month_list)
        else:
            now = datetime.now().replace(day=1)
            latest_month = _latest_available_month_dt(db, base_filters, now)
            date_filter_clause = func.date_trunc("month", ShiftAllowances.duration_month) == latest_month

        sort_by = payload.get("sort_by") or "total_allowance"
        sort_order = payload.get("sort_order") or "default"

    
    else:
        if emp_id:
            base_filters.append(func.lower(func.trim(ShiftAllowances.emp_id)) == emp_id.lower())
        if client_partner:
            base_filters.append(func.lower(func.trim(ShiftAllowances.client_partner)) == client_partner.lower())
        if department:
            base_filters.append(func.lower(func.trim(ShiftAllowances.department)) == department.lower())
        if client:
            base_filters.append(func.lower(func.trim(ShiftAllowances.client)) == client.lower())

        now = datetime.now().replace(day=1)
        if start_month or end_month:
            if not start_month:
                raise HTTPException(status_code=400, detail="start_month is required when end_month is provided")
            sm = _parse_month(start_month, "start_month")
            if end_month:
                em = _parse_month(end_month, "end_month")
                if sm > em:
                    raise HTTPException(status_code=400, detail="start_month cannot be after end_month")
                date_filter_clause = (
                    (func.date_trunc("month", ShiftAllowances.duration_month) >= sm)
                    & (func.date_trunc("month", ShiftAllowances.duration_month) <= em)
                )
            else:
                date_filter_clause = func.date_trunc("month", ShiftAllowances.duration_month) == sm
        else:
            latest_month = _latest_available_month_dt(db, base_filters, now)
            date_filter_clause = func.date_trunc("month", ShiftAllowances.duration_month) == latest_month

   
    q = db.query(
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
    ).filter(*base_filters)

    if join_shift_mapping:
       
        q = q.join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        q = q.filter(func.upper(func.trim(ShiftMapping.shift_type)).in_(shift_values_upper))

    if date_filter_clause is not None:
        q = q.filter(date_filter_clause)

    rows = q.distinct().all()
    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    # Robust ID extraction for bulk mapping fetch
    allowance_ids: List[int] = []
    for r in rows:
        rid = getattr(r, "id", None)
        if rid is None and isinstance(r, (tuple, list)) and len(r) > 0:
            rid = r[0]
        if rid is not None:
            try:
                allowance_ids.append(int(rid))
            except (TypeError, ValueError):
                pass

    # Bulk mapping and rates
    maps = _fetch_mappings_bulk(db, allowance_ids)
    rate_map = {
        (item.shift_type or "").upper().strip(): float(item.amount or 0)
        for item in db.query(ShiftsAmount).all()
    }

    # Build DF rows
    out: List[Dict[str, Any]] = []
    for r in rows:
        # safe id per row
        rid = getattr(r, "id", None)
        if rid is None and isinstance(r, (tuple, list)) and len(r) > 0:
            rid = r[0]

        per_shift = {hdr: 0.0 for hdr in shift_headers}
        shifts_desc: List[str] = []
        total_days = 0.0
        total_amt = 0.0

        for stype, days in maps.get(int(rid) if rid is not None else -1, []):
            rate = rate_map.get(stype, 0.0)
            amt = float(days) * float(rate)

            total_days += float(days)
            total_amt += amt

            shifts_desc.append(f"{stype}-{days:g}*{int(rate):,}=₹{int(amt):,}")

            header = shift_header_map.get(stype, stype)
            per_shift[header] = per_shift.get(header, 0.0) + float(days)

        # Format dates safely
        duration_month_str = r.duration_month.strftime("%Y-%m") if getattr(r, "duration_month", None) else None
        payroll_month_str = r.payroll_month.strftime("%Y-%m") if getattr(r, "payroll_month", None) else None

        rec = {
            "emp_id": r.emp_id,
            "emp_name": r.emp_name,
            "grade": r.grade,
            "department": r.department,
            "client": r.client,
            "project": r.project,
            "project_code": r.project_code,
            "client_partner": r.client_partner,
            "duration_month": duration_month_str,
            "payroll_month": payroll_month_str,
            "shift_details": ", ".join(shifts_desc) if shifts_desc else None,
            "total_days": float(total_days),
            "total_allowance": float(round(total_amt, 2)),
            "delivery_manager": r.delivery_manager,
            "practice_lead": r.practice_lead,
            "billability_status": r.billability_status,
            "practice_remarks": r.practice_remarks,
            "rmg_comments": r.rmg_comments,
        }

        rec.update(per_shift)
        out.append(rec)

    df = pd.DataFrame(out)

    # Sorting
    if sort_order == "default":
        ascending = False if sort_by == "total_allowance" else True
    else:
        ascending = (str(sort_order).lower() == "asc")

    if sort_by in df.columns:
        df = df.sort_values(by=sort_by, ascending=ascending, kind="mergesort")

    return df


def dataframe_to_excel_file(
    df: pd.DataFrame,
    file_path: str,
    sheet_name: str = "Shift Data",
    header_row_height: int = 60,
    freeze_header: bool = True,
) -> str:
    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

    # ensure numeric for currency col
    if "total_allowance" in df.columns:
        df["total_allowance"] = pd.to_numeric(df["total_allowance"], errors="coerce")

    with pd.ExcelWriter(file_path, engine="xlsxwriter") as w:
        df.to_excel(w, sheet_name=sheet_name, index=False)

        ws = w.sheets[sheet_name]
        wb = w.book

        header_fmt = wb.add_format({
            "bold": True,
            "text_wrap": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#EDEDED"
        })
        cell_fmt = wb.add_format({"border": 1, "align": "center", "valign": "vcenter"})
        currency_fmt = wb.add_format({
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "num_format": "₹ #,##0"
        })

        # header
        for c, col in enumerate(df.columns):
            ws.write(0, c, col, header_fmt)

        ws.set_row(0, header_row_height)
        if freeze_header:
            ws.freeze_panes(1, 0)

        # columns
        for c, col in enumerate(df.columns):
            fmt = currency_fmt if col.lower() == "total_allowance" else cell_fmt
            width = min(max(len(str(col)) + 2, 12), 45)
            if col in ("shift_details", "practice_remarks", "rmg_comments"):
                width = 45
            ws.set_column(c, c, width, fmt)

    return file_path


def _atomic_write_excel(df: pd.DataFrame, final_path: str, sheet_name: str = "Shift Data") -> str:
    os.makedirs(os.path.dirname(final_path) or ".", exist_ok=True)
    fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(final_path) or ".", prefix=".tmp_", suffix=".xlsx")
    os.close(fd)

    try:
        dataframe_to_excel_file(df, temp_path, sheet_name=sheet_name)
        os.replace(temp_path, final_path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass

    return final_path



def shift_excel_download_service(
    db: Session,
    emp_id=None,
    client_partner=None,
    start_month=None,
    end_month=None,
    department=None,
    client=None,
    payload=None,
) -> str:
    """Main entry point."""
    if payload:
        default_cache = _is_default_cache_request(db, payload)
    else:
        default_cache = not any([emp_id, client_partner, start_month, end_month, department, client])

    cache_key = f"{LATEST_MONTH_KEY}:excel"
    latest_ym = _get_db_latest_ym(db)

    if default_cache:
        cached = cache.get(cache_key)
        if cached and cached.get("file_path") and cached.get("_cached_month") == latest_ym:
            if os.path.exists(cached["file_path"]):
                return cached["file_path"]

    df = export_filtered_excel_df(
        db=db,
        emp_id=emp_id,
        client_partner=client_partner,
        start_month=start_month,
        end_month=end_month,
        department=department,
        client=client,
        payload=payload
    )

    os.makedirs(EXPORT_DIR, exist_ok=True)
    if default_cache:
        file_path = os.path.join(EXPORT_DIR, DEFAULT_EXPORT_FILE)
    else:
        file_path = os.path.join(EXPORT_DIR, f"shift_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")

    _atomic_write_excel(df, file_path)

    if default_cache:
        cache.set(cache_key, {"_cached_month": latest_ym, "file_path": file_path}, expire=CACHE_TTL)

    return file_path