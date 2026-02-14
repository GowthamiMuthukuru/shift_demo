"""
Service for exporting client summary data as an Excel download (fast)
WITH file-path caching (simple, default-latest only).

- Uses Pandas + XlsxWriter for speed on large datasets.
- Excel headers use config display strings (with '\n') and are wrapped in Excel.
- Avoids stale headers by caching a 'shift signature' (keys + labels).
- Cache technique:
    * For default latest-month request -> stable file name cached by filter-aware month key + signature.
    * For non-default requests -> payload-hash based file name, separately cached.
"""

from __future__ import annotations

import os
import tempfile
import json
import hashlib
from typing import Dict, List, Any, Tuple, Optional, Set

import pandas as pd
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from diskcache import Cache

from utils.shift_config import get_all_shift_keys, get_shift_string
from services.client_summary_service import (
    client_summary_service,
    is_default_latest_month_request,
    latest_month_cache_key,   
    CACHE_TTL,
)
from models.models import ShiftAllowances

# Disk cache shared with summary service
cache = Cache("./diskcache/latest_month")

EXPORT_DIR = "exports"
DEFAULT_EXPORT_FILE = "client_summary_latest.xlsx"


def _shift_header(key: str) -> str:
    """Excel header label for shift key using config (may include '\n')."""
    return get_shift_string(key) or key


def _money(v: Any) -> float:
    """Coerce to float for numeric excel formatting."""
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _get_db_latest_ym(db: Session) -> Optional[str]:
    """
    Return latest month available in DB as 'YYYY-MM', or None if table is empty.
    Note: This is used only to validate default cache freshness.
    """
    latest_dt = db.query(func.max(ShiftAllowances.duration_month)).scalar()
    return latest_dt.strftime("%Y-%m") if latest_dt else None


def _current_shift_signature() -> Tuple[str, ...]:
    """
    Build a signature that changes if shift keys/labels change.
    Ensures we don't serve an Excel with outdated headers.
    """
    keys = [k.upper().strip() for k in get_all_shift_keys()]
    labels = [get_shift_string(k) or k for k in keys]
    # Signature includes both keys and labels to detect renames/reorders
    return tuple(keys + labels)


def _normalize_multi_str_or_list(value: Any) -> Optional[Set[str]]:
    """
    Normalize payload values that can be 'ALL', string, or list -> set[str].
    - Returns None when no filter should be applied ('ALL', None, []).
    - Keeps original case (exact matching) to avoid accidental mismatches.
    """
    if value is None:
        return None
    if isinstance(value, str):
        if value.strip().upper() == "ALL":
            return None
        parts = [p.strip() for p in value.split(",") if p.strip()]
        return set(parts) if parts else None
    if isinstance(value, list):
        parts = [str(p).strip() for p in value if str(p).strip()]
        if len(parts) == 1 and parts[0].upper() == "ALL":
            return None
        return set(parts) if parts else None
    return None


def _write_excel_to_path(df: pd.DataFrame, currency_cols: List[str], file_path: str) -> None:
    """
    Write DataFrame to a specific path with styles; atomic write is handled by the caller.
    """
    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

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

        # Write header explicitly to apply header format
        for c, col_name in enumerate(df.columns):
            ws.write(0, c, col_name, header_fmt)

        ws.set_row(0, 60)
        ws.freeze_panes(1, 0)

        currency_set = set(currency_cols)

        # Autosize columns with basic heuristics
        for c, col_name in enumerate(df.columns):
            lines = str(col_name).split("\n")
            longest = max((len(x) for x in lines), default=len(str(col_name)))
            width = min(max(longest + 2, 12), 45)
            if col_name in ("Client", "Client Partner", "Department"):
                width = max(width, 18)
            fmt = inr_fmt if col_name in currency_set else center_fmt
            ws.set_column(c, c, width, fmt)


def _atomic_write_excel(df: pd.DataFrame, currency_cols: List[str], final_path: str) -> str:
    """
    Write to a temp file and atomically move into place.
    """
    os.makedirs(os.path.dirname(final_path) or ".", exist_ok=True)
    fd, temp_path = tempfile.mkstemp(
        dir=os.path.dirname(final_path) or ".",
        prefix=".tmp_",
        suffix=".xlsx"
    )
    os.close(fd)
    try:
        _write_excel_to_path(df, currency_cols, temp_path)
        os.replace(temp_path, final_path)  # atomic on same filesystem
        return final_path
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass


def _build_dataframe_from_summary(
    summary_data: Dict[str, Any],
    emp_ids_filter: Optional[Set[str]],
    partner_filter: Optional[Set[str]],
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Build the export DataFrame and return (df, shift_cols).
    Filters are applied at row-build time:
      - emp_ids_filter: optional set of employee IDs
      - partner_filter: optional set of client partner names
    """
    shift_keys = [k.upper().strip() for k in get_all_shift_keys()]
    shift_cols = [get_shift_string(k) or k for k in shift_keys]

    rows: List[Dict[str, Any]] = []

    # Iterate periods in ascending order
    for period_key in sorted(summary_data):
        period_data = summary_data[period_key]
        clients = period_data.get("clients")
        if not clients:
            continue

        # clients is a dict keyed by client_name -> client_block
        for client_name, client_block in clients.items():
            partner_value = client_block.get("client_partner", "")
            departments = client_block.get("departments", {})

            for dept_name, dept_block in departments.items():
                employees = dept_block.get("employees", [])

                # If no employees (unlikely but supported), write a dept-only row
                if not employees:
                    if partner_filter and partner_value not in partner_filter:
                        continue

                    row = {
                        "Period": period_key,
                        "Client": client_name,
                        "Client Partner": partner_value,
                        "Employee ID": "",
                        "Department": dept_name,
                        "Head Count": int(dept_block.get("dept_head_count", 0) or 0),
                    }
                    # FIX: read dept totals from plain keys (not "dept_{k}")
                    for k, col in zip(shift_keys, shift_cols):
                        row[col] = _money(dept_block.get(k, 0))
                    row["Total Allowance"] = _money(dept_block.get("dept_total", 0))
                    rows.append(row)
                    continue

                # Employee-level rows
                for emp in employees:
                    emp_id_val = emp.get("emp_id", "")
                    if emp_ids_filter and emp_id_val not in emp_ids_filter:
                        continue

                    emp_partner = emp.get("client_partner", partner_value)
                    if partner_filter and emp_partner not in partner_filter:
                        continue

                    row = {
                        "Period": period_key,
                        "Client": client_name,
                        "Client Partner": emp_partner,
                        "Employee ID": emp_id_val,
                        "Department": dept_name,
                        "Head Count": 1,
                    }
                    # FIX: fallback to dept shift totals using plain keys
                    for k, col in zip(shift_keys, shift_cols):
                        row[col] = _money(emp.get(k, dept_block.get(k, 0)))
                    row["Total Allowance"] = _money(emp.get("total", dept_block.get("dept_total", 0)))
                    rows.append(row)

    if not rows:
        raise HTTPException(404, "No data available for export")

    df = pd.DataFrame(rows)
    # Sort period chronologically
    df["Period"] = pd.to_datetime(df["Period"], format="%Y-%m", errors="coerce")
    df = df.sort_values(by=["Period", "Client", "Department", "Employee ID"])
    df["Period"] = df["Period"].dt.strftime("%Y-%m")

    ordered_cols = (
        ["Period", "Client", "Client Partner", "Employee ID", "Department", "Head Count"]
    ) + shift_cols + ["Total Allowance"]
    # Keep only columns that exist (defensive)
    df = df[[c for c in ordered_cols if c in df.columns]]

    return df, shift_cols


def _payload_hash(payload: dict) -> str:
    """Stable hash for non-default payloads."""
    j = json.dumps(payload or {}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(j.encode("utf-8")).hexdigest()


def _stable_cache_key(payload: dict, default_req: bool, shift_sig: Optional[Tuple[str, ...]]) -> str:
    """
    For default requests, use a fixed key so the file name stays stable,
    based on filter-aware latest-month cache key; for non-default, use a stable hash.
    """
    if default_req:
        # Use filter-aware key so different clients/departments/shifts don't collide
        return f"{latest_month_cache_key(payload)}:excel"
    return f"client_summary:{_payload_hash(payload)}.xlsx"


def client_summary_download_service(db: Session, payload: dict) -> str:
    """
    Generate and export client summary Excel with a simplified caching strategy.

    NOTE:
    - Filtering logic is delegated to `client_summary_service(db, payload)`.
    - This service focuses on: caching, Excel rendering, and atomic writes.
    """
    payload = (payload or {})
    default_req = is_default_latest_month_request(payload)

    # For default requests, we validate cache freshness against the DB latest month
    latest_ym = _get_db_latest_ym(db) if default_req else None
    shift_sig = _current_shift_signature() if default_req else None

    cache_key = _stable_cache_key(payload, default_req, shift_sig)
    final_default_path = os.path.join(EXPORT_DIR, DEFAULT_EXPORT_FILE)

    # Try cache first
    cached = cache.get(cache_key)
    if cached:
        cached_path = cached.get("file_path")
        if default_req:
            cached_month = cached.get("_cached_month")
            cached_signature = cached.get("shift_sig")
            if (
                cached_path
                and os.path.exists(cached_path)
                and cached_signature == shift_sig
                and (
                    (latest_ym is None and cached_month is None)
                    or (latest_ym is not None and cached_month == latest_ym)
                )
            ):
                return cached_path
        else:
            if cached_path and os.path.exists(cached_path):
                return cached_path

    # Build summary data
    summary_data = client_summary_service(db, payload)
    if not summary_data:
        raise HTTPException(404, "No data available")

    # Optional row-level filters
    emp_ids_filter = _normalize_multi_str_or_list(payload.get("emp_id"))
    partner_filter = _normalize_multi_str_or_list(payload.get("client_partner"))

    df, shift_cols = _build_dataframe_from_summary(summary_data, emp_ids_filter, partner_filter)
    currency_cols = shift_cols + ["Total Allowance"]

    # Default requests -> stable file name
    if default_req:
        written_path = _atomic_write_excel(df, currency_cols, final_default_path)
        cache.set(
            cache_key,
            {
                "_cached_month": latest_ym,
                "file_path": written_path,
                "shift_sig": shift_sig,
            },
            expire=CACHE_TTL,
        )
        return written_path

    # Non-default -> hashed filename
    hashed_name = cache_key.split(":", 1)[-1]
    if not hashed_name.endswith(".xlsx"):
        hashed_name += ".xlsx"
    path = os.path.join(EXPORT_DIR, hashed_name)

    written_path = _atomic_write_excel(df, currency_cols, path)
    cache.set(
        cache_key,
        {"file_path": written_path},
        expire=CACHE_TTL,
    )
    return written_path