# services/client_summary_download_service.py
"""
Service for exporting client summary data as an Excel download (fast)
WITH file-path caching (simple, default-latest only).

- Uses Pandas + XlsxWriter for speed on large datasets.
- Excel headers use config display strings (with '\n') and are wrapped in Excel.
- Avoids stale headers by caching a 'shift signature' (keys + labels).
- Cache technique:
    * For default latest-month request -> stable file name cached by filter-aware month key + signature.
    * For non-default requests -> payload-hash based file name, separately cached.

CHANGES:
- Excel now focuses on EMPLOYEE rows only (department-only rows are skipped).
- Keeps "Head Count" in DataFrame for headcount range filtering and drops it only in Excel output.
- Adds Excel AutoFilter on headers for easy filtering (e.g., by Employee Name).
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import os
import tempfile
import json
import hashlib
from typing import Dict, List, Any, Tuple, Optional, Set, Union

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

cache = Cache("./diskcache/latest_month")
EXPORT_DIR = "exports"
DEFAULT_EXPORT_FILE = "client_summary_latest.xlsx"


def _shift_header(key: str) -> str:
    return get_shift_string(key) or key


def _money(v: Any) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _get_db_latest_ym(db: Session) -> Optional[str]:
    latest_dt = db.query(func.max(ShiftAllowances.duration_month)).scalar()
    return latest_dt.strftime("%Y-%m") if latest_dt else None


def _current_shift_signature() -> Tuple[str, ...]:
    keys = [k.upper().strip() for k in get_all_shift_keys()]
    labels = [get_shift_string(k) or k for k in keys]
    return tuple(keys + labels)


def _normalize_multi_str_or_list(value: Any) -> Optional[Set[str]]:
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


def _write_excel_to_path(
    df: pd.DataFrame,
    currency_cols: List[str],
    file_path: str,
    notes_lines: Optional[List[str]] = None,
) -> None:
    """
    Writes the DataFrame to Excel using XlsxWriter.
    Drops the internal "Head Count" column for presentation only.
    Adds AutoFilter to the header row for easy filtering.
    """

    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

    # Presentation-only DataFrame: hide "Head Count" in Excel
    df_to_export = df.drop(columns=["Head Count"], errors="ignore") if df is not None else df

    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        workbook = writer.book

        if df_to_export is not None and not df_to_export.empty:
            sheet_name = "Client Summary"
            df_to_export.to_excel(writer, index=False, sheet_name=sheet_name)
            ws = writer.sheets[sheet_name]

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

            # Header styling
            for c, col_name in enumerate(df_to_export.columns):
                ws.write(0, c, col_name, header_fmt)

            # Header height + freeze header
            ws.set_row(0, 60)
            ws.freeze_panes(1, 0)

            # AutoFilter across entire used range (header row to last row/col)
            last_row = len(df_to_export)  # data rows count
            last_col = len(df_to_export.columns) - 1
            ws.autofilter(0, 0, last_row, last_col)

            # Column widths & formats
            currency_set = set(currency_cols)

            for c, col_name in enumerate(df_to_export.columns):
                lines = str(col_name).split("\n")
                longest = max((len(x) for x in lines), default=len(str(col_name)))
                width = min(max(longest + 2, 12), 45)

                if col_name in ("Client", "Client Partner", "Department", "Employee Name"):
                    width = max(width, 18)

                fmt = inr_fmt if col_name in currency_set else center_fmt
                ws.set_column(c, c, width, fmt)

        if notes_lines:
            ws_notes = workbook.add_worksheet("Notes")
            text_fmt = workbook.add_format({
                "text_wrap": True,
                "align": "left",
                "valign": "top",
            })
            ws_notes.set_column(0, 0, 120)
            for idx, line in enumerate(notes_lines):
                ws_notes.write(idx, 0, line, text_fmt)


def _atomic_write_excel(
    df: pd.DataFrame,
    currency_cols: List[str],
    final_path: str,
    notes_lines: Optional[List[str]] = None,
) -> str:

    os.makedirs(os.path.dirname(final_path) or ".", exist_ok=True)
    fd, temp_path = tempfile.mkstemp(
        dir=os.path.dirname(final_path) or ".",
        prefix=".tmp_",
        suffix=".xlsx"
    )
    os.close(fd)

    try:
        _write_excel_to_path(df, currency_cols, temp_path, notes_lines)
        os.replace(temp_path, final_path)
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
    Build a flat DataFrame with ONLY employee-level rows so 'Employee Name' is always present.
    Department-only rows (with no employees) are intentionally skipped to make the Excel
    output name-centric (as requested).
    """

    shift_keys = [k.upper().strip() for k in get_all_shift_keys()]
    shift_cols = [get_shift_string(k) or k for k in shift_keys]

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
                        "Employee Name": emp.get("emp_name", ""),
                        "Employee ID": emp_id_val,
                        "Department": dept_name,
                       
                        "Head Count": 1,
                    }

                    
                    for k, col in zip(shift_keys, shift_cols):
                        row[col] = _money(emp.get(k, dept_block.get(k, 0)))

                    row["Total Allowance"] = _money(
                        emp.get("total", dept_block.get("dept_total", 0))
                    )

                    rows.append(row)

    df = pd.DataFrame(rows)

    if not df.empty:
        df["Period"] = pd.to_datetime(df["Period"], format="%Y-%m", errors="coerce")
        df = df.sort_values(by=["Period", "Client", "Department", "Employee Name"])
        df["Period"] = df["Period"].dt.strftime("%Y-%m")

    ordered_cols = (
        [
            "Period",
            "Client",
            "Client Partner",
            "Employee Name",
            "Employee ID",
            "Department",
            "Head Count",
        ]
        + shift_cols
        + ["Total Allowance"]
    )

    if not df.empty:
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


def _requested_periods_from_payload(payload: dict) -> List[str]:
    """
    Build the list of requested period strings ('YYYY-MM') from the payload:
    - If both years and months provided -> Cartesian product.
    - If only years -> all 12 months for those years.
    - If only months -> months in current year.
    - If neither -> return empty list (means default/latest mode).
    Assumes months/years already normalized to ascending and deduped.
    """
    years = payload.get("years") or []
    months = payload.get("months") or []
    periods: List[str] = []

    if years and months:
        for y in years:
            for m in months:
                periods.append(f"{int(y):04d}-{int(m):02d}")
    elif years:
        for y in years:
            for m in range(1, 13):
                periods.append(f"{int(y):04d}-{m:02d}")
    elif months:
        current_year = pd.Timestamp.today().year
        for m in months:
            periods.append(f"{int(current_year):04d}-{int(m):02d}")

    return periods


def _parse_headcount_range_str(value: Optional[Union[str, int]]) -> Optional[Tuple[int, int]]:
    """
    Accepts:
      - None / "ALL" -> None
      - "N" -> (N, N)
      - "N-M" -> (N, M)
    Validates positive integers and min <= max. Supports unicode dashes.
    """
    if value is None:
        return None
    s = str(value).strip()
    if s.upper() == "ALL":
        return None

    # normalize unicode dashes
    s = s.replace("–", "-").replace("—", "-").replace("−", "-")

    if "-" in s:
        lo_str, hi_str = [x.strip() for x in s.split("-", 1)]
        if not lo_str.isdigit() or not hi_str.isdigit():
            raise HTTPException(status_code=400, detail=f"Invalid headcount range: '{value}'")
        lo, hi = int(lo_str), int(hi_str)
        if lo <= 0 or hi <= 0 or lo > hi:
            raise HTTPException(status_code=400, detail=f"Invalid headcount range: '{value}'")
        return (lo, hi)

    # single number
    if not s.isdigit():
        raise HTTPException(status_code=400, detail=f"Invalid headcount value: '{value}'")
    n = int(s)
    if n <= 0:
        raise HTTPException(status_code=400, detail=f"Invalid headcount value: '{value}'")
    return (n, n)


def _apply_headcount_filter(df: pd.DataFrame, headcount_range: Optional[Tuple[int, int]]) -> pd.DataFrame:
    """
    Filters rows by department headcount within the given range.
    Headcount is computed per (Period, Client, Client Partner, Department) as the sum of 'Head Count'.
    Works when DataFrame contains only employee rows (Head Count=1 per row).
    """
    if headcount_range is None or df.empty:
        return df

    lo, hi = headcount_range
    keys = ["Period", "Client", "Client Partner", "Department"]

    # Ensure required columns exist gracefully
    missing_keys = [k for k in keys if k not in df.columns]
    if missing_keys:
        # If structure isn't as expected, skip filtering instead of crashing
        return df

    # compute dept headcount
    if "Head Count" not in df.columns:
        # Shouldn't be needed, but be defensive
        df = df.copy()
        df["Head Count"] = 1

    grp = df.groupby(keys, as_index=False)["Head Count"].sum().rename(columns={"Head Count": "_DeptHeadCount"})
    df2 = df.merge(grp, on=keys, how="left")
    mask = (df2["_DeptHeadCount"] >= lo) & (df2["_DeptHeadCount"] <= hi)
    df2 = df2[mask].drop(columns=["_DeptHeadCount"])
    return df2

def _parse_allowance_ranges(allowance):
    """
    Accepts: None | "ALL" -> None
             "min-max"
             ["a-b","c-d"] (OR semantics)
    Returns: list of (float(min), float(max)) or None.
    """
    if allowance in (None, "ALL"):
        return None

    items = allowance if isinstance(allowance, list) else [allowance]
    ranges = []
    for item in items:
        s = str(item or "").strip()
        if "-" not in s:
            raise HTTPException(400, f"Invalid allowance range: {item}. Use 'min-max' like '1000-5000'.")
        parts = s.split("-")
        if len(parts) != 2:
            raise HTTPException(400, f"Invalid allowance range: {item}. Use 'min-max' like '1000-5000'.")
        try:
            lo = float(Decimal(parts[0].strip()))
            hi = float(Decimal(parts[1].strip()))
        except (InvalidOperation, ValueError):
            raise HTTPException(400, f"Invalid allowance numbers in range: {item}.")
        if lo > hi:
            raise HTTPException(400, f"Invalid allowance range (start > end): {item}.")
        ranges.append((lo, hi))
    return ranges or None


def _allowance_in_ranges(value: float, ranges) -> bool:
    if not ranges:
        return True
    try:
        v = float(value if value is not None else 0.0)
    except Exception:
        return False
    return any(lo <= v <= hi for (lo, hi) in ranges)

def client_summary_download_service(db: Session, payload: dict) -> str:
    """
    Generate and export client summary Excel with a simplified caching strategy.

    Enhancements:
    - Sorting ONLY for months/years (ascending).
    - If some requested periods have no data, we add a 'Notes' sheet listing them.
    - If no data at all for requested selection, we write a Notes-only Excel instead of raising.
    - Headcount range filter (string "N" or "N-M") applied at DataFrame level.
    - EMPLOYEE-ONLY rows in Excel (department-only totals are skipped by design).
    - NEW: Allowance filter at CLIENT level applied at DataFrame level (inclusive ranges).
    """
    payload = payload or {}

   
    default_req = is_default_latest_month_request(payload)

    
    if isinstance(payload.get("months"), list) and payload["months"]:
        payload["months"] = sorted({int(m) for m in payload["months"]})
    if isinstance(payload.get("years"), list) and payload["years"]:
        payload["years"] = sorted({int(y) for y in payload["years"]})

    requested_periods = _requested_periods_from_payload(payload)

    latest_ym = _get_db_latest_ym(db) if default_req else None
    shift_sig = _current_shift_signature() if default_req else None

    cache_key = _stable_cache_key(payload, default_req, shift_sig) 
    final_default_path = os.path.join(EXPORT_DIR, DEFAULT_EXPORT_FILE)
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

    notes_lines: List[str] = []
    try:
        summary_data = client_summary_service(db, payload)
    except HTTPException as e:
        if e.status_code == 404 and (payload.get("months") or payload.get("years")):
            notes_lines.append("No data present for the selected month(s)/year(s).")
            empty_df = pd.DataFrame()
            if default_req:
                written_path = _atomic_write_excel(
                    empty_df, [], final_default_path, notes_lines=notes_lines
                )
                cache.set(
                    cache_key,
                    {"_cached_month": latest_ym, "file_path": written_path, "shift_sig": shift_sig},
                    expire=CACHE_TTL,
                )
                return written_path
            else:
                hashed_name = cache_key.split(":", 1)[-1]
                if not hashed_name.endswith(".xlsx"):
                    hashed_name += ".xlsx"
                path = os.path.join(EXPORT_DIR, hashed_name)
                written_path = _atomic_write_excel(
                    empty_df, [], path, notes_lines=notes_lines
                )
                cache.set(cache_key, {"file_path": written_path}, expire=CACHE_TTL)
                return written_path
        raise

   
    emp_ids_filter = _normalize_multi_str_or_list(payload.get("emp_id"))
    partner_filter = _normalize_multi_str_or_list(payload.get("client_partner"))

 
    df, shift_cols = _build_dataframe_from_summary(
        summary_data,
        emp_ids_filter,
        partner_filter,
    )
    currency_cols = shift_cols + ["Total Allowance"]

    headcount_range = _parse_headcount_range_str(payload.get("headcounts"))
    df = _apply_headcount_filter(df, headcount_range)


    allowance_ranges = _parse_allowance_ranges(payload.get("allowance"))
    if allowance_ranges:

        client_col = next((c for c in df.columns if str(c).strip().lower() == "client"), None)
        if not client_col:
          
            raise HTTPException(status_code=400, detail="Cannot apply allowance filter: 'Client' column not found in export DataFrame.")

       
        if "Total Allowance" in df.columns:
            df["Total Allowance"] = pd.to_numeric(df["Total Allowance"], errors="coerce").fillna(0.0)
        else:
            raise HTTPException(status_code=400, detail="Cannot apply allowance filter: 'Total Allowance' column not found in export DataFrame.")

        client_totals = (
            df.groupby(client_col, as_index=False)["Total Allowance"]
              .sum()
              .rename(columns={"Total Allowance": "_client_total"})
        )

        df = df.merge(client_totals, on=client_col, how="left")
        mask = df["_client_total"].apply(lambda v: _allowance_in_ranges(v, allowance_ranges))
        df = df[mask].drop(columns=["_client_total"])

    if requested_periods:
        present_periods = set(summary_data.keys())
        missing_periods = [p for p in requested_periods if p not in present_periods]
        if missing_periods:
            pretty_missing = ", ".join(missing_periods)
            notes_lines.append(
                f"No data present for the following selected period(s): {pretty_missing}"
            )

    if df is None or df.empty:
        if not notes_lines:
            notes_lines.append("No data present.")
        if default_req:
            written_path = _atomic_write_excel(
                pd.DataFrame(), [], final_default_path, notes_lines=notes_lines
            )
            cache.set(
                cache_key,
                {"_cached_month": latest_ym, "file_path": written_path, "shift_sig": shift_sig},
                expire=CACHE_TTL,
            )
            return written_path
        else:
            hashed_name = cache_key.split(":", 1)[-1]
            if not hashed_name.endswith(".xlsx"):
                hashed_name += ".xlsx"
            path = os.path.join(EXPORT_DIR, hashed_name)
            written_path = _atomic_write_excel(
                pd.DataFrame(), [], path, notes_lines=notes_lines
            )
            cache.set(cache_key, {"file_path": written_path}, expire=CACHE_TTL)
            return written_path

    # Write Excel and cache
    if default_req:
        written_path = _atomic_write_excel(
            df, currency_cols, final_default_path, notes_lines=notes_lines
        )
        cache.set(
            cache_key,
            {"_cached_month": latest_ym, "file_path": written_path, "shift_sig": shift_sig},
            expire=CACHE_TTL,
        )
        return written_path

    hashed_name = cache_key.split(":", 1)[-1]
    if not hashed_name.endswith(".xlsx"):
        hashed_name += ".xlsx"
    path = os.path.join(EXPORT_DIR, hashed_name)
    written_path = _atomic_write_excel(
        df, currency_cols, path, notes_lines=notes_lines
    )
    cache.set(cache_key, {"file_path": written_path}, expire=CACHE_TTL)
    return written_path