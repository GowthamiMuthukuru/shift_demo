from __future__ import annotations

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import extract, func

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from utils.client_enums import Company
from utils.shift_config import get_all_shift_keys, get_shift_string

from datetime import datetime, date
from typing import Optional, Dict, Union, Any
from io import BytesIO
from calendar import monthrange
from diskcache import Cache

import pandas as pd


cache = Cache("./diskcache/latest_month")
LATEST_MONTH_KEY = "client_summary:latest_month"


def is_latest_month(db: Session, duration_dt: date) -> bool:
    latest_month = db.query(func.max(ShiftAllowances.duration_month)).scalar()
    if not latest_month:
        return False
    return latest_month.year == duration_dt.year and latest_month.month == duration_dt.month


def _load_shift_rates(db: Session) -> Dict[str, float]:
    """
    Load shift rates from ShiftsAmount table.
    Returns dict like {'PST_MST': 700.0, 'ANZ': 900.0, ...}
    """
    rows = db.query(ShiftsAmount).all()
    rates: Dict[str, float] = {}
    for r in rows:
        if not r.shift_type:
            continue
        rates[(r.shift_type or "").upper().strip()] = float(r.amount or 0.0)
    return rates


def _recalculate_all_mappings(db: Session) -> None:
    """
    Recalculate total_allowance for ALL shift_mapping rows.

     Note: This can be heavy on large data. Keeping as-is as per your file.
    """
    rates = _load_shift_rates(db)

    rows = db.query(ShiftMapping).all()
    for row in rows:
        days = float(row.days or 0.0)
        stype = (row.shift_type or "").upper().strip()
        rate = float(rates.get(stype, 0.0))
        row.total_allowance = days * rate

    db.commit()


def _build_shift_display_map() -> Dict[str, str]:
    """
    SHIFT_KEY -> SHIFT_DISPLAY_STRING (for Excel headers only)
    Example:
        PST_MST -> "PST/MST\n(07 PM - 06 AM)\nINR 700"
    """
    keys = [k.upper().strip() for k in get_all_shift_keys()]
    return {k: (get_shift_string(k) or k) for k in keys}


def fetch_shift_data(db: Session, start: int, limit: int):
    """Fetch paginated shift records for the latest available duration month."""
    current_month = datetime.now().strftime("%Y-%m")

    has_current = (
        db.query(ShiftAllowances)
        .filter(func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == current_month)
        .first()
    )

    if has_current:
        selected_month = current_month
        message = None
    else:
        latest = (
            db.query(func.to_char(ShiftAllowances.duration_month, "YYYY-MM"))
            .order_by(func.to_char(ShiftAllowances.duration_month, "YYYY-MM").desc())
            .first()
        )
        if not latest:
            raise HTTPException(status_code=404, detail="No shift data is available.")
        selected_month = latest[0]
        message = f"No data found for current month {current_month}"

    rates = _load_shift_rates(db)

    _recalculate_all_mappings(db)

    base_q = (
        db.query(ShiftAllowances)
        .options(joinedload(ShiftAllowances.shift_mappings))
        .filter(func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == selected_month)
    )

    total_records = base_q.count()
    records = base_q.order_by(ShiftAllowances.id.asc()).offset(start).limit(limit).all()

    result = []
    for rec in records:
        mappings = rec.shift_mappings or []

        shift_details: Dict[str, float] = {}
        total_allowance = 0.0

        for m in mappings:
            days = float(m.days or 0.0)
            stype = (m.shift_type or "").upper().strip()
            rate = float(rates.get(stype, 0.0))
            m.total_allowance = days * rate
            total_allowance += m.total_allowance

            if days > 0:
                shift_details[stype] = days

        db.commit() 

        client_name = rec.client
        abbr = next((c.name for c in Company if c.value == client_name), None)
        if abbr:
            client_name = abbr

        client_partner_val = getattr(rec, "client_partner", None) or getattr(rec, "account_manager", None)

        result.append({
            "id": rec.id,
            "emp_id": rec.emp_id,
            "emp_name": rec.emp_name,
            "department": rec.department,
            "payroll_month": rec.payroll_month.strftime("%Y-%m") if rec.payroll_month else None,
            "client": client_name,
            "client_partner": client_partner_val,
            "duration_month": rec.duration_month.strftime("%Y-%m") if rec.duration_month else None,
            "total_allowance": float(total_allowance),
            "shift_details": shift_details
        })

    return selected_month, total_records, result, message

def parse_shift_value(value: Any) -> float:
    """Parse and validate shift day input as a non-negative float."""
    if value is None or str(value).strip() == "":
        return 0.0

    raw = str(value).strip()
    if raw in ("-0", "-0.0", "-0.00"):
        raise HTTPException(status_code=400, detail="Negative zero is not allowed")

    try:
        v = float(value)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid shift value '{value}'. Only numeric allowed."
        ) from exc

    if v < 0:
        raise HTTPException(status_code=400, detail="Negative days not allowed.")
    return v


def validate_half_day(value: float, field_name: str):
    """Ensure shift values are in 0.5-day increments."""
    if value is None:
        return

    if value < 0:
        raise HTTPException(status_code=400, detail=f"{field_name} must be non-negative")

    
    if (value * 2) % 1 != 0:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} must be in 0.5 increments (e.g. 1, 1.5, 7.5)"
        )


def validate_not_future_month(month_date: date, field_name: str):
    """Raise error if the given month lies in the future."""
    today = date.today().replace(day=1)
    if month_date > today:
        raise HTTPException(status_code=400, detail=f"{field_name} cannot be a future month")



def update_shift_service(
    db: Session,
    emp_id: str,
    payroll_month: str,
    updates: Dict[str, Union[int, float, str]],  
    duration_month: Optional[str] = None
):
    """
    Update shift days for an employee and recalculate allowances.

     Now accepts dynamic shift keys from config:
        updates = {"PST_MST": 2, "ANZ": 1.5}

    Typically called using request payload:
        { "shifts": { ... } }
      -> updates = payload.shifts
    """

 
    valid_keys = {k.upper().strip() for k in get_all_shift_keys()}

  
    incoming_keys = [(k or "").upper().strip() for k in updates.keys()]
    unknown = [orig for orig in updates.keys() if (orig or "").upper().strip() not in valid_keys]
    if unknown:
        raise HTTPException(status_code=400, detail=f"Invalid shift types: {unknown}")

   
    parsed: Dict[str, float] = {}
    for k, v in updates.items():
        key = (k or "").upper().strip()
        val = parse_shift_value(v)
        validate_half_day(val, key)
        parsed[key] = float(val)

    try:
        payroll_dt = datetime.strptime(payroll_month, "%Y-%m").date().replace(day=1)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid payroll_month format. Use YYYY-MM") from exc

    if not duration_month:
        raise HTTPException(status_code=400, detail="duration_month is required")

    try:
        duration_dt = datetime.strptime(duration_month, "%Y-%m").date().replace(day=1)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid duration_month format. Use YYYY-MM") from exc

    validate_not_future_month(duration_dt, "duration_month")
    validate_not_future_month(payroll_dt, "payroll_month")

    if duration_month == payroll_month:
        raise HTTPException(status_code=400, detail="duration_month and payroll_month cannot be the same")

    if payroll_dt < duration_dt:
        raise HTTPException(status_code=400, detail="Payroll month cannot be earlier than duration month")

    max_days_in_month = monthrange(duration_dt.year, duration_dt.month)[1]
    if sum(parsed.values()) > max_days_in_month:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Total days ({sum(parsed.values())}) cannot exceed "
                f"{max_days_in_month} days of duration month."
            )
        )

   
    rec = (
        db.query(ShiftAllowances)
        .options(joinedload(ShiftAllowances.shift_mappings))
        .filter(
            ShiftAllowances.emp_id == emp_id,
            extract("year", ShiftAllowances.duration_month) == duration_dt.year,
            extract("month", ShiftAllowances.duration_month) == duration_dt.month
        )
        .first()
    )
    if not rec:
        raise HTTPException(status_code=404, detail=f"No shift record found for employee {emp_id}")

    rates = _load_shift_rates(db)

    existing = {(m.shift_type or "").upper().strip(): m for m in (rec.shift_mappings or [])}

    for stype, days in parsed.items():
        if stype in existing:
            mapping = existing[stype]
            mapping.days = days
        else:
            mapping = ShiftMapping(
                shiftallowance_id=rec.id,
                shift_type=stype,
                days=days,
                total_allowance=0.0
            )
            db.add(mapping)
            existing[stype] = mapping

        rate = float(rates.get(stype, 0.0))
        mapping.total_allowance = float(days) * rate

    rec.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(rec)

 
    total_days = 0.0
    total_allowance = 0.0
    details = []

    for m in rec.shift_mappings or []:
        days = float(m.days or 0.0)
        total_days += days

        if total_days > max_days_in_month:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Total assigned days ({total_days}) exceed "
                    f"the duration month limit ({max_days_in_month})."
                )
            )

        stype = (m.shift_type or "").upper().strip()
        rate = float(rates.get(stype, 0.0))
        m.total_allowance = float(days) * rate
        total_allowance += m.total_allowance

        details.append({
            "shift": stype,
            "days": days,
            "total": float(m.total_allowance)
        })

    db.commit()

   
    if is_latest_month(db, duration_dt):
        cache.pop(LATEST_MONTH_KEY, None)

    return {
        "message": "Shift updated successfully",
        "updated_fields": list(parsed.keys()),
        "total_days": float(total_days),
        "total_allowance": float(total_allowance),
        "shift_details": details
    }


def fetch_shift_record(emp_id: str, duration_month: str, payroll_month: str, db: Session):
    """Fetch a single employee shift record with allowance breakdown (dynamic shift keys)."""
    try:
        duration_dt = datetime.strptime(duration_month + "-01", "%Y-%m-%d").date()
        payroll_dt = datetime.strptime(payroll_month + "-01", "%Y-%m-%d").date()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid month format. Expected YYYY-MM") from exc

    rec = (
        db.query(ShiftAllowances)
        .options(joinedload(ShiftAllowances.shift_mappings))
        .filter(
            ShiftAllowances.emp_id == emp_id,
            ShiftAllowances.duration_month == duration_dt,
            ShiftAllowances.payroll_month == payroll_dt
        )
        .first()
    )
    if not rec:
        raise HTTPException(status_code=404, detail="Record not found")

    rates = _load_shift_rates(db)

    shift_keys = [k.upper().strip() for k in get_all_shift_keys()]
    breakdown = {k: 0.0 for k in shift_keys}

    total_allowance = 0.0

    for m in rec.shift_mappings or []:
        stype = (m.shift_type or "").upper().strip()
        days = float(m.days or 0.0)
        rate = float(rates.get(stype, 0.0))

        m.total_allowance = days * rate
        total_allowance += m.total_allowance

        if stype in breakdown:
            breakdown[stype] = days
        else:
           
            breakdown[stype] = days

    db.commit()

    client_partner_val = getattr(rec, "client_partner", None) or getattr(rec, "account_manager", None)

    out = {
        "id": rec.id,
        "emp_id": rec.emp_id,
        "emp_name": rec.emp_name,
        "grade": rec.grade,
        "department": rec.department,
        "client": next((c.name for c in Company if c.value == rec.client), rec.client),
        "project": rec.project,
        "project_code": rec.project_code,
        "client_partner": client_partner_val,
        "practice_lead": rec.practice_lead,
        "delivery_manager": rec.delivery_manager,
        "duration_month": rec.duration_month.strftime("%Y-%m") if rec.duration_month else None,
        "payroll_month": rec.payroll_month.strftime("%Y-%m") if rec.payroll_month else None,
        "billability_status": rec.billability_status,
        "practice_remarks": rec.practice_remarks,
        "rmg_comments": rec.rmg_comments,
        "created_at": rec.created_at.strftime("%Y-%m-%d") if rec.created_at else None,
        "updated_at": rec.updated_at.strftime("%Y-%m-%d") if rec.updated_at else None,
        "total_allowance": float(total_allowance),
        **breakdown
    }

    return out


def generate_employee_shift_excel(emp_id: str, duration_month: str, payroll_month: str, db: Session):
    """
    Generate and stream an Excel file for an employee shift record.

    - Shift columns come from config keys (PST_MST etc.)
    - Excel headers use config display strings:
        "PST/MST\n(07 PM - 06 AM)\nINR 700"
    - All cells centered
    - Multiline headers wrapped + header row height increased
    """
    rec = fetch_shift_record(emp_id, duration_month, payroll_month, db)

    if rec.get("duration_month"):
        rec["duration_month"] = datetime.strptime(rec["duration_month"], "%Y-%m").strftime("%b'%y")
    if rec.get("payroll_month"):
        rec["payroll_month"] = datetime.strptime(rec["payroll_month"], "%Y-%m").strftime("%b'%y")

    shift_keys = [k.upper().strip() for k in get_all_shift_keys()]
    shift_display_map = _build_shift_display_map()

    core_cols = [
        "id", "emp_id", "emp_name", "grade", "department", "client",
        "project", "project_code", "client_partner", "practice_lead",
        "delivery_manager", "duration_month", "payroll_month",
        "billability_status", "practice_remarks", "rmg_comments",
        "created_at", "updated_at", "total_allowance"
    ]

    row = {c: rec.get(c) for c in core_cols}

   
    for k in shift_keys:
        row[shift_display_map.get(k, k)] = rec.get(k, 0.0)

    df = pd.DataFrame([row])

    output = BytesIO()
    sheet_name = "Shift Details"

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)

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

        money_fmt = workbook.add_format({
            "num_format": '₹ #,##0.00',
            "align": "center",
            "valign": "vcenter",
            "border": 1,
        })

   
        for col_idx, col_name in enumerate(df.columns):
            worksheet.write(0, col_idx, col_name, header_fmt)

        worksheet.set_row(0, 60)
        worksheet.freeze_panes(1, 0)

      
        for col_idx, col_name in enumerate(df.columns):
            lines = str(col_name).split("\n")
            longest = max((len(x) for x in lines), default=len(str(col_name)))
            width = min(max(longest + 2, 12), 45)

            if str(col_name) in ("practice_remarks", "rmg_comments"):
                width = 45

            if str(col_name) == "total_allowance":
                worksheet.set_column(col_idx, col_idx, width, money_fmt)
            else:
                worksheet.set_column(col_idx, col_idx, width, cell_fmt)

    output.seek(0)

    filename = f"{emp_id}_{duration_month}_{payroll_month}_shift_data.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )