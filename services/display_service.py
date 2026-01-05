from fastapi import HTTPException
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import extract, func
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from datetime import datetime,date
from typing import Optional
import pandas as pd
from io import BytesIO
from fastapi.responses import StreamingResponse
from utils.client_enums import Company
from calendar import monthrange

def _load_shift_rates(db: Session) -> dict:
    """Return dict like {'A': 300.0, 'B': 350.0, ...}"""
    rows = db.query(ShiftsAmount).all()
    rates = {}
    for r in rows:
        if not r.shift_type:
            continue
        rates[r.shift_type.upper()] = float(r.amount)
    return rates

def _recalculate_all_mappings(db: Session):
    """Recalculate total_allowance for ALL shift_mapping rows."""
    rates = _load_shift_rates(db)

    rows = db.query(ShiftMapping).all()
    for row in rows:
        days = float(row.days or 0)
        rate = rates.get(row.shift_type.upper(), 0.0)
        row.total_allowance = days * rate

    db.commit()


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

        shift_details = {}
        total_allowance = 0.0

        for m in mappings:
            days = float(m.days or 0)
            rate = rates.get(m.shift_type.upper(), 0.0)
            m.total_allowance = days * rate
            total_allowance += m.total_allowance

            if days > 0:
                shift_details[m.shift_type.upper()] = days

        db.commit()

        client_name = rec.client
        abbr = next((c.name for c in Company if c.value == client_name), None)
        if abbr:
            client_name = abbr

        result.append({
            "id": rec.id,
            "emp_id": rec.emp_id,
            "emp_name": rec.emp_name,
            "department": rec.department,
            "payroll_month": rec.payroll_month.strftime("%Y-%m") if rec.payroll_month else None,
            "client": client_name,
            "account_manager": rec.account_manager,
            "duration_month": rec.duration_month.strftime("%Y-%m") if rec.duration_month else None,
            "total_allowance": float(total_allowance),
            "shift_details": shift_details
        })

    return selected_month, total_records, result, message


def parse_shift_value(value):
    """Parse and validate shift day input as a non-negative float."""
    if value is None or str(value).strip() == "":
        return 0.0
    raw = str(value).strip()

    if raw in ("-0", "-0.0", "-0.00"):
        raise HTTPException(
            status_code=400,
            detail="Negative zero is not allowed"
        )

    try:
        v = float(value)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid shift value '{value}'. Only numeric allowed."
        )
    if v < 0:
        raise HTTPException(
            status_code=400,
            detail="Negative days not allowed."
        )
    return v


def validate_half_day(value: float, field_name: str):
    """Ensure shift values are in 0.5-day increments."""
    if value is None:
        return

    if value < 0:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} must be non-negative"
        )

    if (value * 2) % 1 != 0:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} must be in 0.5 increments (e.g. 1, 1.5, 7.5)"
        )


def validate_not_future_month(month_date: date, field_name: str):
    """Raise error if the given month lies in the future."""
    today = date.today().replace(day=1)
    if month_date > today:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} cannot be a future month"
        )





def update_shift_service(
    db: Session,
    emp_id: str,
    payroll_month: str,
    updates: dict,
    duration_month: Optional[str] = None
):
    """Update shift days for an employee and recalculate allowances."""
    allowed_fields = ["shift_a", "shift_b", "shift_c", "prime"]
    unknown = [k for k in updates if k not in allowed_fields]
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid fields: {unknown}"
        )

    parsed = {}
    for k, v in updates.items():
        val = parse_shift_value(v)
        validate_half_day(val, k)
        parsed[k] = val

    key_map = {
        "shift_a": "A",
        "shift_b": "B",
        "shift_c": "C",
        "prime": "PRIME"
    }


    mapped_updates = {
        key_map[k]: (parsed[k] if parsed[k] is not None else 0.0)
        for k in parsed
    }



    try:
        payroll_dt = datetime.strptime(payroll_month, "%Y-%m").date().replace(day=1)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid payroll_month format. Use YYYY-MM"
        )

    if not duration_month:
        raise HTTPException(
            status_code=400,
            detail="duration_month is required"
        )

    try:
        duration_dt = datetime.strptime(duration_month, "%Y-%m").date().replace(day=1)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid duration_month format. Use YYYY-MM"
        )


    validate_not_future_month(duration_dt, "duration_month")
    validate_not_future_month(payroll_dt, "payroll_month")

    if duration_month == payroll_month:
        raise HTTPException(
            status_code=400,
            detail="duration_month and payroll_month cannot be the same"
        )

    if payroll_dt < duration_dt:
        raise HTTPException(
            status_code=400,
            detail="Payroll month cannot be earlier than duration month"
        )


    max_days_in_month = monthrange(duration_dt.year, duration_dt.month)[1]

    if sum(mapped_updates.values()) > max_days_in_month:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Total days ({sum(mapped_updates.values())}) cannot exceed "
                f"{max_days_in_month} days of duration month."
            )
        )


    q = db.query(ShiftAllowances).filter(
        ShiftAllowances.emp_id == emp_id,
        extract("year", ShiftAllowances.duration_month) == duration_dt.year,
        extract("month", ShiftAllowances.duration_month) == duration_dt.month
    )

    rec = q.first()
    if not rec:
        raise HTTPException(
            status_code=404,
            detail=f"No shift record found for employee {emp_id}"
        )


    rates = _load_shift_rates(db)
    existing = {m.shift_type.upper(): m for m in rec.shift_mappings or []}

    for stype, days in mapped_updates.items():
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

        rate = rates.get(stype, 0.0)
        mapping.total_allowance = float(days) * rate
    rec.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(rec)


    total_days = 0.0
    total_allowance = 0.0
    details = []

    for m in rec.shift_mappings:
        days = float(m.days or 0)
        total_days += days

        if total_days > max_days_in_month:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Total assigned days ({total_days}) exceed "
                    f"the duration month limit ({max_days_in_month})."
                )
            )

        rate = rates.get(m.shift_type.upper(), 0.0)
        m.total_allowance = float(days) * rate
        total_allowance += m.total_allowance

        details.append({
            "shift": m.shift_type.upper(),
            "days": days,
            "total": float(m.total_allowance)
        })

    db.commit()

    return {
        "message": "Shift updated successfully",
        "updated_fields": list(mapped_updates.keys()),
        "total_days": float(total_days),
        "total_allowance": float(total_allowance),
        "shift_details": details
    }


def fetch_shift_record(emp_id: str, duration_month: str, payroll_month: str, db: Session):
    """Fetch a single employee shift record with allowance breakdown."""
    try:
        duration_dt = datetime.strptime(duration_month + "-01", "%Y-%m-%d").date()
        payroll_dt = datetime.strptime(payroll_month + "-01", "%Y-%m-%d").date()
    except:
        raise HTTPException(status_code=400, detail="Invalid month format. Expected YYYY-MM")

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

    total_allowance = 0.0
    breakdown = {"A": 0.0, "B": 0.0, "C": 0.0, "PRIME": 0.0}
    for m in rec.shift_mappings:
        days = float(m.days or 0)
        rate = rates.get(m.shift_type.upper(), 0.0)
        m.total_allowance = days * rate
        total_allowance += m.total_allowance
        breakdown[m.shift_type.upper()] = days

    db.commit()

    out = {
        "id": rec.id,
        "emp_id": rec.emp_id,
        "emp_name": rec.emp_name,
        "grade": rec.grade,
        "department": rec.department,
        "client": next((c.name for c in Company if c.value == rec.client), rec.client),
        "project": rec.project,
        "project_code": rec.project_code,
        "account_manager": rec.account_manager,
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

def generate_employee_shift_excel(emp_id: str,
                                  duration_month: str,
                                  payroll_month: str, db: Session):
    """Generate and stream an Excel file for an employee shift record."""
    rec = fetch_shift_record(emp_id, duration_month, payroll_month, db)

    if rec.get("duration_month"):
        rec["duration_month"] = datetime.strptime(rec["duration_month"], "%Y-%m").strftime("%b'%y")
    if rec.get("payroll_month"):
        rec["payroll_month"] = datetime.strptime(rec["payroll_month"], "%Y-%m").strftime("%b'%y")

    df = pd.DataFrame([rec])

    columns = [
        "id", "emp_id", "emp_name", "grade", "department", "client",
        "project", "project_code", "account_manager", "practice_lead",
        "delivery_manager", "duration_month", "payroll_month",
        "billability_status", "practice_remarks", "rmg_comments",
        "created_at", "updated_at", "total_allowance", "A", "B", "C", "PRIME"
    ]

    for c in columns:
        if c not in df.columns:
            df[c] = None

    df = df[columns]


    def format_inr(v):
        try:
            formatted = f"₹ {float(v):,.2f}"
            return formatted.replace("'", "")
        except:
            return v

    df["total_allowance"] = df["total_allowance"].apply(format_inr)


    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Shift Details")

    output.seek(0)
    filename = f"{emp_id}_{duration_month}_{payroll_month}_shift_details.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
