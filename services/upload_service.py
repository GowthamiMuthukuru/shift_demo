import os
import uuid
import io
import pandas as pd
import re
from datetime import datetime, date
from fastapi import HTTPException
from sqlalchemy.orm import Session
from models.models import (
    UploadedFiles,
    ShiftAllowances,
    ShiftMapping,
    ShiftsAmount,
)
from utils.enums import ExcelColumnMap

from schemas.displayschema import CorrectedRow
from typing import List
import calendar


TEMP_FOLDER = "media/error_excels"
os.makedirs(TEMP_FOLDER, exist_ok=True)

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
    "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

def make_json_safe(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(i) for i in obj]
    return obj


def parse_month_format(value: str):
    if not isinstance(value, str):
        return None
    try:
        m, y = value.split("'")
        return datetime(2000 + int(y), MONTH_MAP[m.title()], 1).date()
    except Exception:
        return None


def load_shift_rates(db: Session) -> dict:
    rates = {}
    for r in db.query(ShiftsAmount).all():
        if r.shift_type:
            rates[r.shift_type.upper()] = float(r.amount or 0)
    return rates


def delete_existing_emp_month(db: Session, emp_id, duration_month, payroll_month):
    existing = (
        db.query(ShiftAllowances)
        .filter(
            ShiftAllowances.emp_id == emp_id,
            ShiftAllowances.duration_month == duration_month,
            ShiftAllowances.payroll_month == payroll_month,
        )
        .all()
    )

    for rec in existing:
        db.query(ShiftMapping).filter(
            ShiftMapping.shiftallowance_id == rec.id
        ).delete()
        db.delete(rec)

    db.flush()

def validate_required_excel_columns(df: pd.DataFrame):
    required_columns = {e.value for e in ExcelColumnMap}
    uploaded_columns = set(df.columns)

    missing = required_columns - uploaded_columns

    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Invalid Excel format",
                "missing_columns": sorted(missing)
            }
        )

def validate_excel_data(df: pd.DataFrame):
    errors = []
    error_rows = []

    month_pattern = re.compile(
        r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'[0-9]{2}$"
    )

    for idx, row in df.iterrows():
        row_errors = []

        for col in [
            "shift_a_days", "shift_b_days",
            "shift_c_days", "prime_days", "total_days"
        ]:
            try:
                df.at[idx, col] = float(row.get(col, 0))
                if df.at[idx, col] < 0:
                    row_errors.append(f"Negative value in '{col}'")
            except Exception:
                row_errors.append(f"Invalid numeric value in '{col}'")

        for col in ["duration_month", "payroll_month"]:
            val = str(row.get(col, "")).strip()
            if val and not month_pattern.match(val):
                row_errors.append(f"Invalid month format in '{col}'")

        try:
            total = (
                df.at[idx, "shift_a_days"]
                + df.at[idx, "shift_b_days"]
                + df.at[idx, "shift_c_days"]
                + df.at[idx, "prime_days"]
            )
            if total != df.at[idx, "total_days"]:
                row_errors.append("Total days do not match sum of shifts")
        except Exception:
            pass

        if row_errors:
            r = row.to_dict()
            r["error"] = "; ".join(row_errors)
            error_rows.append(r)
            errors.append(idx)

    clean_df = df.drop(index=errors).reset_index(drop=True)
    error_df = pd.DataFrame(error_rows) if error_rows else None

    return clean_df, error_df


def normalize_error_rows(error_rows):
    normalized = []

    for row in error_rows:
        r = dict(row)
        err_text = r.pop("error", "")
        reason = {}

        for err in err_text.split(";"):
            err = err.strip()
            if "Invalid numeric value" in err or "Negative value" in err:
                col = err.split("'")[1]
                reason[col] = "Expected non-negative numeric value"
            elif "Invalid month format" in err:
                if "duration_month" in err:
                    reason["duration_month"] = "Expected Jan'25"
                elif "payroll_month" in err:
                    reason["payroll_month"] = "Expected Jan'25"
            elif "Total days do not match" in err:
                reason["total_days"] = "Shift days mismatch"

        r["reason"] = reason
        normalized.append(r)

    return normalized

async def process_excel_upload(file, db: Session, user, base_url: str):

    if not file.filename.endswith((".xls", ".xlsx")):
        raise HTTPException(status_code=400, detail="Only Excel files allowed")

    uploaded_file = UploadedFiles(
        filename=file.filename,
        uploaded_by=user.id,
        status="processing"
    )
    db.add(uploaded_file)
    db.commit()
    db.refresh(uploaded_file)

    try:
        df = pd.read_excel(io.BytesIO(await file.read()))

       
        validate_required_excel_columns(df)

       
        df.rename(columns={e.value: e.name for e in ExcelColumnMap}, inplace=True)
        df = df.where(pd.notnull(df), 0)

        clean_df, error_df = validate_excel_data(df)

        error_rows = []
        fname = None

        if error_df is not None and not error_df.empty:
            error_rows = normalize_error_rows(error_df.to_dict(orient="records"))
            fname = f"validation_errors_{uuid.uuid4().hex}.xlsx"
            error_df.to_excel(os.path.join(TEMP_FOLDER, fname), index=False)

        if clean_df.empty:
            raise HTTPException(
                status_code=400,
                detail=make_json_safe({
                    "message": "File processed with errors",
                    "records_inserted": 0,
                    "skipped_records": len(error_rows),
                    "error_file": fname,
                    "error_rows": error_rows,
                })
            )

        clean_df["duration_month"] = clean_df["duration_month"].apply(parse_month_format)
        clean_df["payroll_month"] = clean_df["payroll_month"].apply(parse_month_format)

        shift_rates = load_shift_rates(db)
        inserted = 0

        allowed_fields = {
            "emp_id", "emp_name", "grade", "department",
            "client", "project", "project_code",
            "account_manager", "practice_lead", "delivery_manager",
            "duration_month", "payroll_month",
            "billability_status", "practice_remarks", "rmg_comments",
        }

        for row in clean_df.to_dict(orient="records"):

            delete_existing_emp_month(
                db,
                row.get("emp_id"),
                row.get("duration_month"),
                row.get("payroll_month"),
            )

            sa = ShiftAllowances(**{k: row[k] for k in allowed_fields if k in row})
            db.add(sa)
            db.flush()

            for shift, col in [
                ("A", "shift_a_days"),
                ("B", "shift_b_days"),
                ("C", "shift_c_days"),
                ("PRIME", "prime_days"),
            ]:
                days = float(row.get(col, 0) or 0)
                if days > 0:
                    rate = shift_rates.get(shift, 0)
                    db.add(
                        ShiftMapping(
                            shiftallowance_id=sa.id,
                            shift_type=shift,
                            days=days,
                            total_allowance=days * rate
                        )
                    )

            inserted += 1

        db.commit()

        if error_rows:
            raise HTTPException(
                status_code=400,
                detail=make_json_safe({
                    "message": "File processed with errors",
                    "records_inserted": inserted,
                    "skipped_records": len(error_rows),
                    "error_file": fname,
                    "error_rows": error_rows,
                })
            )

        return {
            "message": "File processed successfully",
            "records_inserted": inserted
        }

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

def parse_yyyy_mm(value: str) -> date:
    """
    Parses month in Mon'YY format (e.g. Jan'25) and returns first day of month
    """
    if not value:
        raise HTTPException(
            status_code=400,
            detail="Month is required in Mon'YY format (e.g. Jan'25)"
        )

    value = value.strip()

    if not re.match(r"^[A-Za-z]{3}'\d{2}$", value):
        raise HTTPException(
            status_code=400,
            detail="Invalid month format. Expected Mon'YY (e.g. Jan'25)"
        )

    try:
        return datetime.strptime(value, "%b'%y").date().replace(day=1)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid month value"
        )


def validate_not_future_month(month_date: date, field_name: str):
    today = date.today().replace(day=1)
    if month_date > today:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} cannot be a future month"
        )


def validate_half_day(value: float, field_name: str):
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


def validate_shift_days(row: CorrectedRow) -> float:
    shifts = {
        "shift_a_days": row.shift_a_days or 0,
        "shift_b_days": row.shift_b_days or 0,
        "shift_c_days": row.shift_c_days or 0,
        "prime_days": row.prime_days or 0,
    }

    total = 0.0

    for name, value in shifts.items():
        try:
            value = float(value)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"{name} must be numeric"
            )

        validate_half_day(value, name)
        total += value

    if total <= 0:
        raise HTTPException(
            status_code=400,
            detail="At least one shift day must be greater than 0"
        )

    return total


def days_in_month(month_date: date) -> int:
    return calendar.monthrange(month_date.year, month_date.month)[1]


def load_shift_rates(db: Session) -> dict:
    rates = {}
    rows = db.query(ShiftsAmount).all()
    for r in rows:
        if r.shift_type:
            rates[r.shift_type.upper()] = float(r.amount or 0)
    return rates


def update_corrected_rows(
    db: Session,
    corrected_rows: List[CorrectedRow]
):
    if not corrected_rows:
        raise HTTPException(
            status_code=400,
            detail="No corrected rows provided"
        )

    failed_rows = []
    shift_rates = load_shift_rates(db)

    for row in corrected_rows:
        try:
            if not row.emp_id or not row.duration_month or not row.project:
                raise HTTPException(
                    status_code=400,
                    detail="emp_id, duration_month and project are required"
                )

            duration_month = parse_yyyy_mm(row.duration_month)
            payroll_month = parse_yyyy_mm(row.payroll_month)

            validate_not_future_month(duration_month, "duration_month")
            validate_not_future_month(payroll_month, "payroll_month")

            if duration_month == payroll_month:
                raise HTTPException(
                    status_code=400,
                    detail="duration_month and payroll_month cannot be the same"
                )

            if payroll_month < duration_month:
                raise HTTPException(
                    status_code=400,
                    detail="Payroll month cannot be earlier than duration month"
                )

            total_shift_days = validate_shift_days(row)

            duration_month_days = days_in_month(duration_month)
            if total_shift_days > duration_month_days:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Total shift days ({total_shift_days}) exceeds "
                        f"duration month days ({duration_month_days})"
                    )
                )

            sa = (
                db.query(ShiftAllowances)
                .filter(
                    ShiftAllowances.emp_id == row.emp_id,
                    ShiftAllowances.duration_month == duration_month,
                    ShiftAllowances.payroll_month == payroll_month,
                )
                .first()
            )

            if not sa:
                sa = ShiftAllowances(
                    emp_id=row.emp_id,
                    duration_month=duration_month,
                    payroll_month=payroll_month,
                    project=row.project,
                )
                db.add(sa)
                db.flush()
            else:
                sa.project = row.project
                db.query(ShiftMapping).filter(
                    ShiftMapping.shiftallowance_id == sa.id
                ).delete()

            shift_map = {
                "A": row.shift_a_days,
                "B": row.shift_b_days,
                "C": row.shift_c_days,
                "PRIME": row.prime_days,
            }

            for shift_type, days in shift_map.items():
                if days and float(days) > 0:
                    rate = shift_rates.get(shift_type, 0)
                    allowance = float(days) * rate

                    db.add(
                        ShiftMapping(
                            shiftallowance_id=sa.id,
                            shift_type=shift_type,
                            days=float(days),
                            total_allowance=allowance,
                        )
                    )

        except HTTPException as e:
            db.rollback()
            failed_rows.append({
                "emp_id": row.emp_id,
                "duration_month": row.duration_month,
                "project": row.project,
                "reason": e.detail,
            })

        except Exception as e:
            db.rollback()
            failed_rows.append({
                "emp_id": row.emp_id,
                "duration_month": row.duration_month,
                "project": row.project,
                "reason": str(e),
            })

    if failed_rows:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Validation failed",
                "failed_rows": failed_rows
            }
        )

    db.commit()

    return {
        "message": "Rows inserted/updated successfully",
        "records_processed": len(corrected_rows)
    }
