"""Services for validating, processing, and uploading shift allowance Excel files."""

import os
import uuid
import io
import re
import calendar
from datetime import datetime, date
from typing import List
import json

import pandas as pd
from fastapi import HTTPException
from sqlalchemy.orm import Session

from models.models import UploadedFiles, ShiftAllowances, ShiftMapping, ShiftsAmount
from schemas.displayschema import CorrectedRow
from utils.enums import ExcelColumnMap


TEMP_FOLDER = "media/error_excels"
os.makedirs(TEMP_FOLDER, exist_ok=True)

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
    "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}


def make_json_safe(obj):
    """Convert dates and nested objects into JSON-safe values."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(i) for i in obj]
    return obj


def parse_month_format(value: str):
    """Parse month in Mon'YY format and return a date."""
    if not isinstance(value, str):
        return None
    try:
        m, y = value.split("'")
        return datetime(2000 + int(y), MONTH_MAP[m.title()], 1).date()
    except Exception:
        return None


def load_shift_rates(db: Session) -> dict:
    """Load shift rates from DB and return a dictionary."""
    rates = {}
    for r in db.query(ShiftsAmount).all():
        if r.shift_type:
            rates[r.shift_type.upper()] = float(r.amount or 0)
    return rates


def delete_existing_emp_month(db, emp_id, client, duration_month, payroll_month):
    """Delete existing shift allowance records for an employee in a month."""
    existing = (
        db.query(ShiftAllowances)
        .filter(
            ShiftAllowances.emp_id == emp_id,
            ShiftAllowances.client == client,
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
    """Check if all required Excel columns are present."""
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
    """Validate Excel data for numeric fields, months, and totals."""
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
    """Convert error rows into JSON-friendly structure."""
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
    """Process uploaded Excel file and persist valid shift records."""

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

            delete_existing_emp_month(db,
                                      row.get("emp_id"),
                                      row.get("client"),
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
        raise HTTPException(status_code=500, detail=str(e)) from e


def parse_yyyy_mm(value: str) -> date:
    """Parse month in Mon'YY format (e.g. Jan'25) and return first day of month."""
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
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="Invalid month value"
        ) from exc


def validate_not_future_month(month_date: date, field_name: str):
    """Ensure the month is not in the future."""
    today = date.today().replace(day=1)
    if month_date > today:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} cannot be a future month"
        )


def validate_half_day(value: float, field_name: str):
    """Ensure value is non-negative and in 0.5 increments."""
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
    """Validate shifts for a row and return total shift days."""
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
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"{name} must be numeric"
            ) from exc

        validate_half_day(value, name)
        total += value

    if total <= 0:
        raise HTTPException(
            status_code=400,
            detail="At least one shift day must be greater than 0"
        )

    return total


def days_in_month(month_date: date) -> int:
    """Return the number of days in the given month."""
    return calendar.monthrange(month_date.year, month_date.month)[1]


def update_corrected_rows(db: Session, corrected_rows: List["CorrectedRow"]):
    """Insert or update corrected shift allowance rows."""
    if not corrected_rows:
        raise HTTPException(400, "No corrected rows provided")

    shift_rates = load_shift_rates(db)
    failed_rows = []

    for row in corrected_rows:
        try:
            duration_month = parse_yyyy_mm(row.duration_month)
            payroll_month = parse_yyyy_mm(row.payroll_month)

            total_shift_days = validate_shift_days(row)
            if total_shift_days > days_in_month(duration_month):
                raise HTTPException(
                    400,
                    "Total shift days exceed number of days in duration month"
                )

            sa = (
                db.query(ShiftAllowances)
                .filter(
                    ShiftAllowances.emp_id == row.emp_id,
                    ShiftAllowances.client == row.client,
                    ShiftAllowances.duration_month == duration_month,
                    ShiftAllowances.payroll_month == payroll_month,
                )
                .first()
            )

            if not sa:
                sa = ShiftAllowances(
                    emp_id=row.emp_id,
                    client=row.client,
                    duration_month=duration_month,
                    payroll_month=payroll_month,
                )
                db.add(sa)
                db.flush()

            sa.emp_name = row.emp_name
            sa.grade = row.grade
            sa.current_status = row.current_status
            sa.department = row.department
            sa.project = row.project
            sa.project_code = row.project_code
            sa.account_manager = row.account_manager
            sa.practice_lead = row.practice_lead
            sa.delivery_manager = row.delivery_manager
            sa.shift_types = row.shift_types
            sa.total_days = row.total_days
            sa.timesheet_billable_days = row.timesheet_billable_days
            sa.timesheet_non_billable_days = row.timesheet_non_billable_days
            sa.diff = row.diff
            sa.final_total_days = row.final_total_days
            sa.billability_status = row.billability_status
            sa.practice_remarks = row.practice_remarks
            sa.rmg_comments = row.rmg_comments
            sa.amar_approval = row.amar_approval
            sa.shift_a_allowances = row.shift_a_allowances
            sa.shift_b_allowances = row.shift_b_allowances
            sa.shift_c_allowances = row.shift_c_allowances
            sa.prime_allowances = row.prime_allowances
            sa.total_days_allowances = row.total_days_allowances
            sa.am_email_attempt = row.am_email_attempt
            sa.am_approval_status = row.am_approval_status

            db.query(ShiftMapping).filter(
                ShiftMapping.shiftallowance_id == sa.id
            ).delete()

            for shift, days in {
                "A": row.shift_a_days,
                "B": row.shift_b_days,
                "C": row.shift_c_days,
                "PRIME": row.prime_days,
            }.items():
                if days and float(days) > 0:
                    rate = shift_rates.get(shift, 0)
                    db.add(
                        ShiftMapping(
                            shiftallowance_id=sa.id,
                            shift_type=shift,
                            days=float(days),
                            total_allowance=float(days) * rate,
                        )
                    )

            db.commit()

        except Exception as e:
            db.rollback()
            reason = e.detail if isinstance(e, HTTPException) else str(e)
            failed_rows.append({
                "emp_id": row.emp_id,
                "client": row.client,
                "duration_month": row.duration_month,
                "payroll_month": row.payroll_month,
                "reason": reason,
            })

    if failed_rows:
        raise HTTPException(
            400,
            json.dumps({
                "message": "Validation failed",
                "failed_rows": failed_rows
            }, default=str)
        )

    return {
        "message": "Rows inserted/updated successfully",
        "records_processed": len(corrected_rows)
    }
