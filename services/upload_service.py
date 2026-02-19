"""Services for validating, processing, and uploading shift allowance Excel files."""

import os
import uuid
import io
import re
from datetime import datetime, date
from decimal import Decimal
import pandas as pd
from fastapi import HTTPException
from sqlalchemy.orm import Session
import calendar
from schemas.displayschema import CorrectedRow
from models.models import UploadedFiles, ShiftAllowances, ShiftMapping, ShiftsAmount
from utils.enums import ExcelColumnMap
from utils.shift_config import get_shift_string, get_all_shift_keys, get_allowance_columns
from fastapi.responses import JSONResponse

TEMP_FOLDER = "media/error_excels"
os.makedirs(TEMP_FOLDER, exist_ok=True)


MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
    "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}


def format_inr(amount):
    try:
        amount = Decimal(amount)
    except Exception:
        return "₹ 0"
    return f"₹ {amount:,.0f}"

def make_json_safe(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(i) for i in obj]
    return obj

def parse_month_format(value: str):
    """Parse Mon'YY format to date object."""
    if not isinstance(value, str):
        return None
    try:
        m, y = value.split("'")
        return datetime(2000 + int(y), MONTH_MAP[m.title()], 1).date()
    except Exception:
        return None

def load_shift_rates(db: Session) -> dict:
    """Load shift allowance rates from DB."""
    return {
        r.shift_type.upper(): float(r.amount or 0)
        for r in db.query(ShiftsAmount).all()
        if r.shift_type
    }

def delete_existing_emp_month(db, emp_id, client, duration_month, payroll_month):
    """Delete existing records for an employee and month before insert."""
    records = (
        db.query(ShiftAllowances)
        .filter(
            ShiftAllowances.emp_id == emp_id,
            ShiftAllowances.client == client,
            ShiftAllowances.duration_month == duration_month,
            ShiftAllowances.payroll_month == payroll_month,
        )
        .all()
    )

    for rec in records:
        db.query(ShiftMapping).filter(ShiftMapping.shiftallowance_id == rec.id).delete()
        db.delete(rec)

    db.flush()

def validate_required_excel_columns(df: pd.DataFrame):
    """Ensure all required columns exist in uploaded Excel."""
    required = {str(e.value) for e in ExcelColumnMap}
    uploaded = {str(c) for c in df.columns}
    missing = required - uploaded
    if missing:
        raise HTTPException(
            status_code=400,
            detail={"message": "Invalid Excel format", "missing_columns": sorted(missing)},
        )

def validate_excel_data(df: pd.DataFrame):
    """Validate Excel data: numeric shifts, month formats, total days."""
    errors, error_rows = [], []
    month_pattern = re.compile(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'[0-9]{2}$")

    shift_keys = get_all_shift_keys()

    for idx, row in df.iterrows():
        row_errors = []

       
        for col in shift_keys + ["total_days"]:
            try:
                df.at[idx, col] = float(row.get(col, 0) or 0)
                if df.at[idx, col] < 0:
                    row_errors.append(f"Negative value in '{col}'")
            except Exception:
                row_errors.append(f"Invalid numeric value in '{col}'")

       
        for col in ["duration_month", "payroll_month"]:
            val = str(row.get(col, "")).strip()
            if val and not month_pattern.match(val):
                row_errors.append(f"Invalid duration_month format in '{col}'")
                row_errors.append(f"Invalid payroll_month format in '{col}'")

       
        try:
            total = sum(float(row.get(c, 0) or 0) for c in shift_keys)
            if abs(total - float(row.get("total_days", 0) or 0)) > 0.01:
                row_errors.append("Total days do not match sum of shifts")
        except Exception:
            pass

        if row_errors:
            r = row.to_dict()
            r["error"] = "; ".join(row_errors)
            error_rows.append(r)
            errors.append(idx)

    return df.drop(index=errors).reset_index(drop=True), (
        pd.DataFrame(error_rows) if error_rows else None
    )

def normalize_error_rows(error_rows):
    """Normalize errors for JSON response."""
    normalized = []
    for row in error_rows:
        r = dict(row)
        err_text = r.pop("error", "")
        reason = {}
        for err in err_text.split(";"):
            err = err.strip()
            if "numeric" in err or "Negative" in err:
                parts = err.split("'")
                if len(parts) >= 2:
                    reason[parts[1]] = "Expected non-negative numeric value"
                else:
                    reason["numeric"] = "Expected non-negative numeric value"
            elif "month format" in err:
                reason["duration_month"] = "Expected Mon'YY format"
            elif "month_format" in err:
                reason["payroll_month"] = "Expected Mon'YY format"
            elif "Total days" in err:
                reason["total_days"] = "Shift days mismatch"
        r["reason"] = reason
        normalized.append(r)
    return normalized

def normalize_header(s: str) -> str:
    """
    Normalize Excel headers for robust matching:
    - trim
    - collapse multiple spaces
    - replace dash variants with '-'
    - lowercase
    """
    if s is None:
        return ""
    s = str(s).strip()
    s = s.replace("–", "-").replace("—", "-").replace("−", "-")
    s = re.sub(r"\s+", " ", s)
    return s.lower()


async def process_excel_upload(file, db: Session, user, base_url: str):
    """Process uploaded Excel for shift allowances."""
    if not file.filename.endswith((".xls", ".xlsx")):
        raise HTTPException(400, "Only Excel files allowed")

    uploaded_file = UploadedFiles(
        filename=file.filename,
        uploaded_by=user.id,
        status="processing",
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
        error_rows, fname = [], None

    
        if error_df is not None and not error_df.empty:
            error_rows = normalize_error_rows(error_df.to_dict("records"))
            fname = f"validation_errors_{uuid.uuid4().hex}.xlsx"
            file_path = os.path.join(TEMP_FOLDER, fname)

            with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
                error_df.to_excel(writer, index=False, sheet_name="Errors")
                workbook = writer.book
                sheet = writer.sheets["Errors"]

              
                fmt_header = workbook.add_format({
                    "align": "center", "valign": "vcenter",
                    "bold": True, "border": 1, "text_wrap": True
                })
                fmt_center = workbook.add_format({
                    "align": "center", "valign": "vcenter", "border": 1
                })
                fmt_days = workbook.add_format({
                    "align": "center", "valign": "vcenter", "border": 1,
                    "num_format": "0.0"   
                })

               
                fmt_inr = workbook.add_format({
                    "align": "center", "valign": "vcenter", "border": 1,
                    "num_format": "₹ #,##0"
                })

                shift_keys = get_all_shift_keys()
                DAY_COLS = set(shift_keys + ["total_days"])


                configured_allowance_cols = get_allowance_columns()

                normalized_allowance_cols = {normalize_header(c) for c in configured_allowance_cols}

                CURRENCY_COLS = {
                    col for col in error_df.columns
                    if normalize_header(col) in normalized_allowance_cols
                }
                CURRENCY_COLS = CURRENCY_COLS - DAY_COLS

                for c, col in enumerate(error_df.columns):

                    header = get_shift_string(col) if col in shift_keys else col
                    header = header or col
                    sheet.write(0, c, header, fmt_header)
                    sheet.set_column(c, c, 25)

                for r, row in enumerate(error_df.itertuples(index=False), start=1):
                    for c, val in enumerate(row):
                        col_name = error_df.columns[c]

                        if col_name in DAY_COLS:
                           
                            try:
                                sheet.write_number(r, c, float(val or 0), fmt_days)
                            except Exception:
                                sheet.write(r, c, "" if val is None else str(val), fmt_center)

                        elif col_name in CURRENCY_COLS:
                           
                            try:
                                sheet.write_number(r, c, float(val or 0), fmt_inr)
                            except Exception:
                                sheet.write(r, c, "" if val is None else str(val), fmt_center)

                        else:
                        
                            sheet.write(r, c, "" if val is None else str(val), fmt_center)

       
        if clean_df.empty:
            raise HTTPException(400, make_json_safe({
                "message": "File processed with errors",
                "records_inserted": 0,
                "skipped_records": len(error_rows),
                "error_file": fname,
                "error_rows": error_rows,
            }))

        clean_df["duration_month"] = clean_df["duration_month"].apply(parse_month_format)
        clean_df["payroll_month"] = clean_df["payroll_month"].apply(parse_month_format)

        shift_rates = load_shift_rates(db)
        inserted = 0

        allowed_fields = {
            "emp_id", "emp_name", "grade", "department",
            "client", "project", "project_code",
            "client_partner", "practice_lead", "delivery_manager",
            "duration_month", "payroll_month",
            "billability_status", "practice_remarks", "rmg_comments",
        }

        shift_keys = get_all_shift_keys()
        for row in clean_df.to_dict("records"):
            delete_existing_emp_month(
                db, row.get("emp_id"), row.get("client"),
                row.get("duration_month"), row.get("payroll_month")
            )

            sa = ShiftAllowances(**{k: row[k] for k in allowed_fields if k in row})
            db.add(sa)
            db.flush()

            for shift in shift_keys:
                days = float(row.get(shift, 0) or 0)
                if days > 0:
                    db.add(ShiftMapping(
                        shiftallowance_id=sa.id,
                        shift_type=shift,
                        days=days,
                        total_allowance=days * shift_rates.get(shift, 0),
                    ))

            inserted += 1

        db.commit()

        if error_rows:
            raise HTTPException(400, make_json_safe({
                "message": "File processed with errors",
                "records_inserted": inserted,
                "skipped_records": len(error_rows),
                "error_file": fname,
                "error_rows": error_rows,
            }))

        return {"message": "File processed successfully", "records_inserted": inserted}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e)) from e

def parse_yyyy_mm(value: str) -> date:
    """Parse month in Mon'YY format (e.g., Jan'25)."""
    if not value:
        raise HTTPException(
            status_code=400,
            detail="Month is required in Mon'YY format (e.g., Jan'25)"
        )
    value = value.strip()
    if not re.match(r"^[A-Za-z]{3}'\d{2}$", value):
        raise HTTPException(
            status_code=400,
            detail="Invalid month format. Expected Mon'YY (e.g., Jan'25)"
        )
    try:
        return datetime.strptime(value, "%b'%y").date().replace(day=1)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="Invalid month value"
        ) from exc

def validate_half_day(value: float, field_name: str):
    """Ensure value is non-negative and in 0.5 increments."""
    if value < 0:
        raise HTTPException(status_code=400, detail=f"{field_name} must be non-negative")
    if (value * 2) % 1 != 0:
        raise HTTPException(status_code=400, detail=f"{field_name} must be in 0.5 increments")

def days_in_month(month_date: date) -> int:
    return calendar.monthrange(month_date.year, month_date.month)[1]

def load_shift_rates(db: Session) -> dict:
    """Load shift allowance rates from DB."""
    return {
        r.shift_type.upper(): float(r.amount or 0)
        for r in db.query(ShiftsAmount).all()
        if r.shift_type
    }

def update_corrected_rows(db: Session, corrected_rows: list[CorrectedRow]):
    """Update corrected rows for shift allowances using dynamic shift config."""
    if not corrected_rows:
        raise HTTPException(400, "No corrected rows provided")

    
    shift_keys = [k.upper().strip() for k in get_all_shift_keys()]
    valid_shift_set = set(shift_keys)

    shift_rates = load_shift_rates(db)
    failed_rows = []

    for row in corrected_rows:
        try:
            duration_month = parse_yyyy_mm(row.duration_month)
            payroll_month = parse_yyyy_mm(row.payroll_month)
            today_month = date.today().replace(day=1)

            if duration_month > today_month:
                raise HTTPException(400, "Duration month cannot be a future month")
            if payroll_month > today_month:
                raise HTTPException(400, "Payroll month cannot be a future month")
            if duration_month == payroll_month:
                raise HTTPException(400, "Duration month and payroll month cannot be the same")
            if payroll_month < duration_month:
                raise HTTPException(400, "Payroll month must be after duration month")

        
            dynamic = row.shift_days or {}
            shifts = {str(k).upper().strip(): float(v or 0) for k, v in dynamic.items()}

            
            if not shifts:
                shifts = {k: float(getattr(row, k, 0) or 0) for k in shift_keys}

           
            unknown = [k for k in shifts.keys() if k not in valid_shift_set]
            if unknown:
                raise HTTPException(400, f"Invalid shift types: {unknown}")

            shifts = {k: float(shifts.get(k, 0) or 0) for k in shift_keys}

            total_shift_days = 0.0
            for shift_name, value in shifts.items():
                validate_half_day(value, shift_name)
                total_shift_days += value

            if total_shift_days <= 0:
                raise HTTPException(400, "At least one shift day must be greater than 0")
            if total_shift_days > days_in_month(duration_month):
                raise HTTPException(400, "Total shift days exceed days in duration month")


            sa = (
                db.query(ShiftAllowances)
                .filter(
                    ShiftAllowances.emp_id == row.emp_id,
                    ShiftAllowances.client == getattr(row, "client", None),
                    ShiftAllowances.duration_month == duration_month,
                    ShiftAllowances.payroll_month == payroll_month,
                )
                .first()
            )

            if not sa:
                sa = ShiftAllowances(
                    emp_id=row.emp_id,
                    client=getattr(row, "client", None),
                    duration_month=duration_month,
                    payroll_month=payroll_month,
                )
                db.add(sa)
                db.flush()

            for attr in [
                "emp_name", "grade", "department", "project", "project_code",
                "client_partner", "practice_lead", "delivery_manager",
                "current_status", "total_days", "timesheet_billable_days",
                "timesheet_non_billable_days", "diff", "final_total_days",
                "billability_status", "practice_remarks", "rmg_comments",
                "amar_approval"
            ]:
                if hasattr(row, attr):
                    setattr(sa, attr, getattr(row, attr))

          
            db.query(ShiftMapping).filter(ShiftMapping.shiftallowance_id == sa.id).delete()

            for shift_name, days in shifts.items():
                if days > 0:
                    db.add(
                        ShiftMapping(
                            shiftallowance_id=sa.id,
                            shift_type=shift_name,
                            days=days,
                            total_allowance=days * shift_rates.get(shift_name, 0),
                        )
                    )

            db.commit()

        except Exception as e:
            db.rollback()
            failed_rows.append({
                "emp_id": row.emp_id,
                "project": getattr(row, "project", ""),
                "duration_month": getattr(row, "duration_month", ""),
                "payroll_month": getattr(row, "payroll_month", ""),
                "reason": e.detail if isinstance(e, HTTPException) else str(e),
            })

    if failed_rows:
        return JSONResponse(
            status_code=400,
            content={
                "message": "Validation failed",
                "failed_rows": failed_rows
            }
        )

    return {
        "message": "Rows inserted/updated successfully",
        "records_processed": len(corrected_rows)
    }