import os
import uuid
import io
import pandas as pd
import re
from datetime import datetime
from fastapi import HTTPException
from sqlalchemy.orm import Session
from models.models import UploadedFiles, ShiftAllowances, ShiftMapping
from utils.enums import ExcelColumnMap
from sqlalchemy.exc import IntegrityError

TEMP_FOLDER = "media/error_excels"
os.makedirs(TEMP_FOLDER, exist_ok=True)

MONTH_MAP = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
    'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
    'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}


def parse_month_format(value: str):
    if not isinstance(value, str):
        return None
    try:
        month_abbr, year_suffix = value.split("'")
        month_num = MONTH_MAP.get(month_abbr.strip().title())
        year_full = 2000 + int(year_suffix)
        if month_num:
            return datetime(year_full, month_num, 1).date()
    except Exception:
        pass
    return None


def validate_excel_data(df: pd.DataFrame):
    errors = []
    error_rows = []

    month_pattern = re.compile(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'[0-9]{2}$")

    for idx, row in df.iterrows():
        row_errors = []

        # UPDATED: decimal-safe numeric validation
        for col in ["shift_a_days", "shift_b_days", "shift_c_days", "prime_days"]:
            value = row[col]
            try:
                if value is None or str(value).strip() == "":
                    df.at[idx, col] = 0.0
                else:
                    df.at[idx, col] = float(value)
            except Exception:
                row_errors.append(f"Invalid numeric value in '{col}' → '{value}'")

        # Validate month format
        for month_col in ["duration_month", "payroll_month"]:
            value = str(row.get(month_col, "")).strip()
            if value and not month_pattern.match(value):
                row_errors.append(
                    f"Invalid month format in '{month_col}' → '{value}' (expected like Feb'25)"
                )

        if row_errors:
            row_data = row.to_dict()
            row_data["error"] = "; ".join(row_errors)
            error_rows.append(row_data)
            errors.append(idx)

    clean_df = df.drop(index=errors).reset_index(drop=True)
    error_df = pd.DataFrame(error_rows) if error_rows else None
    return clean_df, error_df



async def process_excel_upload(file, db: Session, user, base_url: str):
    uploaded_by = user.id

    if not file.filename.endswith((".xls", ".xlsx")):
        raise HTTPException(status_code=400, detail="Only Excel files are allowed")

    uploaded_file = UploadedFiles(
        filename=file.filename,
        uploaded_by=uploaded_by,
        status="processing",
        payroll_month=None,
    )
    db.add(uploaded_file)
    db.commit()
    db.refresh(uploaded_file)

    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))

        # Rename columns using Enum
        column_mapping = {e.value: e.name for e in ExcelColumnMap}
        df.rename(columns=column_mapping, inplace=True)

        # Required columns from Enum
        required_cols = [e.name for e in ExcelColumnMap]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing columns: {missing}")

        # Replace NaN with 0 for avoid type crash
        df = df.where(pd.notnull(df), 0)

        clean_df, error_df = validate_excel_data(df)

        if clean_df.empty:
            raise HTTPException(status_code=400, detail="No valid rows found in file")

        # Parse months
        for col in ["duration_month", "payroll_month"]:
            clean_df[col] = clean_df[col].apply(parse_month_format)

        uploaded_file.payroll_month = clean_df["payroll_month"].iloc[0]
        db.commit()

        inserted_count = 0

        # Fields
        shift_fields = {"shift_a_days", "shift_b_days", "shift_c_days", "prime_days"}
        allowed_fields = {
            "emp_id", "emp_name", "grade", "department",
            "client", "project", "project_code",
            "account_manager", "practice_lead", "delivery_manager",
            "duration_month", "payroll_month",
            "billability_status", "practice_remarks", "rmg_comments",
            "month_year",
        }

        for row in clean_df.to_dict(orient="records"):

            # UPDATED: store decimals as float (not int)
            shift_data = {k: float(row.get(k, 0)) for k in shift_fields}

            sa_payload = {k: row[k] for k in allowed_fields if k in row}

            sa = ShiftAllowances(**sa_payload)
            db.add(sa)
            db.flush()

            mapping_pairs = [
                ("A", shift_data["shift_a_days"]),
                ("B", shift_data["shift_b_days"]),
                ("C", shift_data["shift_c_days"]),
                ("PRIME", shift_data["prime_days"]),
            ]

            for shift_type, days in mapping_pairs:
                if days > 0:
                    db.add(ShiftMapping(
                        shiftallowance_id=sa.id,
                        shift_type=shift_type,
                        days=days  # decimal preserved
                    ))

            inserted_count += 1

        db.commit()

        # Error rows handling
        if error_df is not None and not error_df.empty:
            error_file = f"error_{uuid.uuid4().hex}.xlsx"
            path = os.path.join(TEMP_FOLDER, error_file)
            error_df.to_excel(path, index=False)

            uploaded_file.status = "partially_processed"
            uploaded_file.record_count = inserted_count
            db.commit()

            raise HTTPException(
            status_code=400,
            detail={
                "message": "File partially processed",
                "records_inserted": inserted_count,
                "records_skipped": len(error_df),
                "download_link": f"{base_url}/upload/error-files/{error_file}",
            }
        )

        uploaded_file.status = "processed"
        uploaded_file.record_count = inserted_count
        db.commit()

        return {
            "message": "File processed successfully",
            "records": inserted_count,
        }

    except HTTPException:
        db.rollback()
        uploaded_file.status = "failed"
        db.commit()
        raise

    except Exception as error:
        db.rollback()
        uploaded_file.status = "failed"
        db.commit()
        if "duplicate key value violates unique constraint" in str(error):
            raise HTTPException(
                status_code=400,
                detail="Duplicate data found: This record already exists for the same employee and payroll month."
            )

        raise HTTPException(status_code=500, detail=f"Processing failed: {str(error)}")
