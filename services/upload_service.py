import os
import uuid
import io
import pandas as pd
from datetime import datetime
from fastapi import HTTPException
from sqlalchemy.orm import Session
from models.models import UploadedFiles, ShiftAllowances
from utils.enums import ExcelColumnMap

# Folder for saving generated error Excels
TEMP_FOLDER = "media/error_excels"
os.makedirs(TEMP_FOLDER, exist_ok=True)


# Helper: Validate Excel Data
def validate_excel_data(df: pd.DataFrame, numeric_columns: list):
    errors = []
    error_rows = []

    for idx, row in df.iterrows():
        row_errors = []
        for col in numeric_columns:
            value = row[col]
            # Type check
            if not pd.api.types.is_numeric_dtype(type(value)):
                try:
                    df.at[idx, col] = pd.to_numeric(value)
                except (ValueError, TypeError):
                    row_errors.append(f"Invalid value in '{col}' → '{value}' (expected numeric)")
        if row_errors:
            row_data = row.to_dict()
            row_data["error"] = "; ".join(row_errors)
            error_rows.append(row_data)
            errors.append(idx)

    clean_df = df.drop(index=errors).reset_index(drop=True)
    error_df = pd.DataFrame(error_rows) if error_rows else None
    return clean_df, error_df


# Main Upload Processor
async def process_excel_upload(file, db: Session, user, base_url: str):
    uploaded_by = user.id

    if not file.filename.endswith((".xls", ".xlsx")):
        raise HTTPException(status_code=400, detail="Only Excel files are allowed")

    # Pre-create upload entry
    uploaded_file = UploadedFiles(
        filename=file.filename,
        uploaded_by=uploaded_by,
        status="processing",
        payroll_month="unknown",  # temporary until we parse Excel
    )
    db.add(uploaded_file)
    db.commit()
    db.refresh(uploaded_file)

    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))

        # Map Excel columns to DB field names
        column_mapping = {e.value: e.name for e in ExcelColumnMap}
        df.rename(columns=column_mapping, inplace=True)

        required_columns = [e.name for e in ExcelColumnMap]
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing columns in Excel: {missing}")

        # Replace NaN with 0 for numeric columns
        df = df.where(pd.notnull(df), 0)

        # Numeric columns from your current schema
        int_columns = ["shift_a_days", "shift_b_days", "shift_c_days", "prime_days", "total_days"]
        numeric_columns = int_columns

        # Validate numeric values
        clean_df, error_df = validate_excel_data(df, numeric_columns)

        inserted_count = 0
        if not clean_df.empty:
            # Convert numeric columns
            clean_df[int_columns] = (
                clean_df[int_columns]
                .apply(pd.to_numeric, errors="coerce")
                .round(0)
                .astype("Int64")
            )

            # Extract payroll month from Excel
            payroll_month_val = clean_df["payroll_month"].iloc[0]
            payroll_str = (
                payroll_month_val if isinstance(payroll_month_val, str)
                else datetime.strftime(payroll_month_val, "%m-%Y")
            )
            uploaded_file.payroll_month = payroll_str

            # Prepare rows for insertion
            records = clean_df[required_columns].to_dict(orient="records")

            # Add auto month_year (model also sets default, this is explicit)
            for row in records:
                if not row.get("month_year"):
                    row["month_year"] = datetime.now().strftime("%m-%Y")

            shift_records = [ShiftAllowances(**row) for row in records]

            db.bulk_save_objects(shift_records)
            db.commit()
            inserted_count = len(shift_records)

        # Handle errors if invalid rows
        if error_df is not None and not error_df.empty:
            error_filename = f"error_{uuid.uuid4().hex}.xlsx"
            error_path = os.path.join(TEMP_FOLDER, error_filename)
            error_df.to_excel(error_path, index=False)

            uploaded_file.status = "partially_processed"
            uploaded_file.record_count = inserted_count
            db.commit()

            error_download_link = f"{base_url}/upload/error-files/{error_filename}"

            return {
                "message": "File partially processed. Some rows contained invalid data.",
                "records_inserted": inserted_count,
                "records_skipped": len(error_df),
                "download_link": error_download_link,
                "file_name": error_filename,
            }

        # All rows valid
        uploaded_file.status = "processed"
        uploaded_file.record_count = inserted_count
        db.commit()

        return {
            "message": "File processed successfully",
            "file_id": uploaded_file.id,
            "records": inserted_count,
        }

    except HTTPException:
        db.rollback()
        uploaded_file.status = "failed"
        db.commit()
        raise

    except Exception as e:
        db.rollback()
        uploaded_file.status = "failed"
        db.commit()
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")
