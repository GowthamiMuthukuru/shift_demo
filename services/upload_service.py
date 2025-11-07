import pandas as pd
import io
from fastapi import HTTPException
from sqlalchemy.orm import Session
from models.models import UploadedFiles, ShiftAllowances
from models.enums import ExcelColumnMap

async def process_excel_upload(file, db: Session, user):
    uploaded_by = user.id

    # Validate file type
    if not file.filename.endswith((".xls", ".xlsx")):
        raise HTTPException(status_code=400, detail="Only Excel files are allowed")

    # Create uploaded file record
    uploaded_file = UploadedFiles(
        filename=file.filename,
        uploaded_by=uploaded_by,
        status="processing"
    )
    db.add(uploaded_file)
    db.commit()
    db.refresh(uploaded_file)

    try:
        # Read Excel file
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))

        # Build mapping from Enum (Excel → DB)
        column_mapping = {e.value: e.name for e in ExcelColumnMap}

        # Rename columns based on Enum
        df.rename(columns=column_mapping, inplace=True)

        # Validate required columns
        required_columns = [e.name for e in ExcelColumnMap]
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing columns in Excel: {missing}")

        # Replace NaN and cast numeric columns
        df = df.where(pd.notnull(df), 0)
        df = df.astype({
            "shift_a_days": "int64",
            "shift_b_days": "int64",
            "shift_c_days": "int64",
            "prime_days": "int64",
            "total_days": "int64",
            "billable_days": "int64",
            "non_billable_days": "int64",
            "diff": "int64",
            "final_total_days": "int64",
        })

        # Bulk insert shift allowances
        shift_records = [
            ShiftAllowances(file_id=uploaded_file.id, **row)
            for row in df[required_columns].to_dict(orient="records")
        ]
        db.bulk_save_objects(shift_records)
        db.commit()

        # Update uploaded file status
        uploaded_file.status = "processed"
        uploaded_file.record_count = len(shift_records)
        db.commit()

        return {
            "message": "File processed successfully",
            "file_id": uploaded_file.id,
            "records": len(shift_records)
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
