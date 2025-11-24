import pandas as pd
import os
from datetime import datetime
from sqlalchemy.orm import Session
from models.models import ShiftAllowances, ShiftMapping
 
 
def convert_to_db_date_format(payroll_month: str) -> str:
    """
    Converts MM-YYYY → YYYY-MM-01 (DB format)
    Example: 03-2025 → 2025-03-01
    """
    try:
        dt = datetime.strptime(payroll_month, "%m-%Y")
        return dt.strftime("%Y-%m-01")
    except ValueError:
        raise ValueError("Invalid format. Use MM-YYYY (e.g., 03-2025)")
 
 
def export_excel_by_payroll_month(db: Session, payroll_month: str):
    """
    Fetch all data for the given payroll month and export to Excel.
    Returns file path if found, else None.
    """
 
    db_date = convert_to_db_date_format(payroll_month)
 
    # Fetch main shift records
    shift_records = db.query(ShiftAllowances).filter(
        ShiftAllowances.payroll_month == db_date
    ).all()
 
    if not shift_records:
        return None
 
    # Prepare Excel data
    excel_data = []
 
    for record in shift_records:
        # Fetch all shift types for this record
        shift_types = db.query(ShiftMapping.shift_type).filter(
            ShiftMapping.shiftallowance_id == record.id
        ).all()
 
        # Convert to a list of values
        shift_type_list = [s[0] for s in shift_types] if shift_types else []
 
        excel_data.append({
            "emp_id": record.emp_id,
            "emp_name": record.emp_name,
            "grade": record.grade,
            "department": record.department,
            "client": record.client,
            "project": record.project,
            "project_code": record.project_code,
            "account_manager": record.account_manager,
            "practice_lead": record.practice_lead,
            "delivery_manager": record.delivery_manager,
            "duration_month": record.duration_month,
            "payroll_month": payroll_month,
            "shift_types": ", ".join(shift_type_list), 
            "billability_status": record.billability_status,
            "practice_remarks": record.practice_remarks,
            "rmg_comments": record.rmg_comments
        })
 
    df = pd.DataFrame(excel_data)
    df = df.fillna("")
 
    # Folder
    os.makedirs("exports", exist_ok=True)
 
    filename = f"Shift_Allowances_{payroll_month}.xlsx"
    filepath = os.path.join("exports", filename)
 
    df.to_excel(filepath, index=False)
 
    return filepath