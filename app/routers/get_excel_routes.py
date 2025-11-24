from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user
from services.get_excel_service import export_excel_by_payroll_month
 
router = APIRouter(prefix="/excel")
 
@router.get("/get_excel_data")
def get_excel_data(
    payroll_month: str = Query(..., description="Enter payroll month in MM-YYYY format"),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """
    Export employee data for a given payroll month.
    Example: /excel/get_excel_data?payroll_month=03-2025
    """
 
    try:
        file_path = export_excel_by_payroll_month(db, payroll_month)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
 
    if not file_path:
        raise HTTPException(
            status_code=404,
            detail="No data found for the given payroll month"
        )
 
    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=file_path.split("/")[-1]
    )