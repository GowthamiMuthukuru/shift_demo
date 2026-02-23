from fastapi import APIRouter, Depends, Body
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from db import get_db
from utils.dependencies import get_current_user
from services.get_excel_service import shift_excel_download_service

router = APIRouter(prefix="/excel", tags=["Excel Data"])


@router.post("/download")
def download_excel(
    payload: dict = Body(
        ...,
        example={
            "clients": "ALL",              # string or CSV or list
            "departments": "ALL",          # string or CSV or list
            "years": [2025],               # list[int]
            "months": [1, 2, 3],           # list[int]
            "shifts": "ALL",               # string or CSV or list
            "headcounts": "ALL",           # validated but not used for filtering here
            "emp_id": "IN12345",           # string or CSV or list (exact match, case-insensitive)
            "client_partner": "John Doe",  # string or CSV or list (exact match, case-insensitive)
            # NEW: inclusive allowance ranges (employee-level)
            "allowance": "5000-20000",
            # or
            # "allowance": ["0-5000", "10000-15000"],
            "sort_by": "total_allowance",
            "sort_order": "default"
        }
    ),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    file_path = shift_excel_download_service(
        db=db,
        payload=payload
    )

    return FileResponse(
        path=file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="shift_data.xlsx",
    )