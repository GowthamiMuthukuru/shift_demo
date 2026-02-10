import os
from datetime import datetime
from fastapi import APIRouter, Depends, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from db import get_db
from utils.dependencies import get_current_user
from services.get_excel_service import export_filtered_excel_df, dataframe_to_excel_file

router = APIRouter(prefix="/excel", tags=["Excel Data"])

EXPORT_DIR = "exports"

@router.get("/download")
def download_excel(
    emp_id: str | None = Query(None),
    client_partner: str | None = Query(None),
    department: str | None = Query(None),
    client: str | None = Query(None),
    start_month: str | None = Query(None),
    end_month: str | None = Query(None),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    df = export_filtered_excel_df(
        db=db,
        emp_id=emp_id,
        client_partner=client_partner,
        department=department,
        client=client,
        start_month=start_month,
        end_month=end_month,
    )

    os.makedirs(EXPORT_DIR, exist_ok=True)

    file_path = os.path.join(EXPORT_DIR, "shift_data.xlsx")
    dataframe_to_excel_file(df, file_path=file_path, sheet_name="Shift Data")

   
    return FileResponse(
        path=file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="shift_data.xlsx",
    )