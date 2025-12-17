from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from utils.dependencies import get_current_user
from db import get_db
from services.client_summary_download_service import client_summary_download_service

router = APIRouter(prefix="/client-summary")


@router.post("/download")
def download_client_summary_excel(
    payload: dict = Body(
        ...,
        example={
            "clients": "ALL",
            "start_month": "YYYY-MM",
            "end_month": "YYYY-MM",
            "selected_year": "YYYY",
            "selected_months": ["01", "02"],
            "selected_quarters": ["Q1"]
        }
    ),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    file_path = client_summary_download_service(db=db, payload=payload)

    return FileResponse(
        path=file_path,
        filename="client_summary.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
