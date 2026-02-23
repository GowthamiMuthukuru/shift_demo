"""Routes for client summary: JSON API + Excel download."""
from fastapi import APIRouter, Depends, Body, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional, Union

from db import get_db
from utils.dependencies import get_current_user
from services.client_summary_download_service import client_summary_download_service
from schemas.dashboardschema import ClientSummaryRequest



router = APIRouter(
    prefix="/client-summary",
)


@router.post("/download")
def download_client_summary_excel(
    payload: ClientSummaryRequest = Body(
        ...,
        example={
            "years": [2025, 2026],
            "months": [1, 2, 3],
            "clients": ["Client A", "Client B"],   
            "departments": ["ALL"],
            "emp_id": ["IN01804611"],
            "client_partner": ["John Doe"],
            "shifts": "ALL",
            "headcounts": "1-10",
            "sort_by": "total_allowance",
            "sort_order": "desc",
            "allowance": "5000-20000",
        },
    ),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """
    Generate and download the client summary Excel report.
    """
    try:
        file_path = client_summary_download_service(
            db=db,
            payload=payload.dict(exclude_none=True)
        )
        return FileResponse(
            path=file_path,
            filename="client_summary.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
