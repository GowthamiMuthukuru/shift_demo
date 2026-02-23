from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user
from services.client_summary_service import client_summary_service

router = APIRouter(
    prefix="/client-summary",
    tags=["Client Summary"],
)


@router.post("/")
def client_summary(
    payload: dict = Body(
        ...,
        example={
            "years": [2025, 2026],
            "months": [1, 2, 3],
            "clients": "ALL",
            "departments": "ALL",
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
    return client_summary_service(db=db, payload=payload)
