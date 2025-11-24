from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user
from services.get_interval_summary_service import get_interval_summary_service

router = APIRouter(prefix="/interval")


@router.get("/get_interval_summary")
def get_interval_summary(
    start_month: str,
    end_month: str,
    db: Session = Depends(get_db),
    current_user: Session = Depends(get_current_user)
):
    try:
        result = get_interval_summary_service(start_month, end_month, db)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
