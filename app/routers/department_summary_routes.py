from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from db import get_db
from services.department_summary_service import get_department_summary

router = APIRouter(prefix="/department-summary")


@router.get("/")
def department_summary(
    month: str = Query(..., description="Provide month like YYYY-MM"),
    db: Session = Depends(get_db)
):
    summary = get_department_summary(db, month)
    return summary
