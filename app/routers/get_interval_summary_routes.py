from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
 
from db import get_db
from utils.dependencies import get_current_user  
from services.get_interval_summary_service import get_interval_summary_service
 
router = APIRouter()
 
@router.get("/shift/interval-summary")
def interval_summary(
    start_month: str | None = None,
    end_month: str | None = None,
    account_manager: str | None = None,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)  
):
    try:
        return get_interval_summary_service(
            db=db,
            start_month=start_month,
            end_month=end_month,
            account_manager=account_manager
        )
    except HTTPException as e:
        raise e