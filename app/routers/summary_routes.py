from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user
from services.summary_service import get_client_shift_summary
from schemas.displayschema import ClientSummary
 
router = APIRouter(prefix="/summary", tags=["Summary"])
 
 
@router.get(
    "/client-shift-summary",
    response_model=dict[str, list[ClientSummary]],
    responses={404: {"description": "No records found"}}
)
def client_shift_summary(
    duration_month: str | None = Query(None, description="Format YYYY-MM"),
    account_manager: str | None = Query(None, description="Account manager name"),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
   
    summary = get_client_shift_summary(db, duration_month, account_manager)
 
    return summary