from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user
from services.summary_service import get_client_shift_summary
 
router = APIRouter(prefix="/summary")
 
@router.get("/client-shift-summary")
def client_shift_summary(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """
    Returns total employees per client and total shift days (A, B, C, Prime)
    """
    summary = get_client_shift_summary(db)
    if not summary:
        raise HTTPException(status_code=404, detail="No records found")
    return {"summary": summary}