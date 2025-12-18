from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from db import get_db
from services.search_service import export_filtered_excel
from utils.dependencies import get_current_user

router = APIRouter(
    prefix="/employee-details",
    tags=["Search Details"]
)

@router.get("/search")
def fetch_employee_details(
    emp_id: str | None = Query(None),
    account_manager: str | None = Query(None),
    department: str | None = Query(None),
    client: str | None = Query(None),
    start_month: str | None = Query(None, description="YYYY-MM"),
    end_month: str | None = Query(None, description="YYYY-MM"),
    start: int | None = Query(0, ge=0),
    limit: int | None = Query(10, gt=0),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
  
    return export_filtered_excel(
        db=db,
        emp_id=emp_id,
        account_manager=account_manager,
        department=department,
        client=client,
        start_month=start_month,
        end_month=end_month,
        start=start,
        limit=limit,
    )
