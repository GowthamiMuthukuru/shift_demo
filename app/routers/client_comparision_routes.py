from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from db import get_db
from services.client_comparision_service import (client_comparison_service,get_client_total_allowances,
                                                 get_client_departments_service)
from utils.dependencies import get_current_user
from schemas.displayschema import ClientDeptResponse
router = APIRouter()

@router.get("/client-comparison")
def client_comparison(
    client_name: str = Query(..., alias="client"),
    start_month: str | None = Query(None),
    end_month: str | None = Query(None),
    account_manager: str | None = Query(None),
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return client_comparison_service(
        db=db,
        client_name=client_name,
        start_month=start_month,
        end_month=end_month,
        account_manager=account_manager,       
    )
@router.get("/client-total-allowances")
def client_total_allowances(
    start_month: str | None = None,
    end_month: str | None = None,
    top: str | None = None,
    db: Session = Depends(get_db)
):
    return get_client_total_allowances(db, start_month, end_month, top)

@router.get("/client-departments", response_model=list[ClientDeptResponse] | ClientDeptResponse)
def get_client_departments(client: str | None = None, db: Session = Depends(get_db),current_user=Depends(get_current_user)):
    return get_client_departments_service(db, client)
 