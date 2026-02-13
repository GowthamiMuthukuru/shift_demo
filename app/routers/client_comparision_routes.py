"""
Routes for client comparison, total allowances, and department-wise data.
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from db import get_db
from schemas.displayschema import ClientDeptResponse
from schemas.dashboardschema import ClientTotalAllowanceFilter,DashboardFilter,DashboardResponse
from services.client_comparision_service import (client_comparison_service,
                                                 get_client_total_allowances,
                                                 get_client_departments_service,get_client_dashboard)
from utils.dependencies import get_current_user
from typing import Optional

router = APIRouter()

@router.get("/client-comparison")
def client_comparison(
    client_name: str = Query(..., alias="client"),
    start_month: str | None = Query(None),
    end_month: str | None = Query(None),
    account_manager: str | None = Query(None),
    db: Session = Depends(get_db),
    _current_user = Depends(get_current_user)
):
    """Return comparison data for a client within a date range."""
    return client_comparison_service(
        db=db,
        client_name=client_name,
        start_month=start_month,
        end_month=end_month,
        account_manager=account_manager,
    )
@router.post("/client-total-allowances-piechart")
def client_total_allowances(
    filters: ClientTotalAllowanceFilter,
    db: Session = Depends(get_db),
    _current_user = Depends(get_current_user)
):
    """Return total allowances grouped by client with filters."""
    return get_client_total_allowances(db, filters)

@router.get("/client-departments")
def get_client_departments(
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """Return all unique departments."""
    return get_client_departments_service(db)




@router.post("/dashboard", response_model=DashboardResponse)
def dashboard(
    filters: DashboardFilter,
    client_starts_with: Optional[str] = Query(
        None,
        description="Filter clients starting with given prefix (e.g., M)"
    ),
    db: Session = Depends(get_db),
    _current_user = Depends(get_current_user)
):
    """
    Returns dashboard summary per client with period resolution, future messages,
    and all filters applied.

    client_starts_with can be passed as query param instead of body.
    
    """

    if client_starts_with:
        filters.client_starts_with = client_starts_with

    return get_client_dashboard(db, filters)

