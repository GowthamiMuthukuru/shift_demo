"""
Dashboard routes for graphs, charts, and client summaries.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user

from schemas.dashboardschema import (
    VerticalGraphResponse,
    PieChartClientShift,
    ClientList,
    DashboardFilterRequest,
    ClientAnalyticsRequest
)

from services.dashboard_service import (
    get_horizontal_bar_service,
    get_graph_service,
    get_vertical_bar_service,
    get_piechart_shift_summary,
    get_all_clients_service,
    get_client_dashboard_summary,client_analytics_service
)


router = APIRouter(prefix="/dashboard", tags=["Dashboard"])

@router.get("/horizontal-bar", response_model=dict)
def get_horizontal_bar(
    start_month: str | None = None,
    end_month: str | None = None,
    top: int | None = None,
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user)
):
    """Return horizontal bar chart data."""
    return get_horizontal_bar_service(db, start_month,
                                      end_month,top)

@router.get("/graph", response_model=dict)
def get_graph(
    client_name: str,
    start_month: str | None = None,
    end_month: str | None = None,
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user)
):
    """Return line-graph data for a specific client."""
    return get_graph_service(db, client_name,
                             start_month,end_month)

@router.get("/clients", response_model=ClientList)
def get_clients(db: Session = Depends(get_db)):
    """Return list of all clients."""
    return get_all_clients_service(db)


@router.get("/piechart", response_model=list[PieChartClientShift])
def get_piechart(
    start_month: str | None = None,
    end_month: str | None = None,
    top: str | None = None,
    db: Session = Depends(get_db),
    _current_user = Depends(get_current_user)
):
    """Return pie chart shift summary."""
    return get_piechart_shift_summary(db, start_month, end_month, top)


@router.get("/vertical-bar", response_model=list[VerticalGraphResponse])
def get_vertical_bar(
    start_month: str | None = None,
    end_month: str | None = None,
    top: str | None = None,
    db: Session = Depends(get_db),
    _current_user = Depends(get_current_user)
):
    """Return vertical bar chart data."""
    return get_vertical_bar_service(db, start_month,
                                    end_month,top)


@router.post("/client-allowance-summary")
def client_dashboard_summary(
    payload: DashboardFilterRequest,
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    # Force dict (Pydantic v2)
    payload_dict = payload.model_dump(exclude_none=True)
    return get_client_dashboard_summary(db, payload_dict)


@router.post("/client-analytics")
def client_analytics(payload: ClientAnalyticsRequest, db: Session = Depends(get_db)):
    return client_analytics_service(db, payload.dict())