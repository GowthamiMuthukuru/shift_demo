from fastapi import APIRouter, Depends,Query
from sqlalchemy.orm import Session
from schemas.dashboardschema import VerticalGraphResponse, PieChartClientShift
from db import get_db
from utils.dependencies import get_current_user
from services.dashboard_service import (
    get_horizontal_bar_service, 
    get_graph_service,
    get_all_clients_service,
    get_vertical_bar_service,
    get_piechart_shift_summary
)
from typing import List

router = APIRouter(prefix="/dashboard")

@router.get("/horizontal-bar")
def horizontal_bar(
    start_month: str = Query(...),
    end_month: str | None = Query(None),
    top: int | None = Query(None),
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_horizontal_bar_service(db, start_month, end_month, top)


@router.get("/graph")
def graph(
    client_name: str,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_graph_service(db, client_name)


@router.get("/clients")
def get_clients(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_all_clients_service(db)


@router.get("/piechart")
def piechart(
    start_month: str | None = None,
    end_month: str | None = None,
    top: str | None = None,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)


):
    return get_piechart_shift_summary(
        db=db,
        start_month=start_month,
        end_month=end_month,
        top=top
    )


@router.get("/vertical-bar", response_model=List[dict])
def vertical_bar(
    start_month: str | None = None,
    end_month: str | None = None,
    top: int | None = None,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_vertical_bar_service(
        db=db,
        start_month=start_month,
        end_month=end_month,
        top=top
    )

