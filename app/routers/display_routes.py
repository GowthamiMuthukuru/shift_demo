from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from db import get_db
from models.models import ShiftAllowances
from utils.dependencies import get_current_user
from schemas.displayschema import ShiftAllowancesResponse,EmployeeResponse


router = APIRouter(prefix="/display")

@router.get("/",response_model=list[ShiftAllowancesResponse])
def get_all_data(
    start: int = Query(0, ge=0, description="Starting row index"),
    limit: int = Query(10, gt=0, description="Number of records to fetch"),
    db: Session = Depends(get_db),
    current_user= Depends(get_current_user),
):
    # Fetch data with pagination
    data = db.query(ShiftAllowances).offset(start).limit(limit).all()

    if not data:
        raise HTTPException(status_code=404, detail="No data found for the given range")
    return data

@router.get("/{id}",response_model=EmployeeResponse)
def get_detail_page(id:int, 
                    db:Session = Depends(get_db),
                    current_user=Depends(get_current_user),):
    data = db.query(ShiftAllowances).filter(ShiftAllowances.id==id).first()
    if not data:
        raise HTTPException(status_code=404,detail="Given id doesn't exist")
    return data