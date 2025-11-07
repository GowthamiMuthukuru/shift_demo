from fastapi import APIRouter, UploadFile, Depends, HTTPException
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user
from services.upload_service import process_excel_upload

router = APIRouter(prefix="/upload")

@router.post("/")
async def upload_excel(
    file: UploadFile,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    try:
        result = await process_excel_upload(file=file, db=db, user=current_user)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
