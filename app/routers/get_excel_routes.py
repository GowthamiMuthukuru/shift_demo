from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
import io
from db import get_db
from utils.dependencies import get_current_user
from services.get_excel_service import export_filtered_excel

router = APIRouter(prefix="/excel")


@router.get("/download")
def download_excel(
    emp_id: str | None = Query(None),
    account_manager: str | None = Query(None),
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
):

    df = export_filtered_excel(db, emp_id, account_manager)

    # Convert DataFrame to file stream
    file_stream = io.BytesIO()
    df.to_excel(file_stream, index=False)
    file_stream.seek(0)

    return StreamingResponse(
        file_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=filtered_or_full.xlsx"}
    )
