from datetime import date
from sqlalchemy.sql import func
from models.models import ShiftAllowances
func.date_trunc = lambda part, col: col 

# API ROUTES
EXCEL_URL = "/excel/download"

# HELPER FUNCTION
def seed_excel_data(db):
    db.query(ShiftAllowances).delete()
    db.add(
        ShiftAllowances(
            emp_id="E01",
            emp_name="User",
            duration_month=date(2024, 1, 1),
            payroll_month=date(2024, 2, 1),
        )
    )
    db.commit()

  
# /excel/download API TESTCASES

def test_download_excel_basic(client, db_session):
    seed_excel_data(db_session)

    resp = client.get(EXCEL_URL, params={"start_month": "2024-01"})
    assert resp.status_code == 200



def test_download_excel_filtered(client, db_session):
    seed_excel_data(db_session)

    resp = client.get(
        EXCEL_URL,
        params={"emp_id": "E01", "start_month": "2024-01"},
    )
    assert resp.status_code == 200



def test_download_excel_invalid_month(client):
    resp = client.get(EXCEL_URL, params={"start_month": "2024/01"})
    assert resp.status_code == 400



def test_download_excel_start_after_end(client):
    resp = client.get(
        EXCEL_URL,
        params={"start_month": "2024-05", "end_month": "2024-01"},
    )
    assert resp.status_code == 400
