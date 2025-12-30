"""
Excel download API test cases.

This module contains integration tests for the `/excel/download`
endpoint, validating successful downloads, filtering behavior,
and error handling for invalid date inputs.
"""

from datetime import date
from sqlalchemy.sql import func
from models.models import ShiftAllowances
func.date_trunc = lambda part, col: col

# API ROUTES
EXCEL_URL = "/excel/download"

# HELPER FUNCTION
def seed_excel_data(db):
    """
    Seed database with minimal data required for Excel download tests.

    Args:
        db: Database session fixture.
    """
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
    """
    Verify Excel download succeeds with minimal valid parameters.
    """
    seed_excel_data(db_session)

    resp = client.get(EXCEL_URL, params={"start_month": "2024-01"})
    assert resp.status_code == 200



def test_download_excel_filtered(client, db_session):
    """
    Verify Excel download succeeds when filtered by employee ID.
    """
    seed_excel_data(db_session)

    resp = client.get(
        EXCEL_URL,
        params={"emp_id": "E01", "start_month": "2024-01"},
    )
    assert resp.status_code == 200



def test_download_excel_invalid_month(client):
    """
    Verify request fails when month format is invalid.
    """
    resp = client.get(EXCEL_URL, params={"start_month": "2024/01"})
    assert resp.status_code == 400



def test_download_excel_start_after_end(client):
    """
    Verify request fails when start_month is after end_month.
    """
    resp = client.get(
        EXCEL_URL,
        params={"start_month": "2024-05", "end_month": "2024-01"},
    )
    assert resp.status_code == 400
