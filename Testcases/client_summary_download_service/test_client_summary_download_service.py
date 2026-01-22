"""
Client summary download API test cases.

This module contains integration tests for the `/client-summary/download`
endpoint, validating successful downloads, error handling for invalid
clients, and scenarios where no data is available.
"""

from datetime import date
from fastapi.testclient import TestClient
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from services import client_summary_download_service as service

#API ROUTES
DOWNLOAD_URL = "/client-summary/download"

#HELPER FUNCTION
def setup_data(db):
    """
    Seed the database with valid shift allowance, shift mapping,
    and shift amount data for download tests.

    Args:
        db: SQLAlchemy database session fixture.
    """
    db.query(ShiftMapping).delete()
    db.query(ShiftsAmount).delete()
    db.query(ShiftAllowances).delete()

    test_date = date(2024, 1, 1)

    allowance = ShiftAllowances(
        emp_id="E01",
        emp_name="User",
        client="ClientA",
        department="IT",
        account_manager="AM",
        duration_month=test_date,
        payroll_month=test_date,
    )

    db.add(allowance)
    db.flush()

    db.add_all(
        [
            ShiftMapping(
                shiftallowance_id=allowance.id,
                shift_type="A",
                days=5,
            ),
            ShiftsAmount(
                shift_type="A",
                payroll_year=2024,
                amount=100,
            ),
        ]
    )

    db.commit()

# /client-summary/download API TESTCASES
def test_download_all_clients(client: TestClient, db_session, monkeypatch):
    """
    Verify successful Excel download when requesting data
    for all clients.
    """
    setup_data(db_session)

    def mock_client_summary_service(_db, _payload):
        """
        Mock client summary service returning valid summary data.
        """
        return {
            "2024-01": {
                "clients": {
                    "ClientA": {
                        "account_manager": "AM",
                        "departments": {
                            "IT": {
                                "dept_head_count": 1,
                                "dept_A": 500,
                                "dept_B": 0,
                                "dept_C": 0,
                                "dept_PRIME": 0,
                                "dept_total": 500,
                                "employees": [
                                    {
                                        "emp_id": "E01",
                                        "account_manager": "AM",
                                        "A": 500,
                                        "B": 0,
                                        "C": 0,
                                        "PRIME": 0,
                                        "total": 500,
                                    }
                                ],
                            }
                        },
                    }
                }
            }
        }

    monkeypatch.setattr(
        service,
        "client_summary_service",
        mock_client_summary_service,
    )

    payload = {
        "clients": "ALL",
        "selected_year": "2024",
        "selected_months": ["01"],
    }

    response = client.post(DOWNLOAD_URL, json=payload)

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


def test_download_valid_client_but_no_data(
    client: TestClient,
    db_session,
    monkeypatch,
):
    """
    Verify 404 response when a valid client is provided
    but no data exists for the selected filters.
    """
    setup_data(db_session)

    def mock_empty_summary(_db, _payload):
        """
        Mock service returning no summary data.
        """
        return {}

    monkeypatch.setattr(
        service,
        "client_summary_service",
        mock_empty_summary,
    )

    payload = {
        "clients": {"ClientA": []},
        "selected_year": "2024",
        "selected_months": ["01"],
    }

    response = client.post(DOWNLOAD_URL, json=payload)

    assert response.status_code == 404
    assert "No data" in response.json()["detail"]


def test_download_invalid_client_name(
    client: TestClient,
    db_session,
    monkeypatch,
):
    """
    Verify 404 response when an invalid client name
    is provided.
    """
    setup_data(db_session)

    def mock_empty_summary(_db, _payload):
        """
        Mock service returning no summary data.
        """
        return {}

    monkeypatch.setattr(
        service,
        "client_summary_service",
        mock_empty_summary,
    )

    payload = {
        "clients": {"InvalidClient": []},
        "selected_year": "2024",
        "selected_months": ["01"],
    }

    response = client.post(DOWNLOAD_URL, json=payload)

    assert response.status_code == 404
    assert "No data" in response.json()["detail"]


def test_download_no_data(client: TestClient, db_session, monkeypatch):
    """
    Verify 404 response when no shift data exists
    in the system.
    """
    db_session.query(ShiftMapping).delete()
    db_session.query(ShiftsAmount).delete()
    db_session.query(ShiftAllowances).delete()
    db_session.commit()

    def mock_empty_summary(_db, _payload):
        """
        Mock service returning no summary data.
        """
        return {}

    monkeypatch.setattr(
        service,
        "client_summary_service",
        mock_empty_summary,
    )

    payload = {
        "clients": "ALL",
        "selected_year": "2024",
        "selected_months": ["01"],
    }

    response = client.post(DOWNLOAD_URL, json=payload)

    assert response.status_code == 404
    assert "No data" in response.json()["detail"]
