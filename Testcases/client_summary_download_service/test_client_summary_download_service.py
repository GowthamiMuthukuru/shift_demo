
from datetime import date
from fastapi.testclient import TestClient
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from services import client_summary_download_service as service

# API ROUTES
DOWNLOAD_URL = "/client-summary/download"


# HELPER FUNCTION 
def setup_data(db):
    db.query(ShiftMapping).delete()
    db.query(ShiftsAmount).delete()
    db.query(ShiftAllowances).delete()

    d = date(2024, 1, 1)
    sa = ShiftAllowances(
        emp_id="E01",
        emp_name="User",
        client="ClientA",
        department="IT",
        account_manager="AM",
        duration_month=d,
        payroll_month=d,
    )
    db.add(sa)
    db.flush()

    db.add_all([
        ShiftMapping(shiftallowance_id=sa.id, shift_type="A", days=5),
        ShiftsAmount(shift_type="A", payroll_year=2024, amount=100),
    ])
    db.commit()


# /client-summary/download API TESTCASES

def test_download_all_clients(client: TestClient, db_session, monkeypatch):
    setup_data(db_session)

    def mock_fetch_rows(*args, **kwargs):
        class Row:
            duration_month = date(2024, 1, 1)
            client = "ClientA"
            department = "IT"
            emp_id = "E01"
            emp_name = "User"
            account_manager = "AM"
            shift_type = "A"
            days = 5
            amount = 100
        return [Row()]

    monkeypatch.setattr(service, "fetch_rows", mock_fetch_rows)

    payload = {
        "clients": "ALL",
        "selected_year": "2024",
        "selected_months": ["01"],
    }

    resp = client.post(DOWNLOAD_URL, json=payload)
    assert resp.status_code == 200


def test_download_valid_client_but_no_data(client: TestClient, db_session):
    setup_data(db_session)

    payload = {
        "clients": {"ClientA": []},  
        "selected_year": "2024",
        "selected_months": ["01"],
    }
    resp = client.post(DOWNLOAD_URL, json=payload)
    assert resp.status_code == 404


def test_download_invalid_client_name(client: TestClient, db_session):
    setup_data(db_session)

    payload = {
        "clients": {"InvalidClient": []},  
        "selected_year": "2024",
        "selected_months": ["01"],
    }
    resp = client.post(DOWNLOAD_URL, json=payload)
    assert resp.status_code == 404


def test_download_no_data(client: TestClient, db_session):
    db_session.query(ShiftMapping).delete()
    db_session.query(ShiftsAmount).delete()
    db_session.query(ShiftAllowances).delete()
    db_session.commit()

    payload = {
        "clients": "ALL",
        "selected_year": "2024",
        "selected_months": ["01"],
    }
    resp = client.post(DOWNLOAD_URL, json=payload)
    assert resp.status_code == 404
