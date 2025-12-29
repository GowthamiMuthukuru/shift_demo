from datetime import date
from fastapi.testclient import TestClient
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount

# API ROUTES
DASHBOARD_URL = "/dashboard/client-allowance-summary"


# HELPER FUNCTION 

def seed_dashboard_data(db):
    db.query(ShiftAllowances).delete(); db.query(ShiftMapping).delete(); db.query(ShiftsAmount).delete(); db.commit()
    sa = ShiftAllowances(emp_id="E01", emp_name="Test User", client="ClientA", department="IT",
                         account_manager="AM1", duration_month=date(2024,1,1), payroll_month=date(2024,1,1))
    db.add(sa); db.commit()
    db.add(ShiftMapping(shiftallowance_id=sa.id, shift_type="A", days=5))
    db.add(ShiftsAmount(shift_type="A", payroll_year=2024, amount=100)); db.commit()



# /dashboard/client-allowance-summary API TESTCASES


def test_dashboard_all_clients_success(client: TestClient, db_session):
    seed_dashboard_data(db_session)
    resp = client.post(DASHBOARD_URL, json={"clients": "ALL"})
    assert resp.status_code == 200
    data = resp.json()["dashboard"]
    assert data["total_allowance"] > 0
    assert data["head_count"] == 1



def test_dashboard_specific_client_success(client: TestClient, db_session):
    seed_dashboard_data(db_session)
    payload = {"clients": {"ClientA": ["IT"]}, "selected_year": "2024", "selected_months": ["01"]}
    resp = client.post(DASHBOARD_URL, json=payload)
    assert resp.status_code == 200
    dashboard = resp.json()["dashboard"]
    assert "ClientA" in dashboard["clients"]



def test_dashboard_start_month_and_year_error(client: TestClient):
    payload = {"clients": "ALL", "start_month": "2024-01", "selected_year": "2024"}
    resp = client.post(DASHBOARD_URL, json=payload)
    assert resp.status_code == 400
    assert "not both" in resp.json()["detail"]


def test_dashboard_start_after_end_error(client: TestClient):
    payload = {"clients": "ALL", "start_month": "2024-05", "end_month": "2024-01"}
    resp = client.post(DASHBOARD_URL, json=payload)
    assert resp.status_code == 400
    assert "less than or equal" in resp.json()["detail"]
