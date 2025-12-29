from datetime import date
from fastapi.testclient import TestClient
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from utils.client_enums import Company

from sqlalchemy.sql import func
func.to_char = lambda col, fmt: col  

# API ROUTES
SEARCH_EMPLOYEE_URL = "/employee-details/search"

# /employee-details/search API TESTCASES

def test_search_employee_success(client: TestClient, db_session):
    db_session.query(ShiftMapping).delete()
    db_session.query(ShiftAllowances).delete()
    db_session.query(ShiftsAmount).delete()
    db_session.commit()
    allowance = ShiftAllowances(emp_id="IN01804396", emp_name="Test User", grade="L1", department="IT",
                                client=Company.ATD.value, project="P", account_manager="M",
                                duration_month=date(2024,1,1), payroll_month=date(2024,2,1))
    db_session.add_all([allowance, ShiftsAmount(shift_type="A", amount=500, payroll_year=2024)])
    db_session.commit()
    db_session.add(ShiftMapping(shiftallowance_id=allowance.id, shift_type="A", days=2))
    db_session.commit()
    
    res = client.get(SEARCH_EMPLOYEE_URL, params={"start_month":"2024-01","end_month":"2024-02"}).json()
    emp = res["data"]["employees"][0]
    assert res["total_records"] == 1
    assert emp["emp_id"] == "IN01804396"
    assert res["shift_details"]["A(9PM to 6AM)"] == 2
    assert res["shift_details"]["total_allowance"] == 1000


def test_search_employee_no_data(client: TestClient, db_session):
    db_session.query(ShiftMapping).delete()
    db_session.query(ShiftAllowances).delete()
    db_session.query(ShiftsAmount).delete()
    db_session.commit()

    response = client.get(SEARCH_EMPLOYEE_URL, params={"start_month": "2024-01", "end_month": "2024-02"})
    data = response.json()

    assert response.status_code == 404
    assert "No data found" in data["detail"]



def test_search_employee_invalid_month_format(client: TestClient):
    response = client.get(SEARCH_EMPLOYEE_URL, params={"start_month": "Jan-2024"})
    assert response.status_code == 400
    assert "YYYY-MM" in response.json()["detail"]


def test_search_employee_future_month(client: TestClient):
    response = client.get(SEARCH_EMPLOYEE_URL, params={"start_month": "2099-01"})
    assert response.status_code == 400
    assert "future month" in response.json()["detail"].lower()


def test_search_employee_start_month_greater_than_end_month(client: TestClient):
    response = client.get(
        SEARCH_EMPLOYEE_URL,
        params={"start_month": "2024-05", "end_month": "2024-01"}
    )
    assert response.status_code == 400
    assert "greater than" in response.json()["detail"].lower()


def test_search_employee_end_month_without_start(client: TestClient):
    response = client.get(SEARCH_EMPLOYEE_URL, params={"end_month": "2024-02"})
    data = response.json()

    assert response.status_code == 400
    assert "start_month is required" in data["detail"]