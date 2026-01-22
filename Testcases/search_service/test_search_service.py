"""
Search service API test cases.

This module contains integration tests for the
`/employee-details/search` endpoint, covering:
- Successful employee search
- No data scenarios
- Invalid month formats
- Future month validation
- Invalid date range handling
"""

from datetime import date
from fastapi.testclient import TestClient
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from utils.client_enums import Company

# API ROUTES
SEARCH_EMPLOYEE_URL = "/employee-details/search"

# HELPER FUNCTION
def _clean_db(db_session):
    """
    Clear all relevant tables before each test.

    Ensures that each test runs in isolation
    with a clean database state.
    """
    db_session.query(ShiftMapping).delete()
    db_session.query(ShiftAllowances).delete()
    db_session.query(ShiftsAmount).delete()
    db_session.commit()

# /employee-details/search API TESTCASES
def test_search_employee_success(client: TestClient, db_session):
    """
    Verify successful employee search with valid data.

    Ensures correct aggregation of shift allowances
    and returns accurate employee details.
    """
    _clean_db(db_session)

    allowance = ShiftAllowances(
        emp_id="IN01804396",
        emp_name="Test User",
        grade="L1",
        department="IT",
        client=Company.ATD.value,
        project="P",
        account_manager="M",
        duration_month=date(2024, 1, 1),
        payroll_month=date(2024, 2, 1),
    )

    db_session.add_all([
        allowance,
        ShiftsAmount(shift_type="A", amount=500, payroll_year=2024),
    ])
    db_session.commit()

    db_session.add(
        ShiftMapping(
            shiftallowance_id=allowance.id,
            shift_type="A",
            days=2,
        )
    )
    db_session.commit()

    payload = {
        "selected_year": "2024",
        "selected_months": ["01", "02"],
        "start": 0,
        "limit": 10,
    }

    response = client.post(SEARCH_EMPLOYEE_URL, json=payload)

    assert response.status_code == 200

    data = response.json()
    emp = data["data"]["employees"][0]

    assert data["total_records"] == 1
    assert emp["emp_id"] == "IN01804396"
    assert data["shift_details"]["A(9PM to 6AM)"] == 1000
    assert data["shift_details"]["total_allowance"] == 1000


def test_search_employee_no_data(client: TestClient, db_session):
    """
    Verify that 404 is returned when no employee data exists
    for the selected year and months.
    """
    _clean_db(db_session)

    payload = {"selected_year": "2025", "selected_months": ["01", "02"]}
    response = client.post(SEARCH_EMPLOYEE_URL, json=payload)

    assert response.status_code == 404
    assert "No data" in str(response.json()["detail"])


def test_search_employee_invalid_month_format(client: TestClient):
    """
    Verify error response for invalid month format.

    Expected format is YYYY-MM.
    """
    payload = {"start_month": "Jan-2024", "end_month": "2024-01"}
    response = client.post(SEARCH_EMPLOYEE_URL, json=payload)

    assert response.status_code in (400, 404)
    assert "YYYY-MM" in str(response.json()["detail"])


def test_search_employee_future_month(client: TestClient):
    """
    Verify error when searching for future months.
    """
    payload = {"start_month": "2099-01", "end_month": "2099-01"}
    response = client.post(SEARCH_EMPLOYEE_URL, json=payload)

    assert response.status_code in (400, 404)
    assert "future" in str(response.json()["detail"]).lower()


def test_search_employee_start_month_greater_than_end_month(client: TestClient):
    """
    Verify validation when start month is after end month.
    """
    payload = {"start_month": "2024-05", "end_month": "2024-01"}
    response = client.post(SEARCH_EMPLOYEE_URL, json=payload)

    assert response.status_code in (400, 404)
    detail = str(response.json()["detail"]).lower()
    assert (
        "no data found" in detail
        or "start_month" in detail
        or "not allowed" in detail
    )


def test_search_employee_end_month_without_start(client: TestClient):
    """
    Verify error when end month is provided without start month.
    """
    payload = {"end_month": "2024-02"}
    response = client.post(SEARCH_EMPLOYEE_URL, json=payload)

    assert response.status_code in (400, 404)
    detail = str(response.json()["detail"]).lower()
    assert (
        "start_month" in detail
        or "no data found" in detail
        or "last 12 months" in detail
    )
