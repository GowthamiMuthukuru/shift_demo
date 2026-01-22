"""
Client summary API test cases.

This module contains integration tests for the `/client-summary`
endpoint, validating successful responses for all clients and
specific clients, as well as error handling for invalid inputs.
"""

from datetime import date
from models.models import ShiftAllowances

# API ROUTES
CLIENT_SUMMARY_URL = "/client-summary"

# HELPER FUNCTION
def seed_client_summary_data(db):
    """
    Insert minimal shift allowance data required for client summary tests.

    This helper ensures at least one employee record exists so that
    the client summary API can return valid results.
    """
    db.query(ShiftAllowances).delete()
    db.add(
        ShiftAllowances(
            emp_id="E01",
            emp_name="User",
            client="ClientA",
            department="IT",
            duration_month=date(2024, 1, 1),
            payroll_month=date(2024, 1, 1),
        )
    )
    db.commit()

# /client-summary API TESTCASES
def test_client_summary_all_clients_success(client, db_session):
    """
    Verify that requesting summary data for all clients
    returns a successful response.
    """
    seed_client_summary_data(db_session)

    response = client.post(CLIENT_SUMMARY_URL, json={"clients": "ALL"})

    assert response.status_code == 200
    assert isinstance(response.json(), dict)


def test_client_summary_specific_client_success(client, db_session):
    """
    Verify that requesting summary data for a specific client
    and department returns valid monthly data.
    """
    seed_client_summary_data(db_session)

    payload = {
        "clients": {"ClientA": ["IT"]},
        "selected_year": "2024",
        "selected_months": ["01"],
    }

    response = client.post(CLIENT_SUMMARY_URL, json=payload)

    assert response.status_code == 200
    month_data = response.json()["2024-01"]
    assert "clients" in month_data or "message" in month_data


def test_client_summary_missing_payload(client):
    """
    Verify that sending an empty payload returns data
    for the latest available month.
    """
    response = client.post(CLIENT_SUMMARY_URL, json={})

    assert response.status_code == 200
    assert isinstance(response.json(), dict)


def test_client_summary_invalid_quarter(client):
    """
    Verify that an invalid quarter value results in
    a 400 Bad Request response.
    """
    payload = {
        "clients": "ALL",
        "selected_year": "2024",
        "selected_quarters": ["Q5"],
    }

    response = client.post(CLIENT_SUMMARY_URL, json=payload)

    assert response.status_code == 400
    assert "Invalid quarter" in response.text
