from fastapi.testclient import TestClient
from models.models import ShiftAllowances

# API ROUTES
CLIENT_DEPTS_URL = "/client-departments"


# HELPER FUNCTION 

def add_row(db, client, dept):
    db.add(
        ShiftAllowances(
            emp_id="E01",
            emp_name="User",
            client=client,
            department=dept,
        )
    )
    db.commit()

# /client-departments API TESTCASES

def test_get_all_clients_departments(client: TestClient, db_session):
    add_row(db_session, "ClientA", "IT")
    add_row(db_session, "ClientB", "HR")

    resp = client.get(CLIENT_DEPTS_URL)
    data = resp.json()

    assert resp.status_code == 200
    assert len(data) == 2


def test_get_specific_client_departments(client: TestClient, db_session):
    add_row(db_session, "ClientA", "IT")
    add_row(db_session, "ClientA", "HR")

    resp = client.get(CLIENT_DEPTS_URL, params={"client": "ClientA"})
    data = resp.json()

    assert resp.status_code == 200
    assert data[0]["client"] == "ClientA"
    assert sorted(data[0]["departments"]) == ["HR", "IT"]


def test_get_client_departments_invalid_input(client: TestClient, db_session):
    add_row(db_session, "ClientA", "IT")

    resp = client.get(CLIENT_DEPTS_URL, params={"client": "   "})

    assert resp.status_code == 400
    assert "cannot be empty" in resp.json()["detail"].lower()


def test_get_client_departments_not_found(client: TestClient, db_session):
    add_row(db_session, "ClientA", "IT")

    resp = client.get(CLIENT_DEPTS_URL, params={"client": "Unknown"})

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()
