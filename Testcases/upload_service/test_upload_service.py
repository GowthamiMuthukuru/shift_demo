from io import BytesIO
from datetime import date
import pandas as pd
import os
from models.models import ShiftAllowances
from utils.enums import ExcelColumnMap
from fastapi.testclient import TestClient

TEMP_FOLDER = "media/error_excels"
EXCEL_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


# API ROUTES 
UPLOAD_EXCEL_URL = "/upload/"
CORRECT_ERROR_ROWS_URL = "/upload/correct_error_rows"
ERROR_FILE_DOWNLOAD_URL = "/upload/error-files/{filename}"


# /UPLOAD/ API TESTCASES
def test_upload_valid_excel_success(client, db_session):
    row = {
        ExcelColumnMap.emp_id.value: "IN01800341",
        ExcelColumnMap.emp_name.value: "Test User",
        ExcelColumnMap.grade.value: "L2",
        ExcelColumnMap.department.value: "IT",
        ExcelColumnMap.client.value: "ABC",
        ExcelColumnMap.project.value: "Test Project",
        ExcelColumnMap.project_code.value: "PRJ001",
        ExcelColumnMap.account_manager.value: "Manager",
        ExcelColumnMap.practice_lead.value: "Practice Lead",
        ExcelColumnMap.delivery_manager.value: "Delivery Manager",
        ExcelColumnMap.duration_month.value: "Jan'25",
        ExcelColumnMap.payroll_month.value: "Feb'25",
        ExcelColumnMap.billability_status.value: "Billable",
        ExcelColumnMap.practice_remarks.value: "",
        ExcelColumnMap.rmg_comments.value: "",
        ExcelColumnMap.shift_a_days.value: 2,
        ExcelColumnMap.shift_b_days.value: 1,
        ExcelColumnMap.shift_c_days.value: 0,
        ExcelColumnMap.prime_days.value: 0,
        ExcelColumnMap.total_days.value: 3,
    }

    excel = BytesIO()
    pd.DataFrame([row]).to_excel(excel, index=False)
    excel.seek(0)

    response = client.post(
        UPLOAD_EXCEL_URL,
        files={"file": ("valid.xlsx", excel, EXCEL_MIME)}
    )
    assert response.status_code == 200

    db_session.expire_all()
    assert db_session.query(ShiftAllowances).count() == 1


def test_reupload_same_employee_month_overwrites(client, db_session):
    first = {
        ExcelColumnMap.emp_id.value: "IN01800341",
        ExcelColumnMap.duration_month.value: "Jan'25",
        ExcelColumnMap.payroll_month.value: "Feb'25",
        ExcelColumnMap.shift_a_days.value: 1,
        ExcelColumnMap.shift_b_days.value: 1,
        ExcelColumnMap.total_days.value: 2,
    }

    second = {
        ExcelColumnMap.emp_id.value: "IN01800341",
        ExcelColumnMap.duration_month.value: "Jan'25",
        ExcelColumnMap.payroll_month.value: "Feb'25",
        ExcelColumnMap.shift_a_days.value: 3,
        ExcelColumnMap.shift_b_days.value: 0,
        ExcelColumnMap.total_days.value: 3,
    }

    for data in (first, second):
        excel = BytesIO()
        pd.DataFrame([data]).to_excel(excel, index=False)
        excel.seek(0)
        client.post(
            UPLOAD_EXCEL_URL,
            files={"file": ("data.xlsx", excel, EXCEL_MIME)}
        )

    db_session.expire_all()
    records = db_session.query(ShiftAllowances).filter(
        ShiftAllowances.emp_id == "IN01800341",
        ShiftAllowances.duration_month == date(2025, 1, 1),
        ShiftAllowances.payroll_month == date(2025, 2, 1),
    ).all()

    assert len(records) == 1


def test_partial_invalid_rows(client):
    valid = {
        ExcelColumnMap.emp_id.value: "IN01800341",
        ExcelColumnMap.shift_a_days.value: 2,
    }
    invalid = {
        ExcelColumnMap.emp_id.value: "IN01804070",
        ExcelColumnMap.shift_a_days.value: -1,
    }

    excel = BytesIO()
    pd.DataFrame([valid, invalid]).to_excel(excel, index=False)
    excel.seek(0)

    response = client.post(
        UPLOAD_EXCEL_URL,
        files={"file": ("partial.xlsx", excel, EXCEL_MIME)}
    )
    assert response.status_code == 400


def test_missing_required_columns(client):
    row = {ExcelColumnMap.emp_id.value: "IN01801072"}

    excel = BytesIO()
    pd.DataFrame([row]).to_excel(excel, index=False)
    excel.seek(0)

    response = client.post(
        UPLOAD_EXCEL_URL,
        files={"file": ("missing.xlsx", excel, EXCEL_MIME)}
    )
    assert response.status_code == 400
    assert "detail" in response.json()


def test_all_invalid_rows(client):
    row = {
        ExcelColumnMap.shift_a_days.value: -1,
        ExcelColumnMap.shift_b_days.value: -1,
        ExcelColumnMap.total_days.value: 10,
        ExcelColumnMap.duration_month.value: "Wrong",
        ExcelColumnMap.payroll_month.value: "Wrong",
    }

    excel = BytesIO()
    pd.DataFrame([row]).to_excel(excel, index=False)
    excel.seek(0)

    response = client.post(
        UPLOAD_EXCEL_URL,
        files={"file": ("invalid.xlsx", excel, EXCEL_MIME)}
    )
    assert response.status_code == 400


def test_wrong_file_type(client):
    response = client.post(
        UPLOAD_EXCEL_URL,
        files={"file": ("test.txt", b"not excel", "text/plain")}
    )
    assert response.status_code == 400


# /upload/correct_error_rows API TESTCASES

def test_correct_error_rows_success(client, db_session):
    payload = {
        "corrected_rows": [{
            "emp_id": "IN01800119",
            "duration_month": "Jan'24",
            "payroll_month": "Feb'24",
            "project": "TEST_PROJECT",
            "shift_a_days": 5,
            "shift_b_days": 3,
            "shift_c_days": 0,
            "prime_days": 2
        }]
    }

    response = client.post(CORRECT_ERROR_ROWS_URL, json=payload)

    assert response.status_code == 200
    assert response.json()["records_processed"] == 1


def test_correct_error_rows_empty_payload(client):
    response = client.post(
        CORRECT_ERROR_ROWS_URL,
        json={"corrected_rows": []}
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "No corrected rows provided"


def test_correct_error_rows_invalid_month_format(client):
    payload = {
        "corrected_rows": [{
            "emp_id": "IN01800119",
            "duration_month": "2024-01",
            "payroll_month": "Feb'24",
            "project": "TEST",
            "shift_a_days": 2
        }]
    }

    response = client.post(CORRECT_ERROR_ROWS_URL, json=payload)

    assert response.status_code == 400
    assert response.json()["detail"]["message"] == "Validation failed"


def test_correct_error_rows_future_month(client):
    payload = {
        "corrected_rows": [{
            "emp_id": "IN01800119",
            "duration_month": "Jan'30",
            "payroll_month": "Feb'30",
            "project": "TEST",
            "shift_a_days": 1
        }]
    }

    response = client.post(CORRECT_ERROR_ROWS_URL, json=payload)

    assert response.status_code == 400
    assert "future month" in str(response.json()).lower()


def test_correct_error_rows_same_month(client):
    payload = {
        "corrected_rows": [{
            "emp_id": "IN01800119",
            "duration_month": "Jan'24",
            "payroll_month": "Jan'24",
            "project": "TEST",
            "shift_a_days": 1
        }]
    }

    response = client.post(CORRECT_ERROR_ROWS_URL, json=payload)

    assert response.status_code == 400
    assert "cannot be the same" in str(response.json()).lower()


def test_correct_error_rows_invalid_shift_days(client):
    payload = {
        "corrected_rows": [{
            "emp_id": "IN01800119",
            "duration_month": "Jan'24",
            "payroll_month": "Feb'24",
            "project": "TEST",
            "shift_a_days": -1
        }]
    }

    response = client.post(CORRECT_ERROR_ROWS_URL, json=payload)
    assert response.status_code == 400

    data = response.json()
    assert data["detail"]["message"] == "Validation failed"
    assert "shift" in data["detail"]["failed_rows"][0]["reason"].lower()


# /upload/error-files{filename} API TESTCASES

def test_download_error_file_success(client):
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    filename = "error_report.xlsx"
    file_path = os.path.join(TEMP_FOLDER, filename)

    with open(file_path, "wb") as f:
        f.write(b"dummy excel content")

    response = client.get(ERROR_FILE_DOWNLOAD_URL.format(filename=filename))

    assert response.status_code == 200
    assert response.headers["content-type"] == EXCEL_MIME

    os.remove(file_path)


def test_download_error_file_not_found(client):
    response = client.get(ERROR_FILE_DOWNLOAD_URL.format(filename="missing.xlsx"))

    assert response.status_code == 404
    assert response.json()["detail"] == "File not found"


def test_download_error_file_invalid_extension(client):
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    filename = "error_report.txt"
    file_path = os.path.join(TEMP_FOLDER, filename)

    with open(file_path, "w") as f:
        f.write("not an excel file")

    response = client.get(ERROR_FILE_DOWNLOAD_URL.format(filename=filename))

    assert response.status_code == 200
    assert response.headers["content-type"] == EXCEL_MIME

    os.remove(file_path)


def test_download_error_file_path_traversal(client):
    response = client.get(ERROR_FILE_DOWNLOAD_URL.format(filename="../secret.txt"))
    assert response.status_code == 404
   
def test_download_error_file_nested_path(client):
    response = client.get(ERROR_FILE_DOWNLOAD_URL.format(filename="subfolder/file.xlsx"))
    assert response.status_code == 404
  
def test_download_error_file_empty_filename(client):
    response = client.get("/upload/error-files/")
    assert response.status_code == 404
