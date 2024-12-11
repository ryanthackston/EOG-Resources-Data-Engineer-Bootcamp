import pytest
from fastapi.testclient import TestClient
from main import app  # Replace `main` with the actual filename of your FastAPI app
from unittest.mock import AsyncMock, patch
from models import Employee

# Create a TestClient instance for the FastAPI app
client = TestClient(app)

# Mock data for testing
mock_employee = {
    "employee_id": 4,
    "first_name": "John",
    "last_name": "Doe",
    "email": "johndoe@example.com",
    "phone": "123-456-7890",
    "hire_date": "2023-01-01T00:00:00",
    "manager_id": None,
    "job_title": "Software Engineer",
    "is_active_fl": 1,
}

@pytest.fixture
def mock_employee_model():
    return Employee(**mock_employee)

### Test for the /save_employee endpoint
@patch("data_helper.DataHelper.call_func", new_callable=AsyncMock)
def test_save_employee(mock_call_func):
    from models import Employee  # Ensure you import the Employee model

    # Convert mock_employee to a Pydantic model
    mock_employee_model = Employee(**mock_employee)

    # Mock the database call to return an employee ID
    mock_call_func.return_value = 1

    # Perform the POST request with the Employee model
    response = client.post("/save_employee", json=mock_employee_model.dict())

    # Assert that the API response and mocked behavior are correct
    assert response.status_code == 200
    assert response.json() == 1
    mock_call_func.assert_called_once_with(
        "EMPLOYEE_PKG.Save_Employee",
        "NUMBER",
        [
            mock_employee_model.employee_id,
            mock_employee_model.first_name,
            mock_employee_model.last_name,
            mock_employee_model.email,
            mock_employee_model.phone,
            mock_employee_model.hire_date,
            mock_employee_model.manager_id,
            mock_employee_model.job_title,
        ],
    )

### Test for the /get_employee/{employee_id} endpoint
@patch("data_helperV4.DataHelper.call_proc", new_callable=AsyncMock)
def test_get_employee(mock_call_proc):
    mock_call_proc.return_value = [mock_employee]
    response = client.get("/get_employee/1")
    assert response.status_code == 200
    assert response.json() == mock_employee
    mock_call_proc.assert_called_once_with("EMPLOYEE_PKG.Get_Employee", [1], "CURSOR")

### Test for the /get_employees/{manager_id} endpoint
@patch("data_helperV4.DataHelper.call_proc", new_callable=AsyncMock)
def test_get_employees(mock_call_proc):
    mock_call_proc.return_value = [mock_employee]
    response = client.get("/get_employees/1")
    assert response.status_code == 200
    assert response.json() == [mock_employee]
    mock_call_proc.assert_called_once_with("EMPLOYEE_PKG.Get_Employees", [1], "CURSOR")

### Test for the /employees/{employee_id} DELETE endpoint
@patch("data_helperV4.DataHelper.call_proc", new_callable=AsyncMock)
def test_delete_employee(mock_call_proc):
    response = client.delete("/employees/1")
    assert response.status_code == 200
    assert response.json() == {"detail": "Employee with EMPLOYEE_ID 1 logically deleted."}
    mock_call_proc.assert_called_once_with(
        "EMPLOYEE_PKG.Delete_Employee", [1], "NONE"
    )

### Test for the /employees/json/{employee_id} endpoint
@patch("data_helperV4.DataHelper.call_proc", new_callable=AsyncMock)
def test_get_employee_json(mock_call_proc):
    mock_call_proc.return_value = [{"EMPLOYEE_JSON": '{"employee_id": 1}'}]
    response = client.get("/employees/json/1")
    assert response.status_code == 200
    assert response.json() == {"employee_id": 1}
    mock_call_proc.assert_called_once_with(
        "EMPLOYEE_PKG.Get_Employee_JSON_CURSOR", [1], "CURSOR"
    )

### Test for the /employees/json_clob/{employee_id} endpoint
@patch("data_helperV4.DataHelper.call_proc", new_callable=AsyncMock)
def test_get_employee_json_clob(mock_call_proc):
    mock_call_proc.return_value = '{"employee_id": 1}'
    response = client.get("/employees/json_clob/1")
    assert response.status_code == 200
    assert response.json() == {"employee_id": 1}
    mock_call_proc.assert_called_once_with(
        "EMPLOYEE_PKG.Get_Employee_JSON_CLOB", [1], "CLOB"
    )

### Test for the /employees/json_clob_manager/{manager_id} endpoint
@patch("data_helperV4.DataHelper.call_proc", new_callable=AsyncMock)
def test_get_employees_json_clob_manager(mock_call_proc):
    mock_call_proc.return_value = '[{"employee_id": 1}]'
    response = client.get("/employees/json_clob_manager/1")
    assert response.status_code == 200
    assert response.json() == [{"employee_id": 1}]
    mock_call_proc.assert_called_once_with(
        "EMPLOYEE_PKG.Get_Employees_JSON_CLOB", [1], "CLOB"
    )

### Test for the /save_employees_json endpoint
@patch("data_helperV4.DataHelper.call_proc", new_callable=AsyncMock)
def test_save_employees_json(mock_call_proc, mock_employee_model):
    mock_call_proc.return_value = 1
    employees = [mock_employee]
    response = client.post("/save_employees_json", json=employees)
    assert response.status_code == 200
    assert response.json() == [1]
    for emp in employees:
        mock_call_proc.assert_any_call(
            "EMPLOYEE_PKG.save_employees_json", [emp], "NUMBER"
        )
