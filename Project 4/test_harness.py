import requests
import json
from datetime import datetime
from models import Employee

BASE_URL = "http://127.0.0.1:8000"

# Helper function to print response
def print_response(response):
    if response.status_code == 200:
        print("Success:")
        print(json.dumps(response.json(), indent=4))
    else:
        print(f"Error {response.status_code}: {response.text}")

# Test for create_or_update_employee endpoint
def test_create_or_update_employee(employee: Employee):
    print("Testing /save_employees endpoint...")
    employee_dict = employee.dict()
    employee_dict["HIRE_DATE"] = employee_dict["HIRE_DATE"].strftime('%Y-%m-%d %H:%M:%S')
    response = requests.post(f"{BASE_URL}/save_employees", json=employee_dict)
    print_response(response)

# Test for create_or_update_employee_json endpoint
def test_create_or_update_employee_json(employee: Employee):
    print("Testing /save_employees_json endpoint...")
    employee_dict = employee.dict()
    employee_dict["HIRE_DATE"] = employee_dict["HIRE_DATE"].strftime('%Y-%m-%d %H:%M:%S')
    response = requests.post(f"{BASE_URL}/save_employees_json", json=employee_dict)
    print_response(response)

# Test for get_employee endpoint
def test_get_employee(employee_id: int):
    print(f"Testing /get_employee/{employee_id} endpoint...")
    response = requests.get(f"{BASE_URL}/get_employee/{employee_id}")
    print_response(response)

# Test for get_employees endpoint
def test_get_employees(manager_id: int):
    print(f"Testing /employees?manager_id={manager_id} endpoint...")
    response = requests.get(f"{BASE_URL}/employees", params={"manager_id": manager_id})
    print_response(response)

# Test for delete_employee endpoint
def test_delete_employee(employee_id: int):
    print(f"Testing /employees/{employee_id} endpoint...")
    response = requests.delete(f"{BASE_URL}/employees/{employee_id}")
    print_response(response)

# Test for get_employee_json endpoint
def test_get_employee_json(employee_id: int):
    print(f"Testing /employees/json/{employee_id} endpoint...")
    response = requests.get(f"{BASE_URL}/employees/json/{employee_id}")
    print_response(response)

# Test for get_employee_json_clob endpoint
def test_get_employee_json_clob(employee_id: int):
    print(f"Testing /employees/json_clob/{employee_id} endpoint...")
    response = requests.get(f"{BASE_URL}/employees/json_clob/{employee_id}")
    print_response(response)

# Test for get_employees_json_clob_manager endpoint
def test_get_employees_json_clob_manager(manager_id: int):
    print(f"Testing /employees/json_clob_manager/{manager_id} endpoint...")
    response = requests.get(f"{BASE_URL}/employees/json_clob_manager/{manager_id}")
    print_response(response)

# Test for save_employees_json endpoint
def test_save_employees_json(employees):
    print("Testing /save_employees_json endpoint...")
    employee_dicts = []
    for emp in employees:
        emp_dict = emp.dict()
        emp_dict["HIRE_DATE"] = emp_dict["HIRE_DATE"].strftime('%Y-%m-%d %H:%M:%S')
        employee_dicts.append(emp_dict)
    
    response = requests.post(f"{BASE_URL}/save_employees_json", json=employee_dicts)
    print_response(response)

if __name__ == "__main__":
    # Sample employee data for testing
    sample_employee = Employee(
        EMPLOYEE_ID=112,
        FIRST_NAME="Annabelle",
        LAST_NAME="Dunn",
        EMAIL="annabelle.dunn@example.com",
        PHONE="515-123-4444",
        HIRE_DATE=datetime(2016, 9, 17),
        MANAGER_ID=2,
        JOB_TITLE="Administration Assistant",
        IS_ACTIVE_FL=1
    )

    # Sample list of employees for testing bulk operations
    sample_employees = [
        Employee(
            EMPLOYEE_ID=113,
            FIRST_NAME="Jane",
            LAST_NAME="Doe",
            EMAIL="jane.doe@example.com",
            PHONE="098-765-4321",
            HIRE_DATE=datetime(2016, 9, 17),
            MANAGER_ID=2,
            JOB_TITLE="Project Manager",
            IS_ACTIVE_FL=1
        ),
        Employee(
            EMPLOYEE_ID=114,
            FIRST_NAME="Alice",
            LAST_NAME="Smith",
            EMAIL="alice.smith@example.com",
            PHONE="555-123-4567",
            HIRE_DATE=datetime(2016, 9, 17),
            MANAGER_ID=2,
            JOB_TITLE="Business Analyst",
            IS_ACTIVE_FL=1
        )
    ]

    # Run tests
    test_create_or_update_employee(sample_employee)
    test_get_employee(112)
    test_get_employees(2)
    test_delete_employee(112)
    test_get_employee_json(113)
    test_get_employee_json_clob(113)
    test_get_employees_json_clob_manager(2)
    test_save_employees_json(sample_employees)
