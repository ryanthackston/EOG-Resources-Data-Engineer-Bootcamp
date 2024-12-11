import os
import oracledb
import sys
import json
import asyncio
from fastapi import APIRouter, FastAPI, HTTPException, Response, Depends
from typing import Optional, List, Any
from data_helperV4 import DataHelper
from models import Employee
from dotenv import load_dotenv
from datetime import datetime
#from singleton_data_helper import data_helper

# Load environment variables from .env file
load_dotenv()

app = FastAPI()


# Initialize the DataHelper instance at startup
@app.on_event("startup")
def startup_event():
    print("Starting up...")
    DataHelper.instance().init_pool()

@app.on_event("shutdown")
def shutdown_event():
    print("Shutting down...")
    DataHelper.instance().close_pool()


def handle_oracle_exception(e):
    error, = e.args
    raise HTTPException(status_code=500, detail=f"Oracle-Error-Code: {error.code}, Message: {error.message}")

# Helper Function: Transform JSON string to Employee Model
def transform_json_to_employee(data: str) -> Employee:
    employee_data = json.loads(data)
    return Employee.parse_obj(employee_data)



async def save_employee(employee_data: Employee, database: DataHelper = Depends(DataHelper.instance)) -> int:
    try:
        # Convert Employee Pydantic model to a dictionary
        employee_dict = employee_data.dict()

        # Reformat the hire_date using the DataHelper's format_hire_date function
        employee_dict["hire_date"] = DataHelper.format_hire_date(employee_dict.get("hire_date"))

        # Call the function using DataHelper's call_func
        params = [
            employee_dict.get("employee_id"),
            employee_dict.get("first_name"),
            employee_dict.get("last_name"),
            employee_dict.get("email"),
            employee_dict.get("phone"),
            employee_dict.get("hire_date"),
            employee_dict.get("manager_id"),
            employee_dict.get("job_title")
        ]

        employee_id = await database.call_func(
            "EMPLOYEE_PKG.Save_Employee",
            "NUMBER",
            params
        )
        return int(employee_id)
    except oracledb.DatabaseError as e:
        error, = e.args
        print(f"Database Error: {error}")
        raise HTTPException(status_code=500, detail=f"Oracle-Error-Code: {error.code}, Message: {error.message}")
    except Exception as e:
        print(f"Unexpected Error: {str(e)}")
        raise RuntimeError(f"Error in save_employee: {str(e)}")


@app.post("/save_employee", response_model=int)
async def create_or_update_employee(employee: Employee, database: DataHelper = Depends(DataHelper.instance)):
    employee_id = await save_employee(employee, database)
    return employee_id



@app.get("/get_employee/{employee_id}", response_model=Employee)
async def get_employee(employee_id: int, conn=Depends(DataHelper.instance().get_db)):
    try:
        async with conn as connection:
            result = await DataHelper.instance().call_proc(
                "EMPLOYEE_PKG.Get_Employee",
                [employee_id],
                return_type="CURSOR",
                connection=connection
            )
            if not result:
                raise HTTPException(status_code=404, detail="Employee not found")
            employee_data = DataHelper.transform_employee_result(result[0])
            return Employee(**employee_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

@app.get("/get_employees/{manager_id}", response_model=List[Employee])
async def get_employees(manager_id: int, conn=Depends(DataHelper.instance().get_db)):
    """
    Fetch all employees under a specific manager and return them as a list of Employee models.
    """
    try:
        async with conn as connection:
            # Use call_proc with the injected connection
            results = await DataHelper.instance().call_proc(
                "EMPLOYEE_PKG.Get_Employees",
                [manager_id],
                return_type="CURSOR",
                connection=connection
            )

            if not results:
                raise HTTPException(status_code=404, detail="No employees found for the specified manager.")

            # Transform the results into Employee model dictionaries
            employees = [Employee(**DataHelper.transform_employee_result(emp)) for emp in results]
            return employees
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")




@app.delete("/employees/{employee_id}")
def delete_employee(employee_id: int, conn=Depends(DataHelper.instance().get_db)):
    """
    Logically delete an employee by setting their `IS_ACTIVE_FL` flag to 0.
    """
    try:
        # Use call_proc with the injected connection
        DataHelper.instance().call_proc(
            "EMPLOYEE_PKG.Delete_Employee",
            [employee_id],
            return_type="NONE",  # No return value expected
            connection=conn
        )
        return {"detail": f"Employee with EMPLOYEE_ID {employee_id} logically deleted."}

    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))





@app.get("/employees/json/{employee_id}")
async def get_employee_json(employee_id: int, conn=Depends(DataHelper.instance().get_db)):
    """
    Fetch an employee record as JSON using the `EMPLOYEE_PKG.Get_Employee_JSON_CURSOR` procedure.
    """
    try:
        async with conn as connection:
            # Use call_proc to execute the procedure and get the result
            result = await DataHelper.instance().call_proc(
                "EMPLOYEE_PKG.Get_Employee_JSON_CURSOR",
                [employee_id],
                return_type="CURSOR",
                connection=connection
            )

            if not result:
                raise HTTPException(status_code=404, detail="Employee not found")

            # The first row contains the LOB content
            lob_content = result[0][0]
            lob_string = lob_content.read() if hasattr(lob_content, "read") else lob_content

            # Parse the LOB content as JSON and return
            return json.loads(lob_string)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")




@app.get("/employees/json_clob/{employee_id}")
async def get_employee_json_clob(employee_id: int, conn=Depends(DataHelper.instance().get_db)):
    """
    Fetch an employee record as JSON using the `EMPLOYEE_PKG.Get_Employee_JSON_CLOB` procedure.
    """
    try:
        async with conn as connection:
            # Use call_proc to execute the procedure and retrieve the CLOB
            clob_value = await DataHelper.instance().call_proc(
                "EMPLOYEE_PKG.Get_Employee_JSON_CLOB",
                [employee_id],
                return_type="CLOB",
                connection=connection
            )

            if not clob_value:
                raise HTTPException(status_code=404, detail="Employee not found")

            # Read and parse the CLOB content
            employee_data_str = (
                await asyncio.to_thread(clob_value.read)
                if hasattr(clob_value, "read")
                else str(clob_value)
            )
            employee_data = json.loads(employee_data_str)  # Parse the string into JSON

            return employee_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")



    
# GET /employees/json_clob_manager/{manager_id}
@app.get("/employees/json_clob_manager/{manager_id}")
async def get_employees_json_clob_manager(manager_id: int, conn=Depends(DataHelper.instance().get_db)):
    """
    Fetch employees under a manager as JSON using the `EMPLOYEE_PKG.Get_Employees_JSON_CLOB` procedure.
    """
    try:
        async with conn as connection:
            # Use call_proc to execute the procedure and retrieve the CLOB
            clob_value = await DataHelper.instance().call_proc(
                "EMPLOYEE_PKG.Get_Employees_JSON_CLOB",
                [manager_id],
                return_type="CLOB",
                connection=connection
            )

            if not clob_value:
                raise HTTPException(status_code=404, detail="No employees found for the specified manager.")

            # Read and parse the CLOB content
            employees_data_str = (
                await asyncio.to_thread(clob_value.read)
                if hasattr(clob_value, "read")
                else str(clob_value)
            )
            employees_data = json.loads(employees_data_str)  # Parse the string into JSON

            # Convert each employee in the data to an Employee Pydantic model
            employees = [Employee.parse_obj(emp) for emp in employees_data]
            return employees
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    


@app.post("/save_employees_json", response_model=List[int])
async def save_employees_json(employees: List[Employee], conn=Depends(DataHelper.instance().get_connection)):
    """Endpoint to save employee data from JSON."""
    employee_ids = []
    try:
        # Loop through each employee and save it
        for employee in employees:
            # Convert Employee model to dictionary
            employee_dict = employee.dict()

            # Reformat the hire_date to match Oracle's expected format if it's not None
            hire_date = employee_dict.get("hire_date")
            hire_date_str = DataHelper.format_hire_date(hire_date)

            # Set the hire_date back to the correct format for Oracle
            employee_dict["hire_date"] = hire_date_str

            # Serialize the Employee dictionary to JSON
            employee_json = json.dumps(employee_dict)

            # Call the Save_Employee_JSON procedure using DataHelper's call_proc method
            employee_id = DataHelper.instance().call_proc(
                "EMPLOYEE_PKG.save_employees_json",
                [employee_json],
                return_type="NUMBER"
            )

            # Add the generated or updated employee ID to the list
            employee_ids.append(employee_id)

        # Return the list of employee IDs
        return employee_ids

    except Exception as e:
        # Handle any unexpected errors
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
