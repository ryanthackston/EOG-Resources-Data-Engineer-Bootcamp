import os
import oracledb
import threading
from typing import List, Optional, Any
from contextlib import contextmanager
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

class DataHelper:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DataHelper, cls).__new__(cls, *args, **kwargs)
                    cls._instance.user = cls._read_secret(os.getenv('D3_USER'))
                    cls._instance.password = cls._read_secret(os.getenv('D3_PASS'))
                    cls._instance.dsn = cls._read_secret(os.getenv('D3_DSN'))
                    # Debug print statements
                    # print(f"User Path: {os.getenv('D3_USER')}")
                    # print(f"Password Path: {os.getenv('D3_PASS')}")
                    # print(f"DSN Path: {os.getenv('D3_DSN')}")
                    cls._instance.pool = None
                    cls._instance.init_pool()
        return cls._instance

    @staticmethod
    def _read_secret(file_path):
        if file_path is None:
            raise ValueError("Environment variable for secret is not set")
        if os.path.isfile(file_path):
            with open(file_path, 'r') as file:
                return file.read().strip()
        return file_path  # Assuming the value is directly the secret, not a path

    @staticmethod
    def instance():
        if DataHelper._instance is None:
            DataHelper._instance = DataHelper()
        return DataHelper._instance

    def init_pool(self):
        if self.pool is None:
            self.pool = oracledb.create_pool(
                user=self.user,
                password=self.password,
                dsn=self.dsn,
                min=2,
                max=10,
                increment=1
            )
        print("Pool Initialized")

    def close_pool(self):
        if self.pool is not None:
            self.pool.close()
            self.pool = None
        print("Pool Closed")

    def get_connection(self):
        if self.pool is None:
            self.init_pool()
        print("Connection acquired")
        return self.pool.acquire()

    def close_connection(self, connection):
        if self.pool is not None:
            self.pool.release(connection)
        print("Connection released")
        

    @contextmanager
    def get_db(self):
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.close_connection(conn)

    # Format date values in query results
    def _format_dates(self, data):
        for item in data:
            for key, value in item.items():
                if isinstance(value, datetime.datetime):
                    item[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        return data

    # Uses context manager as a cursor for executing stored procedures and handling different types of return values
    def execute_procedure(self, proc_name: str, params: List = None, return_type: str = 'NONE') -> Optional[Any]:
        if params is None:
            params = []

        with self.get_db() as connection:
            try:
                with connection.cursor() as cursor:
                    if return_type == 'CURSOR':
                        out_cursor = connection.cursor()
                        params.append(out_cursor)
                        cursor.callproc(proc_name, params)
                        results = [dict(zip([col[0].upper() for col in out_cursor.description], row)) for row in out_cursor.fetchall()]
                        out_cursor.close()
                        return results
                    elif return_type == 'NUMBER':
                        out_param = cursor.var(oracledb.NUMBER)
                        params.append(out_param)
                        cursor.callproc(proc_name, params)
                        connection.commit()
                        return out_param.getvalue()
                    else:
                        cursor.callproc(proc_name, params)
                        connection.commit()
                        return None
            except Exception as e:
                raise RuntimeError(f"An error occurred: {e}")



    def call_proc(self, proc_name, params, return_type="CURSOR", connection=None):
        """
        Execute a stored procedure and handle various return types.

        Args:
            proc_name (str): The name of the stored procedure to execute.
            params (list): The list of parameters to pass to the stored procedure.
            return_type (str): The expected return type (e.g., "CURSOR", "NUMBER", "CLOB").
            connection: Optional; a pre-existing database connection.

        Returns:
            Any: The result of the stored procedure based on the specified return_type.
        """
        if hasattr(connection, "__enter__"):
            connection = connection.__enter__()


        if connection is None:  # If no connection is provided, use `get_db`
            with self.get_db() as conn:
                return self._execute_proc(proc_name, params, return_type, conn)
        else:
            return self._execute_proc(proc_name, params, return_type, connection)
        
    
    def _execute_proc(self, proc_name, params, return_type, connection):
        """
        Helper function to execute a stored procedure with the given connection.

        Args:
            proc_name (str): Name of the procedure.
            params (list): Parameters to pass.
            return_type (str): Expected return type.
            connection: Database connection.

        Returns:
            Any: Result based on return type.
        """
        with connection.cursor() as cursor:
            if return_type.upper() == 'CURSOR':
                result_cursor = connection.cursor()
                cursor.callproc(proc_name, [*params, result_cursor])
                result = result_cursor.fetchall()
                result_cursor.close()
                return result
            elif return_type.upper() == 'NUMBER':
                output_var = cursor.var(oracledb.NUMBER)
                cursor.callproc(proc_name, [*params, output_var])
                return output_var.getvalue()
            elif return_type.upper() == 'CLOB':
                output_var = cursor.var(oracledb.CLOB)
                cursor.callproc(proc_name, [*params, output_var])
                return output_var.getvalue().read()
            elif return_type.upper() == 'NONE':
                cursor.callproc(proc_name, params)  # Just execute the procedure
                return None  # No return value needed
            else:
                raise Exception(f"Unsupported return type: {return_type}")

                

    def format_hire_date(hire_date):
        """
        Format the hire_date to Oracle's expected format.
        - If already a datetime object, format to ISO format with 'T'.
        - If a string, try parsing it as ISO or space-separated format.
        - Raise ValueError for unexpected types or invalid formats.

        Args:
            hire_date (Union[str, datetime]): The hire_date value to format.

        Returns:
            str: The formatted hire_date in '%Y-%m-%dT%H:%M:%S' format.
        """
        if hire_date:
            if isinstance(hire_date, datetime):
                try:
                    return hire_date.strftime("%Y-%m-%d %H:%M:%S")
                except: 
                    # Format datetime object to ISO format
                    return hire_date.strftime("%Y-%m-%dT%H:%M:%S")
            elif isinstance(hire_date, str):
                # Try parsing as ISO format or space-separated format
                try:
                    return datetime.strptime(hire_date, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    try:
                        return datetime.strptime(hire_date, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        raise ValueError(f"Invalid date format for hire_date: {hire_date}")
            else:
                raise ValueError(f"Unexpected type for hire_date: {type(hire_date)}")
        return None
    


    def call_func(self, func_name: str, return_type: str, params: List[Any], connection=None):
        """
        Executes a PL/SQL function and returns the result.

        Args:
            func_name (str): The name of the PL/SQL function.
            return_type (str): The expected return type of the function (e.g., "NUMBER", "CLOB").
            params (List[Any]): The list of parameters for the function.
            connection: An optional database connection. Uses `get_db` if not provided.

        Returns:
            Any: The result of the PL/SQL function.
        """
        if connection is None:
            with self.get_db() as conn:
                return self._execute_func(func_name, return_type, params, conn)
        return self._execute_func(func_name, return_type, params, connection)
    

    def _execute_func(self, func_name: str, return_type: str, params: List[Any], connection):
        """
        Internal method to execute a PL/SQL function with a given connection.
        Args:
            func_name (str): Name of the PL/SQL function.
            return_type (str): Expected return type of the function.
            params (List[Any]): Parameters to pass to the function.
            connection: Database connection.
        Returns:
            Any: Result of the function.
        """
        try:
            with connection.cursor() as cursor:
                if func_name == "EMPLOYEE_PKG.Save_Employee":
                    # Handle the Save_Employee function specifically
                    results = cursor.callfunc(func_name, oracledb.NUMBER, params)
                    connection.commit()
                    return results
                else:
                    # Handle generic functions
                    result = cursor.callfunc(func_name, getattr(oracledb, return_type.upper()), params)
                    connection.commit()
                    return result
        except oracledb.DatabaseError as e:
            error, = e.args
            raise RuntimeError(f"Database error during execution of {func_name}: {error}")

    
    def transform_employee_result(result):
        """
        Transform a single database result row into a dictionary compatible with the Employee model.

        Args:
            result (tuple): A tuple representing the database row.

        Returns:
            dict: A dictionary containing the transformed data.
        """
        return {
            "employee_id": result[0],
            "first_name": result[1] or "Unknown",
            "last_name": result[2] or "Unknown",
            "email": result[3] or "N/A",
            "phone": result[4] or "N/A",
            "hire_date": DataHelper.format_hire_date(result[5]) if result[5] else None,
            "manager_id": result[6],
            "job_title": result[7] or "Not Specified",
            "is_active_fl": result[8],
        }