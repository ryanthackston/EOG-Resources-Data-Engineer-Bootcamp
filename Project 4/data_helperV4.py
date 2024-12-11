import os
import oracledb
import asyncio
from typing import List, Optional, Any
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from datetime import datetime
import logging
import threading


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
                    cls._instance.pool = None
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

    async def init_pool(self):
        if self.pool is None:
            self.pool = await asyncio.to_thread(
                oracledb.create_pool,
                user=self.user,
                password=self.password,
                dsn=self.dsn,
                min=2,
                max=10,
                increment=1
            )
        print("Pool Initialized")

    async def close_pool(self):
        if self.pool is not None:
            await asyncio.to_thread(self.pool.close)
            self.pool = None
        print("Pool Closed")

    async def get_connection(self):
        if self.pool is None:
            await self.init_pool()
        print("Connection acquired")
        return await asyncio.to_thread(self.pool.acquire)

    async def close_connection(self, connection):
        if connection is not None:
            await asyncio.to_thread(connection.close)
        print("Connection released")

    @asynccontextmanager
    async def get_db(self):
        conn = await self.get_connection()
        try:
            yield conn
        finally:
            await self.close_connection(conn)

    async def execute_procedure(self, proc_name: str, params: List = None, return_type: str = 'NONE') -> Optional[Any]:
        if params is None:
            params = []

        async with self.get_db() as connection:
            try:
                async with asyncio.to_thread(connection.cursor) as cursor:
                    if return_type == 'CURSOR':
                        out_cursor = await asyncio.to_thread(connection.cursor)
                        params.append(out_cursor)
                        await asyncio.to_thread(cursor.callproc, proc_name, params)
                        results = await asyncio.to_thread(
                            lambda: [
                                dict(zip([col[0].upper() for col in out_cursor.description], row))
                                for row in out_cursor.fetchall()
                            ]
                        )
                        await asyncio.to_thread(out_cursor.close)
                        return results
                    elif return_type == 'NUMBER':
                        out_param = await asyncio.to_thread(cursor.var, oracledb.NUMBER)
                        params.append(out_param)
                        await asyncio.to_thread(cursor.callproc, proc_name, params)
                        await asyncio.to_thread(connection.commit)
                        return out_param.getvalue()
                    else:
                        await asyncio.to_thread(cursor.callproc, proc_name, params)
                        await asyncio.to_thread(connection.commit)
                        return None
            except Exception as e:
                raise RuntimeError(f"An error occurred: {e}")

    async def call_proc(self, proc_name, params, return_type="CURSOR", connection=None):
        if connection is None:
            async with self.get_db() as conn:
                return await self._execute_proc(proc_name, params, return_type, conn)
        else:
            return await self._execute_proc(proc_name, params, return_type, connection)

    async def _execute_proc(self, proc_name, params, return_type, connection):
        cursor = await asyncio.to_thread(connection.cursor)  # Open the cursor in a thread
        try:
            if return_type.upper() == 'CURSOR':
                result_cursor = await asyncio.to_thread(connection.cursor)
                await asyncio.to_thread(cursor.callproc, proc_name, [*params, result_cursor])
                result = await asyncio.to_thread(result_cursor.fetchall)
                await asyncio.to_thread(result_cursor.close)
                return result
            elif return_type.upper() == 'NUMBER':
                output_var = await asyncio.to_thread(cursor.var, oracledb.NUMBER)
                await asyncio.to_thread(cursor.callproc, proc_name, [*params, output_var])
                return output_var.getvalue()
            elif return_type.upper() == 'CLOB':
                output_var = await asyncio.to_thread(cursor.var, oracledb.CLOB)
                await asyncio.to_thread(cursor.callproc, proc_name, [*params, output_var])
                return await asyncio.to_thread(output_var.getvalue().read)
            elif return_type.upper() == 'NONE':
                await asyncio.to_thread(cursor.callproc, proc_name, params)
                return None
            else:
                raise Exception(f"Unsupported return type: {return_type}")
        finally:
            await asyncio.to_thread(cursor.close)  # Ensure the cursor is properly closed

    async def call_func(self, func_name: str, return_type: str, params: List[Any], connection=None):
        if connection is None:
            async with self.get_db() as conn:
                return await self._execute_func(func_name, return_type, params, conn)
        return await self._execute_func(func_name, return_type, params, connection)

    async def _execute_func(self, func_name, return_type, params, conn):
        try:
            # Define a synchronous function for the database call
            def execute_cursor():
                with conn.cursor() as cursor:
                    # Ensure return_type is correctly mapped to an oracledb type
                    oracle_return_type = getattr(oracledb, return_type.upper(), None)
                    if not oracle_return_type:
                        raise ValueError(f"Unsupported return type: {return_type}")
                    
                    # Call the PL/SQL function
                    result = cursor.callfunc(func_name, oracle_return_type, params)
                    return result

            # Run the synchronous function in a thread
            return await asyncio.to_thread(execute_cursor)
        except Exception as e:
            print(f"Error executing function {func_name}: {e}")
            raise
        
        
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