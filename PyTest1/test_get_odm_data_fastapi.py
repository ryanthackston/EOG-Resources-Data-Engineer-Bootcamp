import oracledb
import os
from dotenv import load_dotenv
from fastapi import FastAPI

app = FastAPI()

load_dotenv()

# Database connection setup
def get_connection():
    try:
        connection = oracledb.connect(
            user=os.getenv('D3_USER'),
            password=os.getenv('D3_PASS'),
            dsn=os.getenv('D3_DSN')
        )
        print("Database connection successful")
        return connection
    except oracledb.DatabaseError as e:
        print(f"Error connecting to Oracle DB: {e}")
        return None

@app.get("/division_id/{division_id}")
def get_division_data_by_id(division_id: int):
    connection = get_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                # Define the ref cursor for the result set
                ref_cursor = cursor.var(oracledb.CURSOR)

                # Call the procedure Get_ODM_Pkg.GetDivisionDataById
                print(f"Calling Get_ODM_Pkg.GetDivisionDataById for division_id: {division_id}")
                cursor.callproc('Get_ODM_Pkg.GetDivisionDataById', [division_id, ref_cursor])

                # Fetch the data from the ref cursor
                ref_cursor_value = ref_cursor.getvalue()

                # Fetch all rows from the cursor
                results = []
                for row in ref_cursor_value:
                    division_data = {
                        "division_name": row[0],
                        "year_id": row[1],
                        "well_count": row[2],
                        "total_oil_produced": row[3],
                        "total_oil_sales": row[4],
                        "total_gas_produced": row[5],
                        "total_gas_sales": row[6],
                        "total_costs": row[7]
                    }
                    results.append(division_data)

                # Return the division data as a JSON response
                return {"division_data": results}

        except oracledb.DatabaseError as e:
            print(f"Error executing GetDivisionDataById: {e}")
            return {"error": str(e)}
        finally:
            connection.close()
    else:
        return {"error": "Unable to connect to the database"}