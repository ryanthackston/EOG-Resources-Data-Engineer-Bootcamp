# Code is easy to read and follow.
# There is an option to quit.
# got a no credentials specified. Had to import load_dotenv() and add that line of code like the other file.
# 1) no default. Works fine.
# 2) I don't know why #2 wouldn't run. It seems to call the package fine, but never returns.
# 3) not implemented
# 4) works good
# 5) works good
# 6) I don't know why #6 wouldn't run. It seems to call the pacakge fine, but never returns.
# 7) Wrong number or types of arguments in call to 'GETWELLDATABYWEEK'



import oracledb
import os
import sys
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
    
# @app.get("/well_name/{well_id}/{division_id}")
# Function to call the GetWellName procedure
def get_well_name(well_id: int, division_id: int):
    connection = get_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                # Call the function GetWellName directly and store the result
                cursor.execute("""
                    SELECT Get_ODM_Pkg.GetWellName(:well_id, :division_id) FROM dual
                """, well_id=well_id, division_id=division_id)

                # Fetch the result of the function
                result = cursor.fetchone()

                # Return the well name
                return {"well_name": result[0]} if result else {"well_name": "No data found"}

        except oracledb.DatabaseError as e:
            print(f"Error executing GetWellName: {e}")
            return {"error": str(e)}
        finally:
            connection.close()
    else:
        return {"error": "Unable to connect to the database"}

# Function to call the GetDivisionDataById procedure
def get_division_data_by_id(division_id: int):
    connection = get_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                # Execute an anonymous PL/SQL block to call the procedure and retrieve the result
                cursor.execute("""
                    DECLARE
                        v_ref_cursor SYS_REFCURSOR;
                        v_division_name   odm_division.division_name%TYPE;
                        v_year_id         NUMBER;
                        v_well_count      NUMBER;
                        v_total_oil_produced  NUMBER;
                        v_total_oil_sales     NUMBER;
                        v_total_gas_produced  NUMBER;
                        v_total_gas_sales     NUMBER;
                        v_total_costs         NUMBER;
                    BEGIN
                        -- Call the procedure
                        Get_ODM_Pkg.GetDivisionDataById(:division_id, v_ref_cursor);

                        -- Fetch data from the ref cursor and return results as rows
                        LOOP
                            FETCH v_ref_cursor INTO v_division_name, v_year_id, v_well_count, 
                                                 v_total_oil_produced, v_total_oil_sales, 
                                                 v_total_gas_produced, v_total_gas_sales, v_total_costs;
                            EXIT WHEN v_ref_cursor%NOTFOUND;

                            -- Return fetched row using DBMS_SQL.RETURN_RESULT for implicit result set
                            DBMS_SQL.RETURN_RESULT(v_ref_cursor);
                        END LOOP;
                        CLOSE v_ref_cursor;
                    END;
                """, division_id=division_id)

                # Fetch the results returned from the implicit result set
                results = cursor.fetchall()

                # Process the results into a list of dictionaries
                division_data_list = []
                for row in results:
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
                    division_data_list.append(division_data)

                # Return the division data as a JSON response
                return {"division_data": division_data_list}

        except oracledb.DatabaseError as e:
            print(f"Error executing GetDivisionDataById: {e}")
            return {"error": str(e)}
        finally:
            connection.close()
    else:
        return {"error": "Unable to connect to the database"}

# Function to call the GetDivisionName function
def get_division_name(division_id):
    connection = get_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                # Call the function
                division_name = cursor.callfunc('Get_ODM_Pkg.GetDivisionName', str, [division_id])
                
                # Display the division name
                print(f"Division Name: {division_name}")
        except oracledb.DatabaseError as e:
            print(f"Error executing GetDivisionName: {e}")
        finally:
            connection.close()

# Function to call the GetWellDataByMonth procedure
def get_well_data_by_month(division_id):
    connection = get_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                # Define the ref cursor to fetch results
                ref_cursor = cursor.var(oracledb.CURSOR)
                
                # Call the procedure
                cursor.callproc('Get_ODM_Pkg.GetWellDataByMonth', [division_id, ref_cursor])
                
                # Fetch and display the results
                for row in ref_cursor.getvalue():
                    print(row)
        except oracledb.DatabaseError as e:
            print(f"Error executing GetWellDataByMonth: {e}")
        finally:
            connection.close()


def get_division_data():
    connection = get_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                # Define the ref cursor to fetch results
                ref_cursor = cursor.var(oracledb.CURSOR)
                
                # Call the procedure
                cursor.callproc('Get_ODM_Pkg.GetDivisionData', [ref_cursor])
                
                # Fetch and display the results
                for row in ref_cursor.getvalue():
                    print(row)
        except oracledb.DatabaseError as e:
            print(f"Error executing GetWellDataByMonth: {e}")
        finally:
            connection.close()


def get_well_data_by_week():
    connection = get_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                # Define the ref cursor to fetch results
                ref_cursor = cursor.var(oracledb.CURSOR)
                
                # Call the procedure
                cursor.callproc('Get_ODM_Pkg.GetWellDataByWeek', [ref_cursor])
                
                # Fetch and display the results
                for row in ref_cursor.getvalue():
                    print(row)
        except oracledb.DatabaseError as e:
            print(f"Error executing GetWellDataByMonth: {e}")
        finally:
            connection.close()

# Main section to run specific functions
def main():
    while True:
        print("\nPlease choose a function to run:")
        print("1. GetWellName")
        print("2. GetDivisionDataById")
        print("3. GetDivisionName")
        print("4. GetWellDataByMonth")
        print("5. GetDivisionData")
        print("6. GetWellDataByWeek")
        print("Q. Quit")

        choice = input("\nEnter your choice: ").strip().lower()

        if choice == '1':
            well_id = int(input("Enter Well ID: "))
            division_id = int(input("Enter Division ID: "))
            get_well_name(well_id, division_id)
        elif choice == '2':
            division_id = int(input("Enter Division ID: "))
            get_division_data_by_id(division_id)
        elif choice == '3':
            division_id = int(input("Enter Division ID: "))
            get_division_name(division_id)
        elif choice == '4':
            division_id = int(input("Enter Division ID: "))
            get_well_data_by_month(division_id)
        elif choice == '5':
            get_division_data()
        elif choice == '6':
            get_well_data_by_week()
        elif choice == 'q':
            print("Exiting the program.")
            sys.exit(0)
        else:
            print("Invalid choice. Please try again.")

# Main section to run the main function
if __name__ == '__main__':
    main()


