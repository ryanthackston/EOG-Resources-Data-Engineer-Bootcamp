# APEARSALL2 
# task 1 item 4. Min date is 1/1/24. max date is 9/30/24. Stale max.
# ------ODM WELL--------
# 4733 rows transferred from odm_well. I just had to comment out the full queries. 
# Could maybe be easier to read in from another file.
# ODM_WELL level of difficulty 3/10. Was pretty easy.
# -------ODM COMPLETION________
# query got hung up, but ran eventually (somehow think its Tran's fault). got 4749 rows. 
# Same as odm_well, pretty easy to use once already looked at odm_well.
# --------ODM COMP PRODUCTION--------
# is split up into years. Had to change it the destination table,
# was hard coded to a test table not the final table.
# I get millions of warnings to console. 
# Has the forecast data so I think the counts would have been 21MM. 
# Would have ran very fast without the warnings and the extra forecast data.
# My VS code errored in the middle which did not appear to be from the code. 
#---------ODM AFE DETAIL--------------
# 556568 rows. just had to comment correct sql.
#---------ODM AFE DETAIL DAILY---------
# 9,287,020 rows. same running as above
#--------- ODM AFE Header -------------
# 29,412 rows. same running as above
#--------- ODM Division ---------------
# 2 rows, just midland and SA.
#-------- in general ------------------
# would create duplicates. need to go into SQL and delete







import oracledb
import os
from dotenv import load_dotenv
import time
import datetime  # For handling datetime conversion
#import pdb 
 
# Load environment variables
load_dotenv()
 
def get_connection(user, password, dsn):
    """Establish and return a database connection."""
    return oracledb.connect(user=user, password=password, dsn=dsn)
 
def convert_row_to_db_types(row, column_types):
    """Convert row data to match the destination table's column types."""
    new_row = list(row)
    for j, col_type in enumerate(column_types):
        # Skip None values
        if new_row[j] is None:
            continue  # Keep None values intact

        try:
            # Convert int or float to string if the column is expecting VARCHAR
            if col_type == oracledb.DB_TYPE_VARCHAR:
                if isinstance(new_row[j], (int, float)):
                    new_row[j] = str(new_row[j])  # Convert int or float to string

                # Handle unsupported types for VARCHAR
                elif not isinstance(new_row[j], str):
                    print(f"Unsupported type {type(new_row[j])} for VARCHAR column {j}, setting to None.")
                    new_row[j] = None

            # Convert string to number if the column is expecting NUMBER
            elif col_type == oracledb.DB_TYPE_NUMBER:
                if isinstance(new_row[j], str):
                    try:
                        new_row[j] = float(new_row[j])  # Attempt to convert string to float
                    except ValueError:
                        print(f"Error converting string to float for column {j}, setting to None.")
                        new_row[j] = None

                # Handle unsupported types for NUMBER
                elif not isinstance(new_row[j], (int, float)):
                    print(f"Unsupported type {type(new_row[j])} for NUMBER column {j}, setting to None.")
                    new_row[j] = None

            # Handle DATE and TIMESTAMP types by passing the original datetime object
            elif col_type in (oracledb.DB_TYPE_DATE, oracledb.DB_TYPE_TIMESTAMP):
                if not isinstance(new_row[j], (datetime.date, datetime.datetime)):
                    print(f"Unsupported type {type(new_row[j])} for DATE/TIMESTAMP column {j}, setting to None.")
                    new_row[j] = None

            # If the column type is neither VARCHAR, NUMBER, DATE, nor TIMESTAMP, set value to None (unexpected types)
            else:
                print(f"Unexpected column type {col_type} for column {j}, setting to None.")
                new_row[j] = None

        except Exception as e:
            # Log the error and set value to None if any conversion fails
            print(f"Error converting column {j} with value {new_row[j]}: {e}")
            new_row[j] = None

    return tuple(new_row)
 
def migrate_table(source_cursor, dest_cursor, source_query, dest_table, batch_size=100000):
    """Fetch data from source in batches and insert into destination table."""
    source_cursor.execute(source_query)
    source_cursor.arraysize = batch_size  # Set array fetch size for better performance
 
    # Fetch destination table's column information
    dest_cursor.execute(f"SELECT * FROM {dest_table} WHERE 1=0")
    column_names = [desc[0] for desc in dest_cursor.description]
    column_types = [desc[1] for desc in dest_cursor.description]
 
    # Generate placeholders dynamically based on the number of columns
    placeholders = ', '.join([':' + str(i + 1) for i in range(len(column_names))])
    dest_insert_query = f"INSERT INTO {dest_table} ({', '.join(column_names)}) VALUES ({placeholders})"
 
    while True:
        rows = source_cursor.fetchmany(batch_size)
        if not rows:
            break
 
        # Convert types as needed
        converted_rows = [convert_row_to_db_types(row, column_types) for row in rows]
 
        # Insert rows in batches using executemany for better performance
        dest_cursor.executemany(dest_insert_query, converted_rows)
 
def main():
    # Record the start time of the entire migration
    overall_start_time = time.time()
 
    # Define connection details
    source_dsn = os.getenv('P2_DSN')
    dest_dsn = os.getenv('D3_DSN')
 
    # Connect to the source and destination databases
    source_conn = get_connection(
        user=os.getenv('P2_USER'),
        password=os.getenv('P2_PASS'),
        dsn=source_dsn
    )
 
    dest_conn = get_connection(
        user=os.getenv('D3_USER'),
        password=os.getenv('D3_PASS'),
        dsn=dest_dsn
    )
 
    try:
        # Create cursors for both source and destination
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()



 
        # List of tables to migrate
        tables_to_migrate = [
            # ("ODM_INFO.ODM_WELL", """WITH well_ids AS (
            #                 SELECT /*+ PARALLEL(16) */
            #                     w.well_id,
            #                     w.division_id
            #                 FROM
            #                     odm_info.odm_well w
            #                 JOIN
            #                     odm_info.odm_completion c
            #                     ON c.well_id = w.well_id
            #                     AND c.division_id = w.division_id
            #                 JOIN
            #                     odm_dba.odm_comp_production p
            #                     ON p.completion_id = c.completion_id
            #                     AND p.division_id = c.division_id
            #                 WHERE w.division_id IN (10,63)
            #                     AND (p.gross_oil_prod > 0 OR p.gross_gas_prod > 0)
            #                     AND w.well_direction = 'HOR'
            #                 GROUP BY
            #                     w.well_id, w.division_id, c.completion_id
            #                 HAVING
            #                     MIN(EXTRACT(YEAR FROM p.date_value)) >= 2016
            #             )
            #             SELECT distinct w.*
            #             FROM well_ids wi
            #             JOIN odm_info.odm_well w ON wi.well_id = w.well_id AND wi.division_id = w.division_id""", "ODM_WELL"),
            # ("ODM_DBA.ODM_DIVISION", """WITH well_ids AS (
            #                 SELECT /*+ PARALLEL(16) */
            #                     w.well_id,
            #                     w.division_id
            #                 FROM
            #                     odm_info.odm_well w
            #                 JOIN
            #                     odm_info.odm_completion c
            #                     ON c.well_id = w.well_id
            #                     AND c.division_id = w.division_id
            #                 JOIN
            #                     odm_dba.odm_comp_production p
            #                     ON p.completion_id = c.completion_id
            #                     AND p.division_id = c.division_id
            #                 WHERE w.division_id IN (10,63)
            #                     AND (p.gross_oil_prod > 0 OR p.gross_gas_prod > 0)
            #                     AND w.well_direction = 'HOR'
            #                 GROUP BY
            #                     w.well_id, w.division_id, c.completion_id
            #                 HAVING
            #                     MIN(EXTRACT(YEAR FROM p.date_value)) >= 2016
            #             )
            #             SELECT distinct d.*
            #             FROM well_ids wi
            #             JOIN odm_dba.odm_division d ON wi.division_id = d.division_id""", "ODM_DIVISION"),
            # ("ODM_INFO.ODM_COMPLETION", """WITH well_ids AS (
            #                     SELECT /*+ PARALLEL(16) */
            #                         w.well_id,
            #                         w.division_id
            #                     FROM
            #                         odm_info.odm_well w
            #                     JOIN
            #                         odm_info.odm_completion c
            #                         ON c.well_id = w.well_id
            #                         AND c.division_id = w.division_id
            #                     JOIN
            #                         odm_dba.odm_comp_production p
            #                         ON p.completion_id = c.completion_id
            #                         AND p.division_id = c.division_id
            #                     WHERE w.division_id IN (10,63)
            #                         AND (p.gross_oil_prod > 0 OR p.gross_gas_prod > 0)
            #                         AND w.well_direction = 'HOR'
            #                     GROUP BY
            #                         w.well_id, w.division_id, c.completion_id
            #                     HAVING
            #                         MIN(EXTRACT(YEAR FROM p.date_value)) >= 2016
            #                 )
            #                 SELECT distinct c.*
            #                 FROM well_ids wi
            #                 JOIN odm_dba.odm_completion c ON wi.division_id = c.division_id AND wi.well_id = c.well_id
            #                 """, "ODM_COMPLETION"),
            ("ODM_DBA.ODM_AFE_HEADER", """WITH well_ids AS (
                                        SELECT /*+ PARALLEL(16) */
                                            w.well_id,
                                            w.division_id,
                                            c.completion_id
                                        FROM
                                            odm_info.odm_well w
                                        JOIN
                                            odm_info.odm_completion c
                                            ON c.well_id = w.well_id
                                            AND c.division_id = w.division_id
                                        JOIN
                                            odm_dba.odm_comp_production p
                                            ON p.completion_id = c.completion_id
                                            AND p.division_id = c.division_id
                                        WHERE w.division_id IN (10,63)
                                            AND (p.gross_oil_prod > 0 OR p.gross_gas_prod > 0)
                                            AND w.well_direction = 'HOR'
                                        GROUP BY
                                            w.well_id, w.division_id, c.completion_id
                                        HAVING
                                            MIN(EXTRACT(YEAR FROM p.date_value)) >= 2016
                                    )
                                    SELECT distinct h.*
                                    FROM well_ids wi
                                    JOIN odm_dba.odm_afe_header h ON wi.division_id = h.division_id AND wi.well_id = h.well_id""", "ODM_AFE_HEADER"),
            ("ODM_DBA.ODM_AFE_DETAIL", """WITH well_ids AS (
                                        SELECT /*+ PARALLEL(16) */
                                            w.well_id,
                                            w.division_id,
                                            c.completion_id
                                        FROM
                                            odm_info.odm_well w
                                        JOIN
                                            odm_info.odm_completion c
                                            ON c.well_id = w.well_id
                                            AND c.division_id = w.division_id
                                        JOIN
                                            odm_dba.odm_comp_production p
                                            ON p.completion_id = c.completion_id
                                            AND p.division_id = c.division_id
                                        WHERE w.division_id IN (10,63)
                                            AND (p.gross_oil_prod > 0 OR p.gross_gas_prod > 0)
                                            AND w.well_direction = 'HOR'
                                        GROUP BY
                                            w.well_id, w.division_id, c.completion_id
                                        HAVING
                                            MIN(EXTRACT(YEAR FROM p.date_value)) >= 2016
                                    )
                                    SELECT distinct d.*
                                    FROM well_ids wi
                                    JOIN (SELECT DISTINCT well_id, division_id, afe_nbr FROM odm_dba.odm_afe_header) h ON wi.division_id = h.division_id AND wi.well_id = h.well_id
                                    JOIN odm_dba.odm_afe_detail d ON h.afe_nbr = d.afe_nbr AND h.division_id = d.division_id
                                    """, "ODM_AFE_DETAIL"),
                ("ODM_AFE_DETAIL_DAILY", """WITH well_ids AS (
                                    SELECT /*+ PARALLEL(16) */
                                        w.well_id,
                                        w.division_id,
                                        c.completion_id
                                    FROM
                                        odm_info.odm_well w
                                    JOIN
                                        odm_info.odm_completion c
                                        ON c.well_id = w.well_id
                                        AND c.division_id = w.division_id
                                    JOIN
                                        odm_dba.odm_comp_production p
                                        ON p.completion_id = c.completion_id
                                        AND p.division_id = c.division_id
                                    WHERE w.division_id IN (10,63)
                                        AND (p.gross_oil_prod > 0 OR p.gross_gas_prod > 0)
                                        AND w.well_direction = 'HOR'
                                    GROUP BY
                                        w.well_id, w.division_id, c.completion_id
                                    HAVING
                                        MIN(EXTRACT(YEAR FROM p.date_value)) >= 2016
                                )
                                SELECT distinct dd.*
                                FROM well_ids wi
                                JOIN (SELECT DISTINCT well_id, division_id, afe_nbr, perc_well_id_nbr, event_nbr FROM odm_dba.odm_afe_header) h ON wi.division_id = h.division_id AND wi.well_id = h.well_id
                                JOIN odm_dba.odm_afe_detail_daily dd ON h.afe_nbr = dd.afe_nbr AND h.division_id = dd.division_id AND h.perc_well_id_nbr = dd.perc_well_id_nbr AND h.event_nbr = dd.event_nbr
                            """, "ODM_AFE_DETAIL_DAILY"),
                        ("ODM_DBA.ODM_COMP_PRODUCTION", """WITH well_ids AS (
                                                SELECT /*+ PARALLEL(16) */
                                                    w.well_id,
                                                    w.division_id,
                                                    extract(year from min(p.date_value)) as start_prod_year
                                                FROM
                                                    odm_info.odm_well w
                                                JOIN
                                                    odm_info.odm_completion c
                                                    ON c.well_id = w.well_id
                                                    AND c.division_id = w.division_id
                                                JOIN
                                                    odm_dba.odm_comp_production p
                                                    ON p.completion_id = c.completion_id
                                                    AND p.division_id = c.division_id
                                                WHERE w.division_id IN (10,63)
                                                    AND ((p.gross_oil_prod > 0) OR (p.gross_gas_prod > 0))
                                                    AND w.well_direction = 'HOR'
                                                GROUP BY
                                                    w.well_id, w.division_id
                                                HAVING
                                                    MIN(EXTRACT(YEAR FROM p.date_value)) in (2016, 2017)
                                            )
                                            SELECT distinct p.*
                                            FROM well_ids wi
                                            INNER JOIN odm_info.odm_completion c ON c.well_id = wi.WELL_ID AND c.DIVISION_ID = wi.DIVISION_ID 
                                            JOIN odm_dba.odm_comp_production p ON p.completion_id = c.completion_id AND p.division_id = c.division_id""",
                                            "ODM_COMP_PRODUCTION")
            ,            ("ODM_DBA.ODM_COMP_PRODUCTION", """WITH well_ids AS (
                                                SELECT /*+ PARALLEL(16) */
                                                    w.well_id,
                                                    w.division_id,
                                                    extract(year from min(p.date_value)) as start_prod_year
                                                FROM
                                                    odm_info.odm_well w
                                                JOIN
                                                    odm_info.odm_completion c
                                                    ON c.well_id = w.well_id
                                                    AND c.division_id = w.division_id
                                                JOIN
                                                    odm_dba.odm_comp_production p
                                                    ON p.completion_id = c.completion_id
                                                    AND p.division_id = c.division_id
                                                WHERE w.division_id IN (10,63)
                                                    AND ((p.gross_oil_prod > 0) OR (p.gross_gas_prod > 0))
                                                    AND w.well_direction = 'HOR'
                                                GROUP BY
                                                    w.well_id, w.division_id
                                                HAVING
                                                    MIN(EXTRACT(YEAR FROM p.date_value)) in (2018, 2019)
                                            )
                                            SELECT distinct p.*
                                            FROM well_ids wi
                                            INNER JOIN odm_info.odm_completion c ON c.well_id = wi.WELL_ID AND c.DIVISION_ID = wi.DIVISION_ID 
                                            JOIN odm_dba.odm_comp_production p ON p.completion_id = c.completion_id AND p.division_id = c.division_id""",
                                            "ODM_COMP_PRODUCTION"),
                        ("ODM_DBA.ODM_COMP_PRODUCTION", """WITH well_ids AS (
                                                SELECT /*+ PARALLEL(16) */
                                                    w.well_id,
                                                    w.division_id,
                                                    extract(year from min(p.date_value)) as start_prod_year
                                                FROM
                                                    odm_info.odm_well w
                                                JOIN
                                                    odm_info.odm_completion c
                                                    ON c.well_id = w.well_id
                                                    AND c.division_id = w.division_id
                                                JOIN
                                                    odm_dba.odm_comp_production p
                                                    ON p.completion_id = c.completion_id
                                                    AND p.division_id = c.division_id
                                                WHERE w.division_id IN (10,63)
                                                    AND ((p.gross_oil_prod > 0) OR (p.gross_gas_prod > 0))
                                                    AND w.well_direction = 'HOR'
                                                GROUP BY
                                                    w.well_id, w.division_id
                                                HAVING
                                                    MIN(EXTRACT(YEAR FROM p.date_value)) in (2020, 2021)
                                            )
                                            SELECT distinct p.*
                                            FROM well_ids wi
                                            INNER JOIN odm_info.odm_completion c ON c.well_id = wi.WELL_ID AND c.DIVISION_ID = wi.DIVISION_ID 
                                            JOIN odm_dba.odm_comp_production p ON p.completion_id = c.completion_id AND p.division_id = c.division_id""",
                                            "ODM_COMP_PRODUCTION"),
            
                            ("ODM_DBA.ODM_COMP_PRODUCTION", """WITH well_ids AS (
                                                SELECT /*+ PARALLEL(16) */
                                                    w.well_id,
                                                    w.division_id,
                                                    extract(year from min(p.date_value)) as start_prod_year
                                                FROM
                                                    odm_info.odm_well w
                                                JOIN
                                                    odm_info.odm_completion c
                                                    ON c.well_id = w.well_id
                                                    AND c.division_id = w.division_id
                                                JOIN
                                                    odm_dba.odm_comp_production p
                                                    ON p.completion_id = c.completion_id
                                                    AND p.division_id = c.division_id
                                                WHERE w.division_id IN (10,63)
                                                    AND ((p.gross_oil_prod > 0) OR (p.gross_gas_prod > 0))
                                                    AND w.well_direction = 'HOR'
                                                GROUP BY
                                                    w.well_id, w.division_id
                                                HAVING
                                                    MIN(EXTRACT(YEAR FROM p.date_value)) = 2022
                                            )
                                            SELECT distinct p.*
                                            FROM well_ids wi
                                            INNER JOIN odm_info.odm_completion c ON c.well_id = wi.WELL_ID AND c.DIVISION_ID = wi.DIVISION_ID 
                                            JOIN odm_dba.odm_comp_production p ON p.completion_id = c.completion_id AND p.division_id = c.division_id""",
                                            "ODM_COMP_PRODUCTION"),
                            ("ODM_DBA.ODM_COMP_PRODUCTION", """WITH well_ids AS (
                                                SELECT /*+ PARALLEL(16) */
                                                    w.well_id,
                                                    w.division_id,
                                                    extract(year from min(p.date_value)) as start_prod_year
                                                FROM
                                                    odm_info.odm_well w
                                                JOIN
                                                    odm_info.odm_completion c
                                                    ON c.well_id = w.well_id
                                                    AND c.division_id = w.division_id
                                                JOIN
                                                    odm_dba.odm_comp_production p
                                                    ON p.completion_id = c.completion_id
                                                    AND p.division_id = c.division_id
                                                WHERE w.division_id IN (10,63)
                                                    AND ((p.gross_oil_prod > 0) OR (p.gross_gas_prod > 0))
                                                    AND w.well_direction = 'HOR'
                                                GROUP BY
                                                    w.well_id, w.division_id
                                                HAVING
                                                    MIN(EXTRACT(YEAR FROM p.date_value)) = 2023
                                            )
                                            SELECT distinct p.*
                                            FROM well_ids wi
                                            INNER JOIN odm_info.odm_completion c ON c.well_id = wi.WELL_ID AND c.DIVISION_ID = wi.DIVISION_ID 
                                            JOIN odm_dba.odm_comp_production p ON p.completion_id = c.completion_id AND p.division_id = c.division_id""",
                                            "ODM_COMP_PRODUCTION"),
                            ("ODM_DBA.ODM_COMP_PRODUCTION", """WITH well_ids AS (
                                                SELECT /*+ PARALLEL(16) */
                                                    w.well_id,
                                                    w.division_id,
                                                    extract(year from min(p.date_value)) as start_prod_year
                                                FROM
                                                    odm_info.odm_well w
                                                JOIN
                                                    odm_info.odm_completion c
                                                    ON c.well_id = w.well_id
                                                    AND c.division_id = w.division_id
                                                JOIN
                                                    odm_dba.odm_comp_production p
                                                    ON p.completion_id = c.completion_id
                                                    AND p.division_id = c.division_id
                                                WHERE w.division_id IN (10,63)
                                                    AND ((p.gross_oil_prod > 0) OR (p.gross_gas_prod > 0))
                                                    AND w.well_direction = 'HOR'
                                                GROUP BY
                                                    w.well_id, w.division_id
                                                HAVING
                                                    MIN(EXTRACT(YEAR FROM p.date_value)) = 2024
                                            )
                                            SELECT distinct p.*
                                            FROM well_ids wi
                                            INNER JOIN odm_info.odm_completion c ON c.well_id = wi.WELL_ID AND c.DIVISION_ID = wi.DIVISION_ID 
                                            JOIN odm_dba.odm_comp_production p ON p.completion_id = c.completion_id AND p.division_id = c.division_id""",
                                            "ODM_COMP_PRODUCTION")                                
                                                    ]
 
        # Iterate through each table and attempt migration
        for table_name, source_query, dest_table in tables_to_migrate:
            try:
                print(f"Migrating {table_name}...")
                start_time = time.time()
                migrate_table(
                    source_cursor,
                    dest_cursor,
                    source_query,
                    dest_table
                )
                dest_conn.commit()
                print(f"{table_name} migrated successfully in {time.time() - start_time:.2f} seconds.")
            except oracledb.DatabaseError as e:
                print(f"Error migrating {table_name}: {e}")
                # Continue to the next table
 
    except oracledb.DatabaseError as e:
        print(f"Database connection error: {e}")
 
    finally:
        # Close cursors and connections
        source_cursor.close()
        dest_cursor.close()
        source_conn.close()
        dest_conn.close()
 
    # Record the end time and calculate total execution time
    overall_end_time = time.time()
    print(f"Total migration time: {overall_end_time - overall_start_time:.2f} seconds.")
 
if __name__ == "__main__":
    main()