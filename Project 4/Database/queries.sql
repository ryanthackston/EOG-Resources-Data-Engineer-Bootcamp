
CREATE TABLE Employees ( EMPLOYEE_ID NUMBER,
FIRST_NAME VARCHAR2(40),
LAST_NAME VARCHAR2(40),
EMAIL VARCHAR2(60),
PHONE VARCHAR2(20),
HIRE_DATE DATE,
MANAGER_ID NUMBER,
JOB_TITLE VARCHAR2(60) );

CREATE SEQUENCE employee_id_seq
    START WITH 108
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;



CREATE OR REPLACE PACKAGE EMPLOYEE_PKG as 

    TYPE employee_ref_cursor IS REF CURSOR;

    FUNCTION Save_Employee (
            p_employee_id IN NUMBER,
            p_first_name  IN VARCHAR2,
            p_last_name   IN VARCHAR2,
            p_email       IN VARCHAR2,
            p_phone       IN VARCHAR2,
            p_hire_date   IN VARCHAR2,
            p_manager_id  IN NUMBER,
            p_job_title   IN VARCHAR2
    ) RETURN NUMBER;

    PROCEDURE Delete_Employee (
        p_employee_id IN NUMBER
    );

    PROCEDURE Get_Employee (
            p_employee_id IN NUMBER,
            employee_data OUT employee_ref_cursor
    );

    PROCEDURE Get_Employees (
        p_manager_id in NUMBER, 
        employees_data OUT  SYS_REFCURSOR
    );

    PROCEDURE Save_Employee_JSON (
            p_json_data IN CLOB,
            p_employee_id OUT NUMBER
    );

    PROCEDURE Get_Employee_JSON_Cursor (
            p_employee_id IN NUMBER,
            p_json_cursor OUT SYS_REFCURSOR
    );

    PROCEDURE Get_Employee_JSON_CLOB (
            p_id IN NUMBER,
            p_rec OUT CLOB
    );

    PROCEDURE Get_Employees_JSON_CURSOR (
            p_manager_id IN NUMBER,
            p_rec OUT SYS_REFCURSOR
    );

    PROCEDURE Get_Employees_JSON_CLOB (
                p_manager_id IN NUMBER,
                p_rec OUT CLOB
    );

    PROCEDURE save_employees_json (
            p_json_data IN CLOB,
            p_employee_id OUT NUMBER
    );

END EMPLOYEE_PKG;


CREATE OR REPLACE PACKAGE BODY EMPLOYEE_PKG as 

    FUNCTION Save_Employee (
            p_employee_id IN NUMBER,
            p_first_name  IN VARCHAR2,
            p_last_name   IN VARCHAR2,
            p_email       IN VARCHAR2,
            p_phone       IN VARCHAR2,
            p_hire_date   IN VARCHAR2, -- Change type to VARCHAR2 for conversion
            p_manager_id  IN NUMBER,
            p_job_title   IN VARCHAR2
            ) RETURN NUMBER IS
                v_employee_id NUMBER;
            BEGIN
                IF p_employee_id IS NULL THEN
                    -- Generate new EMPLOYEE_ID for CREATE operation
                    v_employee_id := employee_id_seq.NEXTVAL;
                    
                    INSERT INTO Employees (
                        EMPLOYEE_ID, 
                        FIRST_NAME, 
                        LAST_NAME, 
                        EMAIL, 
                        PHONE, 
                        HIRE_DATE, 
                        MANAGER_ID, 
                        JOB_TITLE
                    ) VALUES (
                        v_employee_id,
                        p_first_name,
                        p_last_name,
                        p_email,
                        p_phone,
                        TO_DATE(p_hire_date, 'YYYY-MM-DD HH24:MI:SS'), -- Convert hire_date here
                        p_manager_id,
                        p_job_title
                    );
                ELSE
                    -- Use provided EMPLOYEE_ID for UPDATE operation
                    v_employee_id := p_employee_id;

                    UPDATE Employees
                    SET FIRST_NAME = p_first_name,
                        LAST_NAME  = p_last_name,
                        EMAIL      = p_email,
                        PHONE      = p_phone,
                        HIRE_DATE  = TO_DATE(p_hire_date, 'YYYY-MM-DD HH24:MI:SS'), -- Convert hire_date here
                        MANAGER_ID = p_manager_id,
                        JOB_TITLE  = p_job_title
                    WHERE EMPLOYEE_ID = v_employee_id;
                END IF;
                
                -- Commit the transaction if necessary
                COMMIT;
                
                RETURN v_employee_id;
    END Save_Employee;


    Procedure Delete_Employee (
        p_employee_id IN NUMBER
        ) IS 
            BEGIN 
                UPDATE Employees
                SET IS_ACTIVE_FL = 0
                where EMPLOYEE_ID = p_employee_id;

                COMMIT;
    END Delete_Employee;


    PROCEDURE Get_Employee (
            p_employee_id IN NUMBER,
            employee_data OUT employee_ref_cursor
        ) IS
        BEGIN
            OPEN employee_data FOR
                SELECT EMPLOYEE_ID,
                    FIRST_NAME,
                    LAST_NAME,
                    EMAIL,
                    PHONE,
                    HIRE_DATE,
                    MANAGER_ID,
                    JOB_TITLE,
                    IS_ACTIVE_FL
                FROM EMPLOYEES
                WHERE EMPLOYEE_ID = p_employee_id;
    END Get_Employee;


    procedure Get_Employees (
        p_manager_id in NUMBER, 
        employees_data OUT  SYS_REFCURSOR
        ) IS
            BEGIN 
                OPEN employees_data for
                select  EMPLOYEE_ID,
                        FIRST_NAME,
                        LAST_NAME,
                        EMAIL,
                        PHONE,
                        HIRE_DATE,
                        MANAGER_ID,
                        JOB_TITLE,
                        IS_ACTIVE_FL
                FROM EMPLOYEES
                WHERE MANAGER_ID = p_manager_id;
    END Get_Employees;

    PROCEDURE Save_Employee_JSON (
            p_json_data IN CLOB,
            p_employee_id OUT NUMBER
        ) AS
            l_json_obj      JSON_OBJECT_T;
            l_employee_id   NUMBER;
            l_first_name    VARCHAR2(100);
            l_last_name     VARCHAR2(100);
            l_email         VARCHAR2(100);
            l_phone         VARCHAR2(20);
            l_hire_date     DATE;
            l_manager_id    NUMBER;
            l_job_title     VARCHAR2(100);
            l_is_active_fl  NUMBER;

        BEGIN
            -- Parse the JSON input
            l_json_obj := JSON_OBJECT_T.parse(p_json_data);

            -- Extract the values
            l_employee_id := l_json_obj.get_Number('employee_id');
            l_first_name := l_json_obj.get_String('first_name');
            l_last_name := l_json_obj.get_String('last_name');
            l_email := l_json_obj.get_String('email');
            l_phone := l_json_obj.get_String('phone');
            
            -- Correct the TO_DATE format to handle the full date and time format
            l_hire_date := TO_DATE(l_json_obj.get_String('hire_date'), 'YYYY-MM-DD HH24:MI:SS');

            l_manager_id := l_json_obj.get_Number('manager_id');
            l_job_title := l_json_obj.get_String('job_title');
            l_is_active_fl := l_json_obj.get_Number('is_active_fl');

            -- Check if the employee already exists
            BEGIN
                SELECT employee_id INTO p_employee_id
                FROM Employees
                WHERE employee_id = l_employee_id;

                -- If the employee exists, update their information
                UPDATE Employees
                SET first_name = l_first_name,
                    last_name = l_last_name,
                    email = l_email,
                    phone = l_phone,
                    hire_date = l_hire_date,
                    manager_id = l_manager_id,
                    job_title = l_job_title,
                    is_active_fl = l_is_active_fl
                WHERE employee_id = l_employee_id;

            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    -- If employee doesn't exist, insert a new record
                    INSERT INTO Employees (
                        employee_id, first_name, last_name, email, phone, hire_date, manager_id, job_title, is_active_fl
                    ) VALUES (
                        l_employee_id, l_first_name, l_last_name, l_email, l_phone, l_hire_date, l_manager_id, l_job_title, l_is_active_fl
                    ) RETURNING employee_id INTO p_employee_id;  -- Retrieve new ID into output parameter
            END;

            -- Commit the transaction to save changes
            COMMIT;

        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                RAISE_APPLICATION_ERROR(-20001, 'Error in Save_Employee_JSON: ' || SQLERRM);
    END Save_Employee_JSON;


    PROCEDURE Get_Employee_JSON_Cursor (
            p_employee_id IN NUMBER,
            p_json_cursor OUT SYS_REFCURSOR
        ) AS
            l_employee_id NUMBER;
            l_first_name VARCHAR2(100);
            l_last_name VARCHAR2(100);
            l_email VARCHAR2(100);
            l_phone VARCHAR2(20);
            l_hire_date DATE;
            l_manager_id NUMBER;
            l_job_title VARCHAR2(100);
            l_is_active_fl NUMBER;
            l_json_response CLOB;

        BEGIN
            -- Fetch the employee data based on employee_id
            BEGIN
                SELECT employee_id, first_name, last_name, email, phone, hire_date, manager_id, job_title, is_active_fl
                INTO l_employee_id, l_first_name, l_last_name, l_email, l_phone, l_hire_date, l_manager_id, l_job_title, l_is_active_fl
                FROM Employees
                WHERE employee_id = p_employee_id;

                -- Manually construct the JSON response as a CLOB
                l_json_response := '{"employee_id": ' || l_employee_id ||
                                ', "first_name": "' || l_first_name || '"' ||
                                ', "last_name": "' || l_last_name || '"' ||
                                ', "email": "' || l_email || '"' ||
                                ', "phone": "' || l_phone || '"' ||
                                ', "hire_date": "' || TO_CHAR(l_hire_date, 'YYYY-MM-DD') || '"' ||
                                ', "manager_id": ' || COALESCE(TO_CHAR(l_manager_id), 'null') ||
                                ', "job_title": "' || l_job_title || '"' ||
                                ', "is_active_fl": ' || l_is_active_fl ||
                                '}';

                -- Open a cursor to return the JSON string as a result
                OPEN p_json_cursor FOR
                    SELECT l_json_response AS employee_json FROM dual;

            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    -- If no employee found, return a JSON object with a message
                    l_json_response := '{"message": "Employee not found"}';
                    OPEN p_json_cursor FOR
                        SELECT l_json_response AS employee_json FROM dual;
                WHEN OTHERS THEN
                    RAISE_APPLICATION_ERROR(-20001, 'Error in Get_Employee_JSON_Cursor: ' || SQLERRM);
            END;

    END Get_Employee_JSON_Cursor;


     PROCEDURE Get_Employee_JSON_CLOB (
            p_id IN NUMBER,
            p_rec OUT CLOB
        ) AS
            l_employee_id NUMBER;
            l_first_name VARCHAR2(100);
            l_last_name VARCHAR2(100);
            l_email VARCHAR2(100);
            l_phone VARCHAR2(20);
            l_hire_date DATE;
            l_manager_id NUMBER;
            l_job_title VARCHAR2(100);
            l_is_active_fl NUMBER;

            -- Helper procedure to write text directly to the CLOB
            PROCEDURE append_to_clob(p_clob IN OUT CLOB, p_text IN VARCHAR2) IS
            BEGIN
                DBMS_LOB.WRITEAPPEND(p_clob, LENGTH(p_text), p_text);
            END append_to_clob;

        BEGIN
            -- Initialize the CLOB
            DBMS_LOB.CREATETEMPORARY(p_rec, TRUE);
            DBMS_LOB.TRIM(p_rec, 0);  -- Clear the CLOB in case it has any existing data

            -- Fetch employee data based on the provided ID
            BEGIN
                SELECT employee_id, first_name, last_name, email, phone, hire_date, manager_id, job_title, is_active_fl
                INTO l_employee_id, l_first_name, l_last_name, l_email, l_phone, l_hire_date, l_manager_id, l_job_title, l_is_active_fl
                FROM Employees
                WHERE employee_id = p_id;

                -- Append each part of the JSON string directly to the CLOB
                append_to_clob(p_rec, '{"employee_id": ');
                append_to_clob(p_rec, TO_CHAR(l_employee_id));
                append_to_clob(p_rec, ', "first_name": "' || l_first_name || '"');
                append_to_clob(p_rec, ', "last_name": "' || l_last_name || '"');
                append_to_clob(p_rec, ', "email": "' || l_email || '"');
                append_to_clob(p_rec, ', "phone": "' || l_phone || '"');
                append_to_clob(p_rec, ', "hire_date": "' || TO_CHAR(l_hire_date, 'YYYY-MM-DD') || '"');
                append_to_clob(p_rec, ', "manager_id": ' || COALESCE(TO_CHAR(l_manager_id), 'null'));
                append_to_clob(p_rec, ', "job_title": "' || l_job_title || '"');
                append_to_clob(p_rec, ', "is_active_fl": ' || TO_CHAR(l_is_active_fl) || '}');

            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    -- If no employee found, return a JSON object with a message
                    DBMS_LOB.TRIM(p_rec, 0);
                    append_to_clob(p_rec, '{"message": "Employee not found"}');
                WHEN OTHERS THEN
                    RAISE_APPLICATION_ERROR(-20001, 'Error in Get_Employee_JSON_CLOB: ' || SQLERRM);
            END;

    END Get_Employee_JSON_CLOB;



    PROCEDURE Get_Employees_JSON_CURSOR (
            p_manager_id IN NUMBER,
            p_rec OUT SYS_REFCURSOR
        ) AS
        BEGIN
            OPEN p_rec FOR
                SELECT JSON_OBJECT(
                        'employee_id' VALUE e.employee_id,
                        'first_name' VALUE e.first_name,
                        'last_name' VALUE e.last_name,
                        'email' VALUE e.email,
                        'phone' VALUE e.phone,
                        'hire_date' VALUE TO_CHAR(e.hire_date, 'YYYY-MM-DD'),
                        'manager_id' VALUE e.manager_id,
                        'job_title' VALUE e.job_title,
                        'is_active_fl' VALUE e.is_active_fl
                    ) AS employee_json
                FROM Employees e
                WHERE e.manager_id = p_manager_id;
    END Get_Employees_JSON_CURSOR;

    PROCEDURE Get_Employees_JSON_CLOB (
                p_manager_id IN NUMBER,
                p_rec OUT CLOB
            ) AS
                l_employee_id NUMBER;
                l_first_name VARCHAR2(100);
                l_last_name VARCHAR2(100);
                l_email VARCHAR2(100);
                l_phone VARCHAR2(20);
                l_hire_date DATE;
                l_manager_id NUMBER;
                l_job_title VARCHAR2(100);
                l_is_active_fl NUMBER;
                l_first_record BOOLEAN := TRUE;

                -- Helper procedure to write text directly to the CLOB
                PROCEDURE append_to_clob(p_clob IN OUT CLOB, p_text IN VARCHAR2) IS
                BEGIN
                    DBMS_LOB.WRITEAPPEND(p_clob, LENGTH(p_text), p_text);
                END append_to_clob;

            BEGIN
                -- Initialize the CLOB
                DBMS_LOB.CREATETEMPORARY(p_rec, TRUE);
                DBMS_LOB.TRIM(p_rec, 0);  -- Clear the CLOB in case it has any existing data

                -- Start the JSON array
                append_to_clob(p_rec, '[');

                -- Cursor to fetch employees by manager ID
                FOR employee_rec IN (
                    SELECT employee_id, first_name, last_name, email, phone, hire_date, manager_id, job_title, is_active_fl
                    FROM Employees
                    WHERE manager_id = p_manager_id
                ) LOOP
                    -- Append a comma separator if this is not the first record
                    IF NOT l_first_record THEN
                        append_to_clob(p_rec, ',');
                    ELSE
                        l_first_record := FALSE;
                    END IF;

                    -- Append each employee's JSON object to the CLOB
                    append_to_clob(p_rec, '{"employee_id": ' || TO_CHAR(employee_rec.employee_id));
                    append_to_clob(p_rec, ', "first_name": "' || employee_rec.first_name || '"');
                    append_to_clob(p_rec, ', "last_name": "' || employee_rec.last_name || '"');
                    append_to_clob(p_rec, ', "email": "' || employee_rec.email || '"');
                    append_to_clob(p_rec, ', "phone": "' || employee_rec.phone || '"');
                    append_to_clob(p_rec, ', "hire_date": "' || TO_CHAR(employee_rec.hire_date, 'YYYY-MM-DD') || '"');
                    append_to_clob(p_rec, ', "manager_id": ' || COALESCE(TO_CHAR(employee_rec.manager_id), 'null'));
                    append_to_clob(p_rec, ', "job_title": "' || employee_rec.job_title || '"');
                    append_to_clob(p_rec, ', "is_active_fl": ' || TO_CHAR(employee_rec.is_active_fl) || '}');
                END LOOP;

                -- Close the JSON array
                append_to_clob(p_rec, ']');

                -- Handle case where no employees were found for the manager_id
                IF l_first_record THEN
                    DBMS_LOB.TRIM(p_rec, 0);
                    append_to_clob(p_rec, '[]');  -- Empty JSON array
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    -- Ensure CLOB is cleared on error
                    DBMS_LOB.FREETEMPORARY(p_rec);
                    RAISE_APPLICATION_ERROR(-20001, 'Error in Get_Employees_JSON_CLOB: ' || SQLERRM);
    END Get_Employees_JSON_CLOB;


    

    PROCEDURE save_employees_json (
            p_json_data IN CLOB,
            p_employee_id OUT NUMBER
        ) AS
            l_employee_id    NUMBER;
            l_first_name     VARCHAR2(100);
            l_last_name      VARCHAR2(100);
            l_email          VARCHAR2(100);
            l_phone          VARCHAR2(20);
            l_hire_date      DATE;
            l_manager_id     NUMBER;
            l_job_title      VARCHAR2(100);
            l_is_active_fl   NUMBER;
            l_first_record   BOOLEAN := TRUE;

        BEGIN
            -- Use JSON_TABLE to parse and process each employee record
            FOR rec IN (
                SELECT *
                FROM json_table(
                    p_json_data,
                    '$[*]' COLUMNS (
                        employee_id   NUMBER  PATH '$.employee_id',
                        first_name    VARCHAR2(100) PATH '$.first_name',
                        last_name     VARCHAR2(100) PATH '$.last_name',
                        email         VARCHAR2(100) PATH '$.email',
                        phone         VARCHAR2(20)  PATH '$.phone',
                        hire_date     VARCHAR2(20)  PATH '$.hire_date',
                        manager_id    NUMBER        PATH '$.manager_id',
                        job_title     VARCHAR2(100) PATH '$.job_title',
                        is_active_fl  NUMBER        PATH '$.is_active_fl'
                    )
                )
            ) LOOP
                -- Assign each field to local variables for easier reference
                l_employee_id := rec.employee_id;
                l_first_name  := rec.first_name;
                l_last_name   := rec.last_name;
                l_email       := rec.email;
                l_phone       := rec.phone;
                l_hire_date :=  CASE
                                    WHEN INSTR(rec.hire_date, 'T') > 0 THEN TO_DATE(rec.hire_date, 'YYYY-MM-DD"T"HH24:MI:SS')
                                    ELSE TO_DATE(rec.hire_date, 'YYYY-MM-DD HH24:MI:SS')
                                END;
                l_manager_id  := rec.manager_id;
                l_job_title   := rec.job_title;
                l_is_active_fl := rec.is_active_fl;

                -- Insert or update the employee record
                BEGIN
                    -- Attempt to update an existing record if l_employee_id is not NULL
                    IF l_employee_id IS NOT NULL THEN
                        UPDATE Employees
                        SET first_name = l_first_name,
                            last_name = l_last_name,
                            email = l_email,
                            phone = l_phone,
                            hire_date = l_hire_date,
                            manager_id = l_manager_id,
                            job_title = l_job_title,
                            is_active_fl = l_is_active_fl
                        WHERE employee_id = l_employee_id;
                    END IF;

                    -- Check if update affected any rows or if l_employee_id was NULL
                    IF SQL%ROWCOUNT = 0 THEN
                        -- If no rows were updated, use the sequence to generate a new employee_id if necessary
                        IF l_employee_id IS NULL THEN
                            l_employee_id := employee_id_seq.NEXTVAL;
                        END IF;

                        -- Insert a new record with either the provided employee_id or the sequence-generated ID
                        INSERT INTO Employees (
                            employee_id, first_name, last_name, email, phone, hire_date, manager_id, job_title, is_active_fl
                        ) VALUES (
                            l_employee_id, l_first_name, l_last_name, l_email, l_phone, l_hire_date, l_manager_id, l_job_title, l_is_active_fl
                        );
                    END IF;

                EXCEPTION
                    WHEN OTHERS THEN
                        ROLLBACK;
                        RAISE_APPLICATION_ERROR(-20001, 'Error in save_employees_json: ' || SQLERRM);
                END;
            END LOOP;

            -- Commit the transaction
            COMMIT;

            -- Set the output employee_id
            p_employee_id := l_employee_id;

        EXCEPTION
            WHEN OTHERS THEN
                -- Rollback the transaction if there’s an error
                ROLLBACK;
                RAISE_APPLICATION_ERROR(-20001, 'Error in save_employees_json: ' || SQLERRM);
    END save_employees_json;


END EMPLOYEE_PKG;



