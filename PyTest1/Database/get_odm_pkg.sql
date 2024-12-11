-- anonymous blocks in bottom of this file. 
-- 1) GetWellName anonymous block works. Its a procedure not a function
-- 2) procedure runs. anonymous block works. numbers except well count are in ballpark. 
-- 750000 well count needed to be count distinct. Gas sales, oil sales are null.
-- 3) No procedure.
-- 4) GetDivisionName block works. is a function. good
-- 5) Block works. Missing june data for Mcgregor 5h. No cost for that month and inner join filtered out prod data.
-- 6) procedure runs. anonymous block works. Oil prod was 144,994,335. 
-- 750000 well count needed to be count distinct, correct should be 412. Gas sales, oil sales are null. 
-- 7) The left join vs inner join breaks the weeks that don't have costs. So McGregor 5H did not return anything





SELECT well_name
FROM odm_well
WHERE well_id = p_well_id
AND division_id = p_division_id



CREATE MATERIALIZED VIEW getDivisionData_mv as
WITH prod AS (
                SELECT /*+ PARALLEL(16) */ 
                    c.division_id, 
                    d.division_name, 
                    EXTRACT(YEAR FROM p.date_value) AS Year_id,
                    count(well_id) AS well_count,
                    sum(p.gross_oil_prod) AS total_oil_produced,
                    sum(p.gross_gas_prod) AS total_gas_produced
                FROM odm_completion c 
                JOIN odm_division d ON c.division_id = d.division_id
                JOIN ODM_COMP_PRODUCTION p ON c.completion_id = p.completion_id 
                    AND c.division_id = p.division_id
                GROUP BY 
                    c.division_id, 
                    d.division_name, 
                    EXTRACT(YEAR FROM p.date_value)
            ),
            costs AS (
                SELECT /*+ PARALLEL(16) */
                    dd.division_id,
                    EXTRACT(YEAR FROM dd.rpt_date) AS year_id,
                    SUM(field_est_totl_amt) AS total_costs
                FROM ODM_AFE_DETAIL_DAILY dd 
                GROUP BY dd.division_id, EXTRACT(YEAR FROM dd.rpt_date)
            )
            SELECT DISTINCT
            	p.division_id,
                p.division_name, 
                p.year_id, 
                p.well_count,
                p.total_oil_produced, 
                p.total_oil_produced * avg_oil_price_yr AS total_oil_sales,
                p.total_gas_produced,
                p.total_gas_produced * avg_gas_price_yr AS total_gas_sales,
                c.total_costs
            FROM prod p 
            JOIN costs c ON p.year_id = c.year_id 
                AND p.division_id = c.division_id
            JOIN price_data pd ON pd.year_id = p.year_id;


CREATE MATERIALIZED VIEW GetWellDataBymonth_mv as
WITH prices AS (
SELECT month_id, 
trunc(pd.DATES_FULL, 'MONTH') AS month_start, 
avg(OIL_PRICES_FULL) AS oil_price_monthly, 
avg(GAS_PRICE_BBL) AS gas_price_monthly
FROM PRICE_DATA pd
GROUP BY month_id, 
trunc(pd.DATES_FULL, 'MONTH')
),
prod AS (
SELECT 
w.well_name, 
c.well_id,
w.division_name, 
c.division_id,
extract(MONTH FROM p.date_value) AS month_id, 
trunc(p.date_value, 'MONTH') AS month_start,
sum(gross_oil_prod) AS oil_produced,
sum(gross_gas_prod) AS gas_produced
FROM ODM_COMPLETION c
JOIN ODM_WELL w ON w.WELL_ID = c.WELL_ID AND w.DIVISION_ID = c.DIVISION_ID 
JOIN ODM_COMP_PRODUCTION p ON c.COMPLETION_ID = p.COMPLETION_ID AND c.DIVISION_ID = p.DIVISION_ID 
GROUP BY 
w.well_name, 
c.well_id,
w.division_name, 
c.division_id,
extract(MONTH FROM p.date_value),
trunc(p.date_value, 'MONTH')
),
costs AS (
	SELECT /*+ PARALLEL(16) */  
	h.well_id,
	h.division_id,
	extract(MONTH FROM dd.rpt_date) AS month_id,
	trunc(dd.rpt_date, 'MONTH') AS month_start,
	sum(field_est_totl_amt) AS total_costs
	FROM (SELECT DISTINCT afe_nbr, event_nbr, well_id, division_id, perc_well_id_nbr FROM odm_afe_header) h
	JOIN ODM_AFE_DETAIL_DAILY dd 
		ON h.afe_nbr = dd.afe_nbr 
		AND h.division_id = dd.division_id 
		AND h.perc_well_id_nbr = dd.perc_well_id_nbr
		AND dd.event_nbr = h.event_nbr
	GROUP BY h.well_id,
	h.division_id,
	extract(MONTH FROM dd.rpt_date),
	trunc(dd.rpt_date, 'MONTH')
)
SELECT 
p.division_id,
p.well_name,
p.division_name,
p.month_id,
p.month_start,
round(p.oil_produced, 2) AS oil_produced,
round(pd.oil_price_monthly, 2) AS avg_oil_price,
round(p.oil_produced * pd.oil_price_monthly, 2) AS oil_sales,
round(p.gas_produced, 2) AS gas_produced,
round(pd.gas_price_monthly, 2) AS avg_gas_price,
round(p.oil_produced * pd.oil_price_monthly, 2) AS gas_sales
FROM prod p
JOIN prices pd ON p.month_id = pd.MONTH_id AND p.month_start = pd.month_start
JOIN costs c 
	ON p.well_id = c.well_id 
	AND p.division_id = c.division_id 
	AND p.month_id = c.month_id 
	AND p.month_start = c.month_start



CREATE MATERIALIZED VIEW GetWellDataByWeek_mv as
WITH prices AS (
SELECT 
    week_id, 
    trunc(pd.DATES_FULL, 'IW') AS week_start, 
    avg(OIL_PRICES_FULL) AS oil_price_weekly, 
    avg(GAS_PRICE_BBL) AS gas_price_weekly
FROM 
    PRICE_DATA pd
GROUP BY 
    week_id, 
    trunc(pd.DATES_FULL, 'IW')
),
prod AS (
SELECT 
w.well_name, 
c.well_id,
w.division_name, 
c.division_id,
to_number(to_char(p.date_value, 'IW')) AS week_id, 
trunc(p.date_value, 'IW') AS week_start, 
sum(gross_oil_prod) AS oil_produced,
sum(gross_gas_prod) AS gas_produced
FROM ODM_COMPLETION c
JOIN ODM_WELL w ON w.WELL_ID = c.WELL_ID AND w.DIVISION_ID = c.DIVISION_ID 
JOIN ODM_COMP_PRODUCTION p ON c.COMPLETION_ID = p.COMPLETION_ID AND c.DIVISION_ID = p.DIVISION_ID 
GROUP BY 
w.well_name, 
c.well_id,
w.division_name, 
c.division_id,
to_number(to_char(p.date_value, 'IW')),
trunc(p.date_value, 'IW')
),
costs AS (
	SELECT /*+ PARALLEL(16) */  
	h.well_id,
	h.division_id,
	to_number(to_char(dd.rpt_date, 'IW')) AS week_id, 
	trunc(dd.rpt_date, 'IW') AS week_start,
	sum(field_est_totl_amt) AS total_costs
	FROM (SELECT DISTINCT afe_nbr, event_nbr, well_id, division_id, perc_well_id_nbr FROM odm_afe_header) h
	JOIN ODM_AFE_DETAIL_DAILY dd 
		ON h.afe_nbr = dd.afe_nbr 
		AND h.division_id = dd.division_id 
		AND h.perc_well_id_nbr = dd.perc_well_id_nbr
		AND dd.event_nbr = h.event_nbr
	GROUP BY h.well_id,
	h.division_id,
	to_number(to_char(dd.rpt_date, 'IW')), 
	trunc(dd.rpt_date, 'IW')
)
SELECT 
p.division_id,
p.well_name,
p.division_name,
p.week_id,
p.week_start,
round(p.oil_produced, 2) AS oil_produced,
round(pd.oil_price_weekly, 2) AS avg_oil_price,
round(p.oil_produced * pd.oil_price_weekly, 2) AS oil_sales,
round(p.gas_produced, 2) AS gas_produced,
round(pd.gas_price_weekly, 2) AS avg_gas_price,
round(p.oil_produced * pd.gas_price_weekly, 2) AS gas_sales
FROM prod p
JOIN prices pd ON p.week_id = pd.week_id AND p.week_start = pd.week_start
JOIN costs c 
	ON p.well_id = c.well_id 
	AND p.division_id = c.division_id 
	AND p.week_id = c.week_id 
	AND p.week_start = c.week_start



CREATE OR REPLACE PACKAGE get_odm_pkg AS 

    FUNCTION GetWellName (
            p_well_id     IN odm_well.well_id%TYPE,
            p_division_id IN odm_well.division_id%TYPE
        ) RETURN odm_well.well_name%TYPE;

    PROCEDURE GetDivisionDataById (
        p_division_id IN odm_division.division_id%TYPE,
        o_ref_cursor  OUT SYS_REFCURSOR
    );

    FUNCTION GetDivisionName (
        p_division_id IN odm_division.division_id%TYPE
    ) RETURN odm_division.division_name%TYPE;

    PROCEDURE GetWellDataByMonth (
        p_division_id IN odm_well.division_id%TYPE,
        o_ref_cursor  OUT SYS_REFCURSOR
    );


    PROCEDURE GetDivisionData (
        o_ref_cursor  OUT SYS_REFCURSOR
    );

    PROCEDURE GetWellDataByWeek (
         p_well_id      IN NUMBER,
        p_division_id  IN NUMBER,
        p_ref_cursor   OUT SYS_REFCURSOR
    );


END get_odm_pkg;

CREATE OR REPLACE PACKAGE BODY get_odm_pkg AS 

    FUNCTION GetWellName (
            p_well_id     IN odm_well.well_id%TYPE,
            p_division_id IN odm_well.division_id%TYPE
        ) RETURN odm_well.well_name%TYPE
        AS
            v_well_name odm_well.well_name%TYPE;
        BEGIN
            -- Selecting well_name based on the provided well_id and division_id
            SELECT well_name
            INTO v_well_name
            FROM odm_well
            WHERE well_id = p_well_id
            AND division_id = p_division_id;

            -- Return the well name
            RETURN v_well_name;

        EXCEPTION
            -- Handling case when no well is found
            WHEN NO_DATA_FOUND THEN
                RETURN 'No well found';
            -- Re-raise any other exceptions
            WHEN OTHERS THEN
                RAISE;
    END GetWellName;


    PROCEDURE GetDivisionDataById (
            p_division_id IN odm_division.division_id%TYPE,
            o_ref_cursor   OUT SYS_REFCURSOR
        ) IS
        BEGIN
            OPEN o_ref_cursor FOR
                SELECT 
                    DIVISION_NAME,
                    YEAR_ID,
                    WELL_COUNT,
                    TOTAL_OIL_PRODUCED,
                    TOTAL_OIL_SALES,
                    TOTAL_GAS_PRODUCED,
                    TOTAL_GAS_SALES,
                    TOTAL_COSTS
                FROM getDivisionData_mv
                WHERE division_id = p_division_id;
    END GetDivisionDataById;

    FUNCTION GetDivisionName (
            p_division_id IN odm_division.division_id%TYPE
        ) RETURN odm_division.division_name%TYPE IS
            v_division_name odm_division.division_name%TYPE;
        BEGIN
            -- Select the division name based on the division ID
            SELECT division_name
            INTO v_division_name
            FROM odm_division d
            WHERE d.division_id = p_division_id;
            
            RETURN v_division_name;
        
        EXCEPTION
            -- Handle case when no data is found
            WHEN NO_DATA_FOUND THEN
            RETURN 'No division found';
            -- Handle all other exceptions
            WHEN OTHERS THEN
            RAISE;
    END GetDivisionName;


    PROCEDURE GetWellDataByMonth (
            p_division_id  IN odm_well.division_id%TYPE,
            o_ref_cursor   OUT SYS_REFCURSOR
        )
        IS
        BEGIN
            OPEN o_ref_cursor FOR
                SELECT 
                    WELL_NAME,
                    DIVISION_NAME,
                    MONTH_ID,
                    MONTH_START,
                    OIL_PRODUCED,
                    AVG_OIL_PRICE,
                    OIL_SALES,
                    GAS_PRODUCED,
                    AVG_GAS_PRICE,
                    GAS_SALES 
                FROM GetWellDataBymonth_mv
                WHERE division_id = p_division_id;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('No data found for the given division.');
            WHEN OTHERS THEN
                RAISE;
    END GetWellDataByMonth;



        PROCEDURE GetDivisionData (
                o_ref_cursor  OUT SYS_REFCURSOR
            ) IS
            BEGIN
                OPEN o_ref_cursor FOR
                    SELECT 
                        DIVISION_NAME,
                        YEAR_ID,
                        WELL_COUNT,
                        TOTAL_OIL_PRODUCED,
                        TOTAL_OIL_SALES,
                        TOTAL_GAS_PRODUCED,
                        TOTAL_GAS_SALES,
                        TOTAL_COSTS
                    FROM getDivisionData_mv;
        END GetDivisionData;



    PROCEDURE GetWellDataByWeek (
                p_well_id      IN NUMBER,
                p_division_id  IN NUMBER,
                p_ref_cursor   OUT SYS_REFCURSOR
            )
            IS
            BEGIN
                OPEN p_ref_cursor FOR
                    WITH prices AS (
                        SELECT 
                            week_id, 
                            trunc(pd.DATES_FULL, 'IW') AS week_start, 
                            avg(OIL_PRICES_FULL) AS oil_price_weekly, 
                            avg(GAS_PRICE_BBL) AS gas_price_weekly
                        FROM 
                            PRICE_DATA pd
                        GROUP BY 
                            week_id, 
                            trunc(pd.DATES_FULL, 'IW')
                    ),
                    prod AS (
                        SELECT 
                            w.well_name, 
                            c.well_id,
                            w.division_name, 
                            c.division_id,
                            to_number(to_char(p.date_value, 'IW')) AS week_id, 
                            trunc(p.date_value, 'IW') AS week_start, 
                            sum(gross_oil_prod) AS oil_produced,
                            sum(gross_gas_prod) AS gas_produced
                        FROM ODM_COMPLETION c
                        JOIN ODM_WELL w ON w.WELL_ID = c.WELL_ID AND w.DIVISION_ID = c.DIVISION_ID 
                        JOIN ODM_COMP_PRODUCTION p ON c.COMPLETION_ID = p.COMPLETION_ID AND c.DIVISION_ID = p.DIVISION_ID 
                        WHERE c.WELL_ID = p_well_id 
                        AND c.DIVISION_ID = p_division_id
                        GROUP BY 
                            w.well_name, 
                            c.well_id,
                            w.division_name, 
                            c.division_id,
                            to_number(to_char(p.date_value, 'IW')),
                            trunc(p.date_value, 'IW')
                    ),
                    costs AS (
                        SELECT /*+ PARALLEL(16) */  
                            h.well_id,
                            h.division_id,
                            to_number(to_char(dd.rpt_date, 'IW')) AS week_id, 
                            trunc(dd.rpt_date, 'IW') AS week_start,
                            sum(field_est_totl_amt) AS total_costs
                        FROM (SELECT DISTINCT afe_nbr, event_nbr, well_id, division_id, perc_well_id_nbr FROM odm_afe_header) h
                        JOIN ODM_AFE_DETAIL_DAILY dd 
                            ON h.afe_nbr = dd.afe_nbr 
                            AND h.division_id = dd.division_id 
                            AND h.perc_well_id_nbr = dd.perc_well_id_nbr
                            AND dd.event_nbr = h.event_nbr
                        WHERE h.WELL_ID = p_well_id 
                        AND h.DIVISION_ID = p_division_id
                        GROUP BY h.well_id,
                            h.division_id,
                            to_number(to_char(dd.rpt_date, 'IW')), 
                            trunc(dd.rpt_date, 'IW')
                    )
                    SELECT 
                        p.well_name,
                        p.division_name,
                        p.week_id,
                        p.week_start,
                        round(p.oil_produced, 2) AS oil_produced,
                        round(pd.oil_price_weekly, 2) AS avg_oil_price,
                        round(p.oil_produced * pd.oil_price_weekly, 2) AS oil_sales,
                        round(p.gas_produced, 2) AS gas_produced,
                        round(pd.gas_price_weekly, 2) AS avg_gas_price,
                        round(p.gas_produced * pd.gas_price_weekly, 2) AS gas_sales
                    FROM prod p
                    JOIN prices pd ON p.week_id = pd.week_id AND p.week_start = pd.week_start
                    JOIN costs c 
                        ON p.well_id = c.well_id 
                        AND p.division_id = c.division_id 
                        AND p.week_id = c.week_id 
                        AND p.week_start = c.week_start;
    END GetWellDataByWeek;

END get_odm_pkg;

-- GetWellName
DECLARE
    v_well_name odm_well.well_name%TYPE;
BEGIN
    get_odm_pkg.GetWellName(p_well_id => 1855054384, p_division_id => 63, o_well_name => v_well_name);
    DBMS_OUTPUT.PUT_LINE('Well Name: ' || v_well_name);
END;
/

-- GetDivisionDataById
DECLARE
    v_ref_cursor SYS_REFCURSOR;
    v_division_name odm_division.division_name%TYPE;
    v_year_id NUMBER;
    v_well_count NUMBER;
    v_total_oil_produced NUMBER;
    v_total_oil_sales NUMBER;
    v_total_gas_produced NUMBER;
    v_total_gas_sales NUMBER;
    v_total_costs NUMBER;
BEGIN
    get_odm_pkg.GetDivisionDataById(p_division_id => 63, o_ref_cursor => v_ref_cursor);

    LOOP
        FETCH v_ref_cursor INTO v_division_name, v_year_id, v_well_count, v_total_oil_produced, 
                               v_total_oil_sales, v_total_gas_produced, v_total_gas_sales, v_total_costs;
        EXIT WHEN v_ref_cursor%NOTFOUND;

        DBMS_OUTPUT.PUT_LINE('Division: ' || v_division_name || ', Year: ' || v_year_id || 
                             ', Well Count: ' || v_well_count || ', Oil Produced: ' || v_total_oil_produced || 
                             ', Oil Sales: ' || v_total_oil_sales || ', Gas Produced: ' || v_total_gas_produced || 
                             ', Gas Sales: ' || v_total_gas_sales || ', Total Costs: ' || v_total_costs);
    END LOOP;
    
    CLOSE v_ref_cursor;
END;
/

-- GetDivisionName
DECLARE
    v_division_name odm_division.division_name%TYPE;
BEGIN
    v_division_name := get_odm_pkg.GetDivisionName(p_division_id => 63);
    DBMS_OUTPUT.PUT_LINE('Division Name: ' || v_division_name);
END;
/

-- GetWellDataByMonth
DECLARE
    v_ref_cursor SYS_REFCURSOR;
    v_well_name odm_well.well_name%TYPE;
    v_division_name odm_division.division_name%TYPE;
    v_month_id NUMBER;
    v_month_start DATE;
    v_oil_produced NUMBER;
    v_avg_oil_price NUMBER;
    v_oil_sales NUMBER;
    v_gas_produced NUMBER;
    v_avg_gas_price NUMBER;
    v_gas_sales NUMBER;
BEGIN
    get_odm_pkg.GetWellDataByMonth(p_division_id => 63, o_ref_cursor => v_ref_cursor);

    LOOP
        FETCH v_ref_cursor INTO v_well_name, v_division_name, v_month_id, v_month_start, v_oil_produced, 
                               v_avg_oil_price, v_oil_sales, v_gas_produced, v_avg_gas_price, v_gas_sales;
        EXIT WHEN v_ref_cursor%NOTFOUND;

        DBMS_OUTPUT.PUT_LINE('Well Name: ' || v_well_name || ', Division: ' || v_division_name || 
                             ', Month: ' || v_month_id || ', Oil Produced: ' || v_oil_produced || 
                             ', Avg Oil Price: ' || v_avg_oil_price || ', Oil Sales: ' || v_oil_sales || 
                             ', Gas Produced: ' || v_gas_produced || ', Avg Gas Price: ' || v_avg_gas_price || 
                             ', Gas Sales: ' || v_gas_sales);
    END LOOP;
    
    CLOSE v_ref_cursor;
END;
/

-- GetDivisionData
DECLARE
    v_ref_cursor SYS_REFCURSOR;
    v_division_name odm_division.division_name%TYPE;
    v_year_id NUMBER;
    v_well_count NUMBER;
    v_total_oil_produced NUMBER;
    v_total_oil_sales NUMBER;
    v_total_gas_produced NUMBER;
    v_total_gas_sales NUMBER;
    v_total_costs NUMBER;
BEGIN
    get_odm_pkg.GetDivisionData(o_ref_cursor => v_ref_cursor);

    LOOP
        FETCH v_ref_cursor INTO v_division_name, v_year_id, v_well_count, v_total_oil_produced, 
                               v_total_oil_sales, v_total_gas_produced, v_total_gas_sales, v_total_costs;
        EXIT WHEN v_ref_cursor%NOTFOUND;

        DBMS_OUTPUT.PUT_LINE('Division: ' || v_division_name || ', Year: ' || v_year_id || 
                             ', Well Count: ' || v_well_count || ', Oil Produced: ' || v_total_oil_produced || 
                             ', Oil Sales: ' || v_total_oil_sales || ', Gas Produced: ' || v_total_gas_produced || 
                             ', Gas Sales: ' || v_total_gas_sales || ', Total Costs: ' || v_total_costs);
    END LOOP;
    
    CLOSE v_ref_cursor;
END;
/

-- GetWellDataByWeek
DECLARE
    v_ref_cursor SYS_REFCURSOR;
    v_well_name odm_well.well_name%TYPE;
    v_division_name odm_division.division_name%TYPE;
    v_week_id NUMBER;
    v_week_start DATE;
    v_oil_produced NUMBER;
    v_avg_oil_price NUMBER;
    v_oil_sales NUMBER;
    v_gas_produced NUMBER;
    v_avg_gas_price NUMBER;
    v_gas_sales NUMBER;
BEGIN
    get_odm_pkg.GetWellDataByWeek(p_well_id => 1855054384, p_division_id => 63, p_ref_cursor => v_ref_cursor);

    LOOP
        FETCH v_ref_cursor INTO v_well_name, v_division_name, v_week_id, v_week_start, v_oil_produced, 
                               v_avg_oil_price, v_oil_sales, v_gas_produced, v_avg_gas_price, v_gas_sales;
        EXIT WHEN v_ref_cursor%NOTFOUND;

        DBMS_OUTPUT.PUT_LINE('Well Name: ' || v_well_name || ', Division: ' || v_division_name || 
                             ', Week: ' || v_week_id || ', Oil Produced: ' || v_oil_produced || 
                             ', Avg Oil Price: ' || v_avg_oil_price || ', Oil Sales: ' || v_oil_sales || 
                             ', Gas Produced: ' || v_gas_produced || ', Avg Gas Price: ' || v_avg_gas_price || 
                             ', Gas Sales: ' || v_gas_sales);
    END LOOP;
    
    CLOSE v_ref_cursor;
END;
/





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
    -- Call the procedure with division_id = 63
    Get_ODM_Pkg.GetDivisionDataById(63, v_ref_cursor);

    -- Fetch the data from the ref cursor
    LOOP
        FETCH v_ref_cursor INTO v_division_name, v_year_id, v_well_count, 
                              v_total_oil_produced, v_total_oil_sales, 
                              v_total_gas_produced, v_total_gas_sales, v_total_costs;
        EXIT WHEN v_ref_cursor%NOTFOUND;

        -- Display the fetched data
        DBMS_OUTPUT.PUT_LINE('Division Name: ' || v_division_name);
        DBMS_OUTPUT.PUT_LINE('Year ID: ' || v_year_id);
        DBMS_OUTPUT.PUT_LINE('Well Count: ' || v_well_count);
        DBMS_OUTPUT.PUT_LINE('Total Oil Produced: ' || v_total_oil_produced);
        DBMS_OUTPUT.PUT_LINE('Total Oil Sales: ' || v_total_oil_sales);
        DBMS_OUTPUT.PUT_LINE('Total Gas Produced: ' || v_total_gas_produced);
        DBMS_OUTPUT.PUT_LINE('Total Gas Sales: ' || v_total_gas_sales);
        DBMS_OUTPUT.PUT_LINE('Total Costs: ' || v_total_costs);
        DBMS_OUTPUT.PUT_LINE('--------------------------------------------');
    END LOOP;

    -- Close the cursor
    CLOSE v_ref_cursor;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        IF v_ref_cursor%ISOPEN THEN
            CLOSE v_ref_cursor;
        END IF;
END;