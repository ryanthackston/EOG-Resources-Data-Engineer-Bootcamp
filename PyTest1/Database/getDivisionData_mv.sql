CREATE MATERIALIZED VIEW getDivisionData_mv as
WITH prod AS (
                SELECT /*+ PARALLEL(16) */ 
                    c.division_id, 
                    d.division_name, 
                    EXTRACT(YEAR FROM p.date_value) AS Year_id,
                    count(distinct well_id) AS well_count,
                    sum(p.gross_oil_prod) AS total_oil_produced,
                    sum(p.gross_gas_prod) AS total_gas_produced
                FROM odm_completion c 
                JOIN odm_division d ON c.division_id = d.division_id
                JOIN ODM_COMP_PRODUCTION p ON c.completion_id = p.completion_id 
                    AND c.division_id = p.division_id
                join price_data pd 
                    on pd.week_id = to_char(p.date_value,'WW')
                    and pd.month_id = extract(month from p.date_value)
                    and pd.year_id = extract(year from p.date_value)
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
                AND p.division_id = c.division_id;