
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