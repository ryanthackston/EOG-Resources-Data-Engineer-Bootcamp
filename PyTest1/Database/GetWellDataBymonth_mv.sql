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