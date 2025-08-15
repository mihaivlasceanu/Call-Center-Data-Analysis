/*=========================
 IMPORTING THE DATASETS
=========================*/

CREATE TABLE calls_data (
	call_id INTEGER,
	call_datetime TEXT,
	customer TEXT,
	call_reason TEXT,
	agent TEXT,
	abandon_flag BOOLEAN,
	first_contact_resolution_flag BOOLEAN,
	customer_satisfaction INTEGER,
	handle_time INTEGER,
	wait_time INTEGER,
	talk_time INTEGER
)


-- \COPY calls_data FROM 'C:\Users\Mihai\Desktop\Call Center\calls_data_expert.csv' WITH CSV HEADER DELIMITER ','

SHOW DATESTYLE

SET DATESTYLE = DMY

ALTER TABLE calls_data
ALTER COLUMN call_datetime TYPE TIMESTAMP USING call_datetime::TIMESTAMP WITHOUT TIME ZONE

SELECT
*
FROM calls_data
LIMIT 5

/*=========================
 CREATING ADDITIONAL TABLES
=========================*/

-- Creating a date scaffolding/dimension table

CREATE TABLE date_scaffolding (
date_time TIMESTAMP
)

-- \COPY date_scaffolding FROM 'C:\Users\Mihai\Desktop\Call Center\scaffolding_single_column_v2.csv' WITH CSV HEADER DELIMITER ','

SELECT * FROM date_scaffolding
LIMIT 20

-- Creating a cross table with dates and employee names only

SELECT 
DISTINCT agent
INTO agents_only
FROM calls_data

SELECT * FROM agents_only

CREATE TABLE date_and_agents AS
SELECT * FROM agents_only
CROSS JOIN date_scaffolding
ORDER BY 1,2

SELECT * FROM date_and_agents
LIMIT 20

-- Creating a cross table with dates and customer names only

SELECT 
DISTINCT customer
INTO customers_only
FROM calls_data

SELECT * FROM customers_only

CREATE TABLE date_and_customers AS
SELECT * FROM customers_only
CROSS JOIN date_scaffolding
ORDER BY 1,2

SELECT * FROM date_and_customers
LIMIT 20

-- Creating a cross table with dates, customer names and call reasons only

SELECT 
DISTINCT call_reason
INTO call_reasons_only
FROM calls_data

SELECT * FROM call_reasons_only

CREATE TABLE date_and_customers_and_reasons AS
SELECT * FROM customers_only
CROSS JOIN date_scaffolding
CROSS JOIN call_reasons_only
ORDER BY 1,2

SELECT * FROM date_and_customers_and_reasons
LIMIT 20

-- Creating a cross table with dates, agent names and call reasons only

CREATE TABLE date_and_agents_and_reasons AS
SELECT * FROM agents_only
CROSS JOIN date_scaffolding
CROSS JOIN call_reasons_only
ORDER BY 1,2

SELECT * FROM date_and_agents_and_reasons
LIMIT 20


/*=========================
 PERFORMANCE OVERVIEW
=========================*/

-- 1.1 TIC - Total Incoming Calls
-- percent change vs previous month (MoM, Month-over-Month)
-- percent change vs same month, previous year (SMPY)

WITH tic_cte AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
COUNT(call_id) AS tic,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2
)

SELECT
year,
month,
tic,
LAG(tic, 1) OVER (ORDER BY year, month_order) AS tic_mom,
ROUND(100.0*(tic - LAG(tic, 1) OVER (ORDER BY year, month_order)) / LAG(tic, 1) OVER (ORDER BY year, month_order), 2) AS pct_change_mom,
LAG(tic, 12) OVER (ORDER BY year, month_order) AS tic_smpy,
ROUND(100.0*(tic - LAG(tic, 12) OVER (ORDER BY year, month_order)) / LAG(tic, 12) OVER (ORDER BY year, month_order), 2) AS pct_change_smpy
FROM tic_cte

-- 1.2 TIC - Total Incoming Calls (percent change Year-to-Date)

WITH tic_cte AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
COUNT(call_id) AS tic,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2
)

, tic_ytd_cte AS (
SELECT
year,
month,
month_order,
tic,
SUM(tic) OVER (PARTITION BY year ORDER BY year, month_order) AS tic_ytd
FROM tic_cte
GROUP BY year, month, month_order, tic
)

SELECT
year, 
month, 
tic_ytd,
LAG(tic_ytd, 12) OVER (ORDER BY year, month_order) AS tic_ytd_prev_year,
ROUND(100.0*(tic_ytd - LAG(tic_ytd, 12) OVER (ORDER BY year, month_order)) / LAG(tic_ytd, 12) OVER (ORDER BY year, month_order), 1) AS pct_change_ytd
FROM tic_ytd_cte


-- 2.1 Abandoned Calls
-- percent change vs previous month (MoM, Month-over-Month)
-- percent change vs same month, previous year (SMPY)

WITH abandoned_cte AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
COUNT(call_id) FILTER (WHERE abandon_flag = true) AS abandoned_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2
)

SELECT
year,
month,
abandoned_calls,
LAG(abandoned_calls, 1) OVER (ORDER BY year, month_order) AS abandoned_mom,
ROUND(100.0*(abandoned_calls - LAG(abandoned_calls, 1) OVER (ORDER BY year, month_order)) / LAG(abandoned_calls, 1) OVER (ORDER BY year, month_order), 2) AS pct_change_mom,
LAG(abandoned_calls, 12) OVER (ORDER BY year, month_order) AS abandoned_smpy,
ROUND(100.0*(abandoned_calls - LAG(abandoned_calls, 12) OVER (ORDER BY year, month_order)) / LAG(abandoned_calls, 12) OVER (ORDER BY year, month_order), 2) AS pct_change_smpy
FROM abandoned_cte

-- 2.2 Abandoned Calls (percent change Year-to-Date)

WITH abandoned_cte AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
COUNT(call_id) FILTER (WHERE abandon_flag = true) AS abandoned_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2
)

, abandoned_ytd_cte AS (
SELECT
year,
month,
month_order,
abandoned_calls,
SUM(abandoned_calls) OVER (PARTITION BY year ORDER BY year, month_order) AS abandoned_ytd
FROM abandoned_cte
GROUP BY year, month, month_order, abandoned_calls
)

SELECT
year, 
month, 
abandoned_ytd,
LAG(abandoned_ytd, 12) OVER (ORDER BY year, month_order) AS abandoned_ytd_prev_year,
ROUND(100.0*(abandoned_ytd - LAG(abandoned_ytd, 12) OVER (ORDER BY year, month_order)) / LAG(abandoned_ytd, 12) OVER (ORDER BY year, month_order), 1) AS pct_change_ytd
FROM abandoned_ytd_cte


-- 3.1 FCR - First Contact Resolution
-- percent change vs previous month (MoM, Month-over-Month)
-- percent change vs same month, previous year (SMPY)

WITH fcr_cte AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true) AS fcr,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2
)

SELECT
year,
month,
fcr,
LAG(fcr, 1) OVER (ORDER BY year, month_order) AS fcr_mom,
ROUND(100.0*(fcr - LAG(fcr, 1) OVER (ORDER BY year, month_order)) / LAG(fcr, 1) OVER (ORDER BY year, month_order), 2) AS pct_change_mom,
LAG(fcr, 12) OVER (ORDER BY year, month_order) AS fcr_smpy,
ROUND(100.0*(fcr - LAG(fcr, 12) OVER (ORDER BY year, month_order)) / LAG(fcr, 12) OVER (ORDER BY year, month_order), 2) AS pct_change_smpy
FROM fcr_cte

-- 3.2 FCR (percent change Year-to-Date)

WITH fcr_cte AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true) AS fcr,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2
)

, fcr_ytd_cte AS (
SELECT
year,
month,
month_order,
fcr,
SUM(fcr) OVER (PARTITION BY year ORDER BY year, month_order) AS fcr_ytd
FROM fcr_cte
GROUP BY year, month, month_order, fcr
)

SELECT
year, 
month, 
fcr_ytd,
LAG(fcr_ytd, 12) OVER (ORDER BY year, month_order) AS fcr_ytd_prev_year,
ROUND(100.0*(fcr_ytd - LAG(fcr_ytd, 12) OVER (ORDER BY year, month_order)) / LAG(fcr_ytd, 12) OVER (ORDER BY year, month_order), 1) AS pct_change_ytd
FROM fcr_ytd_cte


-- 4.1 Average Talk Time
-- percent change vs previous month (MoM, Month-over-Month)
-- percent change vs same month, previous year (SMPY)

WITH talk_cte AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
ROUND(AVG(talk_time),1) AS avg_talk_time,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2
)

SELECT
year,
month,
avg_talk_time,
LAG(avg_talk_time, 1) OVER (ORDER BY year, month_order) AS avg_talk_time_mom,
ROUND(100.0*(avg_talk_time - LAG(avg_talk_time, 1) OVER (ORDER BY year, month_order)) / LAG(avg_talk_time, 1) OVER (ORDER BY year, month_order), 2) AS pct_change_mom,
LAG(avg_talk_time, 12) OVER (ORDER BY year, month_order) AS avg_talk_time_smpy,
ROUND(100.0*(avg_talk_time - LAG(avg_talk_time, 12) OVER (ORDER BY year, month_order)) / LAG(avg_talk_time, 12) OVER (ORDER BY year, month_order), 2) AS pct_change_smpy
FROM talk_cte

-- 4.2 Average Talk Time (percent change Year-to-Date)

WITH talk_cte AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
ROUND(AVG(talk_time),1) AS avg_talk_time,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2
)

, talk_ytd_cte AS (
SELECT
year,
month,
month_order,
avg_talk_time,
ROUND(AVG(avg_talk_time) OVER (PARTITION BY year ORDER BY year, month_order), 1) AS avg_talk_time_ytd
FROM talk_cte
GROUP BY year, month, month_order, avg_talk_time
)

SELECT
year, 
month, 
avg_talk_time_ytd,
LAG(avg_talk_time_ytd, 12) OVER (ORDER BY year, month_order) AS avg_talk_time_ytd_prev_year,
ROUND(100.0*(avg_talk_time_ytd - LAG(avg_talk_time_ytd, 12) OVER (ORDER BY year, month_order)) / LAG(avg_talk_time_ytd, 12) OVER (ORDER BY year, month_order), 1) AS pct_change_ytd
FROM talk_ytd_cte


-- 5.1 Average Wait Till Answer
-- percent change vs previous month (MoM, Month-0ver-Month)
-- percent change vs same month, previous year (SMPY)

WITH wait_cte AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
ROUND(AVG(wait_time),1) AS avg_wait_time,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2
)

SELECT
year,
month,
avg_wait_time,
LAG(avg_wait_time, 1) OVER (ORDER BY year, month_order) AS avg_wait_time_mom,
ROUND(100.0*(avg_wait_time - LAG(avg_wait_time, 1) OVER (ORDER BY year, month_order)) / LAG(avg_wait_time, 1) OVER (ORDER BY year, month_order), 2) AS pct_change_mom,
LAG(avg_wait_time, 12) OVER (ORDER BY year, month_order) AS avg_wait_time_smpy,
ROUND(100.0*(avg_wait_time - LAG(avg_wait_time, 12) OVER (ORDER BY year, month_order)) / LAG(avg_wait_time, 12) OVER (ORDER BY year, month_order), 2) AS pct_change_smpy
FROM wait_cte

-- 5.2 Average Wait Till Answer (percent change Year-to-Date)

WITH wait_cte AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
ROUND(AVG(wait_time),1) AS avg_wait_time,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2
)

, wait_ytd_cte AS (
SELECT
year,
month,
month_order,
avg_wait_time,
ROUND(AVG(avg_wait_time) OVER (PARTITION BY year ORDER BY year, month_order), 1) AS avg_wait_time_ytd
FROM wait_cte
GROUP BY year, month, month_order, avg_wait_time
)

SELECT
year, 
month, 
avg_wait_time_ytd,
LAG(avg_wait_time_ytd, 12) OVER (ORDER BY year, month_order) AS avg_wait_time_ytd_prev_year,
ROUND(100.0*(avg_wait_time_ytd - LAG(avg_wait_time_ytd, 12) OVER (ORDER BY year, month_order)) / LAG(avg_wait_time_ytd, 12) OVER (ORDER BY year, month_order), 1) AS pct_change_ytd
FROM wait_ytd_cte


-- 6.1 CSAT - Customer Satisfaction Score
-- percent change vs previous month (MoM, Month-over-Month)
-- percent change vs same month, previous year (SMPY)

WITH csat_cte AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
ROUND(AVG(customer_satisfaction),1) AS avg_csat,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2
)

SELECT
year,
month,
avg_csat,
LAG(avg_csat, 1) OVER (ORDER BY year, month_order) AS avg_csat_mom,
ROUND(100.0*(avg_csat - LAG(avg_csat, 1) OVER (ORDER BY year, month_order)) / LAG(avg_csat, 1) OVER (ORDER BY year, month_order), 2) AS pct_change_mom,
LAG(avg_csat, 12) OVER (ORDER BY year, month_order) AS avg_csat_smpy,
ROUND(100.0*(avg_csat - LAG(avg_csat, 12) OVER (ORDER BY year, month_order)) / LAG(avg_csat, 12) OVER (ORDER BY year, month_order), 2) AS pct_change_smpy
FROM csat_cte

-- 6.2 CSAT - Customer Satisfaction Score (percent change Year-to-Date)

WITH csat_cte AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
ROUND(AVG(customer_satisfaction),1) AS avg_csat,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2
)

, csat_ytd_cte AS (
SELECT
year,
month,
month_order,
avg_csat,
ROUND(AVG(avg_csat) OVER (PARTITION BY year ORDER BY year, month_order), 1) AS avg_csat_ytd
FROM csat_cte
GROUP BY year, month, month_order, avg_csat
)

SELECT
year, 
month, 
avg_csat_ytd,
LAG(avg_csat_ytd, 12) OVER (ORDER BY year, month_order) AS avg_csat_ytd_prev_year,
ROUND(100.0*(avg_csat_ytd - LAG(avg_csat_ytd, 12) OVER (ORDER BY year, month_order)) / LAG(avg_csat_ytd, 12) OVER (ORDER BY year, month_order), 1) AS pct_change_ytd
FROM csat_ytd_cte


-- 7.1 Incoming Calls by Business Day and Hour

WITH day_hour_cte AS (
SELECT 
*,
DATE_TRUNC('hour', call_datetime) AS call_datetime_2,
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
TRIM(TO_CHAR(call_datetime, 'Day')) AS day,
DATE_PART('hour', DATE_TRUNC('hour', call_datetime)) AS hour
FROM calls_data
)

, day_hour_cte_2 AS (
SELECT
call_datetime_2,
year,
month,
day,
hour,
COUNT(*) AS calls
FROM day_hour_cte
GROUP BY 1,2,3,4,5
)

, day_hour_cte_3 AS (
SELECT
year, 
month,
day,
hour,
ROUND(AVG(calls)) As avg_calls
FROM day_hour_cte_2
GROUP BY 1,2,3,4
)

SELECT
year,
month,
day,
MAX(CASE WHEN hour = 7 THEN avg_calls END) AS "7",
MAX(CASE WHEN hour = 8 THEN avg_calls END) AS "8",
MAX(CASE WHEN hour = 9 THEN avg_calls END) AS "9",
MAX(CASE WHEN hour = 10 THEN avg_calls END) AS "10",
MAX(CASE WHEN hour = 11 THEN avg_calls END) AS "11",
MAX(CASE WHEN hour = 12 THEN avg_calls END) AS "12",
MAX(CASE WHEN hour = 13 THEN avg_calls END) AS "13",
MAX(CASE WHEN hour = 14 THEN avg_calls END) AS "14",
MAX(CASE WHEN hour = 15 THEN avg_calls END) AS "15",
MAX(CASE WHEN hour = 16 THEN avg_calls END) AS "16",
MAX(CASE WHEN hour = 17 THEN avg_calls END) AS "17",
MAX(CASE WHEN hour = 18 THEN avg_calls END) AS "18",
MAX(CASE WHEN hour = 19 THEN avg_calls END) AS "19",
MAX(CASE WHEN hour = 20 THEN avg_calls END) AS "20"
FROM day_hour_cte_3
GROUP by year, month, day
ORDER BY year, 
		 ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month),
		 ARRAY_POSITION(ARRAY['Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], day)
LIMIT 20

-- 7.2 Incoming Calls by Business Day and Hour (Year-to-Date)

WITH day_hour_cte AS (
SELECT 
*,
DATE_TRUNC('hour', call_datetime) AS call_datetime_2,
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
TRIM(TO_CHAR(call_datetime, 'Day')) AS day,
DATE_PART('hour', DATE_TRUNC('hour', call_datetime)) AS hour
FROM calls_data
)

, day_hour_cte_2 AS (
SELECT
call_datetime_2,
year,
month,
day,
hour,
COUNT(*) AS calls
FROM day_hour_cte
GROUP BY 1,2,3,4,5
)

, day_hour_cte_3 AS (
SELECT
year, 
month,
day,
hour,
ROUND(AVG(calls)) As avg_calls
FROM day_hour_cte_2
GROUP BY 1,2,3,4
)

, day_hour_ytd AS (
SELECT
year, 
month,
day,
hour,
avg_calls,
ROUND(AVG(avg_calls) OVER (PARTITION BY day, hour, year ORDER BY year, ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month))) AS avg_calls_ytd
FROM day_hour_cte_3
ORDER BY year, 
		 ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month),
		 ARRAY_POSITION(ARRAY['Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], day),
		 hour
)

SELECT
year,
month,
day,
MAX(CASE WHEN hour = 7 THEN avg_calls_ytd END) AS "7",
MAX(CASE WHEN hour = 8 THEN avg_calls_ytd END) AS "8",
MAX(CASE WHEN hour = 9 THEN avg_calls_ytd END) AS "9",
MAX(CASE WHEN hour = 10 THEN avg_calls_ytd END) AS "10",
MAX(CASE WHEN hour = 11 THEN avg_calls_ytd END) AS "11",
MAX(CASE WHEN hour = 12 THEN avg_calls_ytd END) AS "12",
MAX(CASE WHEN hour = 13 THEN avg_calls_ytd END) AS "13",
MAX(CASE WHEN hour = 14 THEN avg_calls_ytd END) AS "14",
MAX(CASE WHEN hour = 15 THEN avg_calls_ytd END) AS "15",
MAX(CASE WHEN hour = 16 THEN avg_calls_ytd END) AS "16",
MAX(CASE WHEN hour = 17 THEN avg_calls_ytd END) AS "17",
MAX(CASE WHEN hour = 18 THEN avg_calls_ytd END) AS "18",
MAX(CASE WHEN hour = 19 THEN avg_calls_ytd END) AS "19",
MAX(CASE WHEN hour = 20 THEN avg_calls_ytd END) AS "20"
FROM day_hour_ytd
GROUP by year, month, day
ORDER BY year, 
		 ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month),
		 ARRAY_POSITION(ARRAY['Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], day)
LIMIT 20

-- 8.1 Top Customers by TIC (Total Incoming Calls) + percent change MoM (Month-over-Month)

WITH customer_tic AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
COUNT(call_id) AS tic
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, tic_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
COALESCE(tic, 0) AS tic
FROM all_months t1
LEFT JOIN customer_tic t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_tic_mom AS (
SELECT 
year,
month,
customer,
tic,
LAG(tic, 1) OVER (PARTITION BY customer ORDER BY year, month) AS tic_pm,
ROUND(100.0*(tic - LAG(tic, 1) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(tic, 1) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change_mom,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY tic DESC) AS customer_rank
FROM tic_with_missing_months_added
)

SELECT
*
FROM customer_tic_mom
WHERE customer_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20

-- 8.2 Top Customers by TIC (Total Incoming Calls) + percent change SMPY (same month, previous year)

WITH customer_tic AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
COUNT(call_id) AS tic
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, tic_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
COALESCE(tic, 0) AS tic
FROM all_months t1
LEFT JOIN customer_tic t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_tic_smpy AS (
SELECT 
year,
month,
customer,
tic,
LAG(tic, 12) OVER (PARTITION BY customer ORDER BY year, month) AS tic_smpy,
ROUND(100.0*(tic - LAG(tic, 12) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(tic, 12) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change_smpy,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY tic DESC) AS customer_rank
FROM tic_with_missing_months_added
)

SELECT
*
FROM customer_tic_smpy
WHERE customer_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 100

-- 8.3 Top Customers by TIC (Total Incoming Calls) + percent change YTD (Year-to-Date)

WITH customer_tic AS (
SELECT
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
customer,
COUNT(call_id) AS tic
FROM calls_data
GROUP BY 1,2,3
)

, customer_tic_ytd AS (
SELECT
year,
month,
customer,
tic,
SUM(tic) OVER (PARTITION BY year, customer ORDER BY year, month) AS tic_ytd
FROM customer_tic
)

, customer_tic_ytd_rank AS (
SELECT 
year,
month,
customer,
tic_ytd,
LAG(tic_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month) AS tic_pytd,
ROUND(100.0*(tic_ytd - LAG(tic_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(tic_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY tic_ytd DESC) AS customer_rank
FROM customer_tic_ytd
) 

SELECT
*
FROM customer_tic_ytd_rank
WHERE customer_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 100


-- 9.1 Top Customers by AHT (Average Handle Time) + percent change MoM (Month-over-Month)

WITH customer_aht AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(AVG(handle_time), 1) AS aht
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, aht_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
aht
FROM all_months t1
LEFT JOIN customer_aht t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_aht_mom AS (
SELECT 
year,
month,
customer,
aht,
LAG(aht, 1) OVER (PARTITION BY customer ORDER BY year, month) AS aht_pm,
ROUND(100.0*(aht - LAG(aht, 1) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(aht, 1) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change_mom,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY aht DESC NULLS LAST) AS customer_rank
FROM aht_with_missing_months_added
)

SELECT
*
FROM customer_aht_mom
WHERE customer_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20

-- 9.2 Top Customers by AHT (Average Handle Time) + percent change SMPY (same month, previous year)

WITH customer_aht AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(AVG(handle_time), 1) AS aht
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, aht_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
aht
FROM all_months t1
LEFT JOIN customer_aht t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_aht_smpy AS (
SELECT 
year,
month,
customer,
aht,
LAG(aht, 12) OVER (PARTITION BY customer ORDER BY year, month) AS aht_smpy,
ROUND(100.0*(aht - LAG(aht, 12) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(aht, 12) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change_smpy,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY aht DESC NULLS LAST) AS customer_rank
FROM aht_with_missing_months_added
)

SELECT
*
FROM customer_aht_smpy
WHERE customer_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 102

-- 9.3 Top Customers by AHT (Average Handle Time) + percent change YTD (Year-to-Date)

WITH customer_aht AS (
SELECT
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
customer,
ROUND(AVG(handle_time), 1) AS aht
FROM calls_data
GROUP BY 1,2,3
)

, customer_aht_ytd AS (
SELECT
year,
month,
customer,
aht,
ROUND(AVG(aht) OVER (PARTITION BY year, customer ORDER BY year, month), 1) AS aht_ytd
FROM customer_aht
)

, customer_aht_ytd_rank AS (
SELECT 
year,
month,
customer,
aht_ytd,
LAG(aht_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month) AS aht_pytd,
ROUND(100.0*(aht_ytd - LAG(aht_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(aht_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY aht_ytd DESC NULLS LAST) AS customer_rank
FROM customer_aht_ytd
) 

SELECT
*
FROM customer_aht_ytd_rank
WHERE customer_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 100


-- 10.1 Top Customers by FCR (First Contact Resolution) + percent change MoM (Month-over-Month)

WITH customer_fcr AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true) AS fcr
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, fcr_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
fcr
FROM all_months t1
LEFT JOIN customer_fcr t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_fcr_mom AS (
SELECT 
year,
month,
customer,
fcr,
LAG(fcr, 1) OVER (PARTITION BY customer ORDER BY year, month) AS fcr_pm,
ROUND(100.0*(fcr - LAG(fcr, 1) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(fcr, 1) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change_mom,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY fcr DESC NULLS LAST) AS customer_rank
FROM fcr_with_missing_months_added
)

SELECT
*
FROM customer_fcr_mom
WHERE customer_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20

-- 10.2 Top Customers by FCR (First Contact Resolution) + percent change SMPY (same month, previous year)

WITH customer_fcr AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true) AS fcr
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, fcr_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
fcr
FROM all_months t1
LEFT JOIN customer_fcr t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_fcr_smpy AS (
SELECT 
year,
month,
customer,
fcr,
LAG(fcr, 12) OVER (PARTITION BY customer ORDER BY year, month) AS fcr_smpy,
ROUND(100.0*(fcr - LAG(fcr, 12) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(fcr, 12) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change_smpy,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY fcr DESC NULLS LAST) AS customer_rank
FROM fcr_with_missing_months_added
)

SELECT
*
FROM customer_fcr_smpy
WHERE customer_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 102

-- 10.3 Top Customers by FCR (First Contact Resolution) + percent change YTD (Year-to-Date)

WITH customer_fcr AS (
SELECT
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
customer,
COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true) AS fcr
FROM calls_data
GROUP BY 1,2,3
)

, customer_fcr_ytd AS (
SELECT
year,
month,
customer,
fcr,
SUM(fcr) OVER (PARTITION BY year, customer ORDER BY year, month) AS fcr_ytd
FROM customer_fcr
)

, customer_fcr_ytd_rank AS (
SELECT 
year,
month,
customer,
fcr_ytd,
LAG(fcr_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month) AS fcr_pytd,
ROUND(100.0*(fcr_ytd - LAG(fcr_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(fcr_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY fcr_ytd DESC NULLS LAST) AS customer_rank
FROM customer_fcr_ytd
) 

SELECT
*
FROM customer_fcr_ytd_rank
WHERE customer_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 102


-- 11.1 Top Agents by TIC (Total Incoming Calls) + percent change MoM (Month-over-Month)

WITH agent_tic AS (
SELECT
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
agent,
COUNT(call_id) AS tic
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, tic_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
tic
FROM all_months t1
LEFT JOIN agent_tic t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_tic_mom AS (
SELECT 
year,
month,
agent,
tic,
LAG(tic, 1) OVER (PARTITION BY agent ORDER BY year, month) AS tic_pm,
ROUND(100.0*(tic - LAG(tic, 1) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(tic, 1) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change_mom,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY tic DESC NULLS LAST) AS agent_rank
FROM tic_with_missing_months_added
)

SELECT
*
FROM agent_tic_mom
WHERE agent_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20

-- 11.2 Top Agents by TIC (Total Incoming Calls) + percent change SMPY (same month, previous year)

WITH agent_tic AS (
SELECT
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
agent,
COUNT(call_id) AS tic
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, tic_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
tic
FROM all_months t1
LEFT JOIN agent_tic t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_tic_smpy AS (
SELECT 
year,
month,
agent,
tic,
LAG(tic, 12) OVER (PARTITION BY agent ORDER BY year, month) AS tic_smpy,
ROUND(100.0*(tic - LAG(tic, 12) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(tic, 12) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change_smpy,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY tic DESC NULLS LAST) AS agent_rank
FROM tic_with_missing_months_added
)

SELECT
*
FROM agent_tic_smpy
WHERE agent_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 102

-- 11.3 Top Agents by TIC (Total Incoming Calls) + percent change YTD (Year-to-Date)

WITH agent_tic AS (
SELECT
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
agent,
COUNT(call_id) AS tic
FROM calls_data
GROUP BY 1,2,3
)

, agent_tic_ytd AS (
SELECT
year,
month,
agent,
tic,
SUM(tic) OVER (PARTITION BY year, agent ORDER BY year, month) AS tic_ytd
FROM agent_tic
)

, agent_tic_ytd_rank AS (
SELECT 
year,
month,
agent,
tic_ytd,
LAG(tic_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month) AS tic_pytd,
ROUND(100.0*(tic_ytd - LAG(tic_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(tic_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY tic_ytd DESC) AS agent_rank
FROM agent_tic_ytd
) 

SELECT
*
FROM agent_tic_ytd_rank
WHERE agent_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 101


-- 12.1 Top Agents by AHT (Average Handle Time) + percent change MoM (Month-over-Month)

WITH agent_aht AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(AVG(handle_time), 1) AS aht
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, aht_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
aht
FROM all_months t1
LEFT JOIN agent_aht t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_aht_mom AS (
SELECT 
year,
month,
agent,
aht,
LAG(aht, 1) OVER (PARTITION BY agent ORDER BY year, month) AS aht_pm,
ROUND(100.0*(aht - LAG(aht, 1) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(aht, 1) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change_mom,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY aht DESC NULLS LAST) AS agent_rank
FROM aht_with_missing_months_added
)

SELECT
*
FROM agent_aht_mom
WHERE agent_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20

-- 12.2 Top Agents by AHT (Average Handle Time) + percent change SMPY (same month, previous year)

WITH agent_aht AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(AVG(handle_time), 1) AS aht
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, aht_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
aht
FROM all_months t1
LEFT JOIN agent_aht t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_aht_smpy AS (
SELECT 
year,
month,
agent,
aht,
LAG(aht, 12) OVER (PARTITION BY agent ORDER BY year, month) AS aht_smpy,
ROUND(100.0*(aht - LAG(aht, 12) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(aht, 12) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change_smpy,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY aht DESC NULLS LAST) AS agent_rank
FROM aht_with_missing_months_added
)

SELECT
*
FROM agent_aht_smpy
WHERE agent_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 100

-- 12.3 Top Agents by AHT (Average Handle Time) + percent change YTD (Year-to-Date)

WITH agent_aht AS (
SELECT
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
agent,
ROUND(AVG(handle_time), 1) AS aht
FROM calls_data
GROUP BY 1,2,3
)

, agent_aht_ytd AS (
SELECT
year,
month,
agent,
aht,
ROUND(AVG(aht) OVER (PARTITION BY year, agent ORDER BY year, month), 1) AS aht_ytd
FROM agent_aht
)

, agent_aht_ytd_rank AS (
SELECT 
year,
month,
agent,
aht_ytd,
LAG(aht_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month) AS aht_pytd,
ROUND(100.0*(aht_ytd - LAG(aht_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(aht_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY aht_ytd DESC NULLS LAST) AS agent_rank
FROM agent_aht_ytd
) 

SELECT
*
FROM agent_aht_ytd_rank
WHERE agent_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 100


-- 13.1 Top Agents by FCR (First Contact Resolution) + percent change MoM (Month-over-Month)

WITH agent_fcr AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true) AS fcr
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, fcr_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
fcr
FROM all_months t1
LEFT JOIN agent_fcr t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_fcr_mom AS (
SELECT 
year,
month,
agent,
fcr,
LAG(fcr, 1) OVER (PARTITION BY agent ORDER BY year, month) AS fcr_pm,
ROUND(100.0*(fcr - LAG(fcr, 1) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(fcr, 1) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change_mom,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY fcr DESC NULLS LAST) AS agent_rank
FROM fcr_with_missing_months_added
)

SELECT
*
FROM agent_fcr_mom
WHERE agent_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20

-- 13.2 Top Agents by FCR (First Contact Resolution) + percent change SMPY (same month, previous year)

WITH agent_fcr AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true) AS fcr
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, fcr_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
fcr
FROM all_months t1
LEFT JOIN agent_fcr t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_fcr_smpy AS (
SELECT 
year,
month,
agent,
fcr,
LAG(fcr, 12) OVER (PARTITION BY agent ORDER BY year, month) AS fcr_smpy,
ROUND(100.0*(fcr - LAG(fcr, 12) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(fcr, 12) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change_smpy,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY fcr DESC NULLS LAST) AS agent_rank
FROM fcr_with_missing_months_added
)

SELECT
*
FROM agent_fcr_smpy
WHERE agent_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 102

-- 13.3 Top Agents by FCR (First Contact Resolution) + percent change YTD (Year-to-Date)

WITH agent_fcr AS (
SELECT
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
agent,
COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true) AS fcr
FROM calls_data
GROUP BY 1,2,3
)

, agent_fcr_ytd AS (
SELECT
year,
month,
agent,
fcr,
SUM(fcr) OVER (PARTITION BY year, agent ORDER BY year, month) AS fcr_ytd
FROM agent_fcr
)

, agent_fcr_ytd_rank AS (
SELECT 
year,
month,
agent,
fcr_ytd,
LAG(fcr_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month) AS fcr_pytd,
ROUND(100.0*(fcr_ytd - LAG(fcr_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(fcr_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY fcr_ytd DESC NULLS LAST) AS agent_rank
FROM agent_fcr_ytd
) 

SELECT
*
FROM agent_fcr_ytd_rank
WHERE agent_rank <= 5
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 102


/*=========================
 CUSTOMER ANALYSIS
=========================*/

-- 1.1 Answered Calls by Issue
-- percent change vs previous month (MoM, Month-0ver-Month)
-- percent of total for the "current" month

WITH answered_by_reason AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
call_reason,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3
)

SELECT
year,
month,
call_reason,
answered_calls,
LAG(answered_calls, 1) OVER (PARTITION BY call_reason ORDER BY year, month_order) AS answered_mom,
ROUND(100.0*(answered_calls - LAG(answered_calls, 1) OVER (PARTITION BY call_reason ORDER BY year, month_order)) / LAG(answered_calls, 1) OVER (PARTITION BY call_reason ORDER BY year, month_order), 2) AS pct_change_mom,
SUM(answered_calls) OVER (PARTITION BY year, month) AS answered_total,
ROUND(100.0*answered_calls/SUM(answered_calls) OVER (PARTITION BY year, month), 2)  AS pct_of_total
FROM answered_by_reason
ORDER BY year, 
		 ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month),
		 call_reason
LIMIT 20

-- 1.2 Answered Calls by Issue
-- percent change vs same month, previous year (SMPY)
-- percent of total for the "current" month

WITH answered_by_reason AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
call_reason,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3
)

SELECT
year,
month,
call_reason,
answered_calls,
LAG(answered_calls, 12) OVER (PARTITION BY call_reason ORDER BY year, month_order) AS answered_smpy,
ROUND(100.0*(answered_calls - LAG(answered_calls, 12) OVER (PARTITION BY call_reason ORDER BY year, month_order)) / LAG(answered_calls, 12) OVER (PARTITION BY call_reason ORDER BY year, month_order), 2) AS pct_change_smpy,
SUM(answered_calls) OVER (PARTITION BY year, month) AS answered_total,
ROUND(100.0*answered_calls/SUM(answered_calls) OVER (PARTITION BY year, month), 2)  AS pct_of_total
FROM answered_by_reason
ORDER BY year, 
		 ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month),
		 call_reason
LIMIT 20
OFFSET 100

-- 1.3 Answered Calls by Issue (percent change Year-to-Date)

WITH answered_by_reason AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
call_reason,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3
)

, answered_by_reason_ytd AS (
SELECT
year,
month,
month_order,
call_reason,
answered_calls,
SUM(answered_calls) OVER (PARTITION BY year, call_reason ORDER BY year, month_order) AS answered_ytd
FROM answered_by_reason
GROUP BY year, month, month_order, call_reason, answered_calls
)

SELECT
year, 
month, 
call_reason,
answered_ytd,
LAG(answered_ytd, 12) OVER (PARTITION BY call_reason ORDER BY year, month_order) AS answered_ytd_prev_year,
ROUND(100.0*(answered_ytd - LAG(answered_ytd, 12) OVER (PARTITION BY call_reason ORDER BY year, month_order)) / LAG(answered_ytd, 12) OVER (PARTITION BY call_reason ORDER BY year, month_order), 1) AS pct_change_ytd,
SUM(answered_ytd) OVER (PARTITION BY year, month) AS answered_total,
ROUND(100.0*answered_ytd/SUM(answered_ytd) OVER (PARTITION BY year, month), 2)  AS pct_of_total
FROM answered_by_reason_ytd
ORDER BY year, 
		 ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month),
		 call_reason
LIMIT 20
OFFSET 100


-- 2.1 Answered Calls by Issue and by Customer
-- percent change vs previous month (MoM, Month-over-Month)
-- percent of total for the "current" month

WITH answered_by_reason_and_customer AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
call_reason,
customer,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3,4
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer, 
call_reason
FROM date_and_customers_and_reasons
)

, answered_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
month_order,
t1.customer,
t1.call_reason,
answered_calls
FROM all_months t1
LEFT JOIN answered_by_reason_and_customer t2 ON t1.year = t2.year 
					  AND t1.month = t2.month_order
					  AND t1.customer = t2.customer
					  AND t1.call_reason=t2.call_reason
)

SELECT
year,
month,
customer,
call_reason,
answered_calls,
LAG(answered_calls, 1) OVER (PARTITION BY customer, call_reason ORDER BY year, month) AS answered_mom,
ROUND(100.0*(answered_calls - LAG(answered_calls, 1) OVER (PARTITION BY customer, call_reason ORDER BY year, month)) / NULLIF(LAG(answered_calls, 1) OVER (PARTITION BY customer, call_reason ORDER BY year, month), 0), 2) AS pct_change_mom,
SUM(answered_calls) OVER (PARTITION BY year, month, customer) AS answered_total,
ROUND(100.0*answered_calls/SUM(answered_calls) OVER (PARTITION BY year, month, customer), 2)  AS pct_of_total
FROM answered_with_missing_months_added
ORDER BY year, 
		 month,
		 customer,
		 call_reason
LIMIT 20
OFFSET 100

-- 2.2 Answered Calls by Issue and by Customer
-- percent change vs same month, previous year (SMPY)
-- percent of total for the "current" month

WITH answered_by_reason_and_customer AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
call_reason,
customer,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3,4
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer, 
call_reason
FROM date_and_customers_and_reasons
)

, answered_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
month_order,
t1.customer,
t1.call_reason,
answered_calls
FROM all_months t1
LEFT JOIN answered_by_reason_and_customer t2 ON t1.year = t2.year 
					  AND t1.month = t2.month_order
					  AND t1.customer = t2.customer
					  AND t1.call_reason=t2.call_reason
)

SELECT
year,
month,
customer,
call_reason,
answered_calls,
LAG(answered_calls, 12) OVER (PARTITION BY customer, call_reason ORDER BY year, month) AS answered_smpy,
ROUND(100.0*(answered_calls - LAG(answered_calls, 12) OVER (PARTITION BY customer, call_reason ORDER BY year, month)) / NULLIF(LAG(answered_calls, 12) OVER (PARTITION BY customer, call_reason ORDER BY year, month), 0), 2) AS pct_change_smpy,
SUM(answered_calls) OVER (PARTITION BY year, month, customer) AS answered_total,
ROUND(100.0*answered_calls/SUM(answered_calls) OVER (PARTITION BY year, month, customer), 2)  AS pct_of_total
FROM answered_with_missing_months_added
ORDER BY year, 
		 month,
		 customer,
		 call_reason
LIMIT 20
OFFSET 1000

-- 2.3 Answered Calls by Issue and by Customer (percent change Year-to-Date)

WITH answered_by_reason_and_customer AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
call_reason,
customer,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3,4
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer, 
call_reason
FROM date_and_customers_and_reasons
)

, answered_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
month_order,
t1.customer,
t1.call_reason,
answered_calls
FROM all_months t1
LEFT JOIN answered_by_reason_and_customer t2 ON t1.year = t2.year 
					  AND t1.month = t2.month_order
					  AND t1.customer = t2.customer
					  AND t1.call_reason=t2.call_reason
)

, answered_by_reason_and_customer_ytd AS (
SELECT
year,
month,
customer,
call_reason,
answered_calls,
SUM(answered_calls) OVER (PARTITION BY year, customer, call_reason ORDER BY year, month) AS answered_calls_ytd
FROM answered_with_missing_months_added
)

SELECT
year,
month,
customer,
call_reason,
answered_calls_ytd,
LAG(answered_calls_ytd, 12) OVER (PARTITION BY customer, call_reason ORDER BY year, month) AS answered_calls_pytd,
ROUND(100.0*(answered_calls_ytd - LAG(answered_calls_ytd, 12) OVER (PARTITION BY customer, call_reason ORDER BY year, month)) / NULLIF(LAG(answered_calls_ytd, 12) OVER (PARTITION BY customer, call_reason ORDER BY year, month), 0), 1) AS pct_change,
SUM(answered_calls_ytd) OVER (PARTITION BY year, month, customer) AS answered_ytd_total,
ROUND(100.0*answered_calls_ytd/SUM(answered_calls_ytd) OVER (PARTITION BY year, month, customer), 2)  AS pct_of_total
FROM answered_by_reason_and_customer_ytd
ORDER BY year, 
		 month,
		 customer,
		 call_reason
LIMIT 20
OFFSET 1000


-- 3.1 Call Duration
-- percent change vs previous month (MoM, Month-over-Month)
-- percent of total for the "current" month

WITH answered_by_duration AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
CASE WHEN talk_time <= 180 THEN '<3'
WHEN talk_time BETWEEN 181 AND 300 THEN '3-5'
WHEN talk_time BETWEEN 301 AND 600 THEN '5-10'
WHEN talk_time BETWEEN 601 AND 900 THEN '10-15'
WHEN talk_time BETWEEN 901 AND 1200 THEN '15-20'
WHEN talk_time BETWEEN 1201 AND 1500 THEN '20-25'
WHEN talk_time BETWEEN 1501 AND 1800 THEN '25-30'
WHEN talk_time >= 1801 THEN '>30' END AS call_duration,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3
)

SELECT
year, 
month, 
call_duration,
answered_calls,
LAG(answered_calls, 1) OVER (PARTITION BY call_duration ORDER BY year, month_order) AS answered_mom,
ROUND(100.0*(answered_calls - LAG(answered_calls, 1) OVER (PARTITION BY call_duration ORDER BY year, month_order)) / LAG(answered_calls, 1) OVER (PARTITION BY call_duration ORDER BY year, month_order), 1) AS pct_change_mom
FROM answered_by_duration
ORDER BY year, 
		 ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month),
		 ARRAY_POSITION(ARRAY['<3', '3-5', '5-10', '10-15', '15-20', '20-25', '25-30', '>30'], call_duration)
LIMIT 20

-- 3.2 Call Duration
-- percent change vs same month, previous year (SMPY)
-- percent of total for the "current" month

WITH answered_by_duration AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
CASE WHEN talk_time <= 180 THEN '<3'
WHEN talk_time BETWEEN 181 AND 300 THEN '3-5'
WHEN talk_time BETWEEN 301 AND 600 THEN '5-10'
WHEN talk_time BETWEEN 601 AND 900 THEN '10-15'
WHEN talk_time BETWEEN 901 AND 1200 THEN '15-20'
WHEN talk_time BETWEEN 1201 AND 1500 THEN '20-25'
WHEN talk_time BETWEEN 1501 AND 1800 THEN '25-30'
WHEN talk_time >= 1801 THEN '>30' END AS call_duration,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3
)

SELECT
year, 
month, 
call_duration,
answered_calls,
LAG(answered_calls, 12) OVER (PARTITION BY call_duration ORDER BY year, month_order) AS answered_smpy,
ROUND(100.0*(answered_calls - LAG(answered_calls, 12) OVER (PARTITION BY call_duration ORDER BY year, month_order)) / LAG(answered_calls, 12) OVER (PARTITION BY call_duration ORDER BY year, month_order), 1) AS pct_change_smpy
FROM answered_by_duration
ORDER BY year, 
		 ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month),
		 ARRAY_POSITION(ARRAY['<3', '3-5', '5-10', '10-15', '15-20', '20-25', '25-30', '>30'], call_duration)
LIMIT 20
OFFSET 100

-- 3.3 Call Duration (percent change Year-to-Date)

WITH answered_by_duration AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
CASE WHEN talk_time <= 180 THEN '<3'
WHEN talk_time BETWEEN 181 AND 300 THEN '3-5'
WHEN talk_time BETWEEN 301 AND 600 THEN '5-10'
WHEN talk_time BETWEEN 601 AND 900 THEN '10-15'
WHEN talk_time BETWEEN 901 AND 1200 THEN '15-20'
WHEN talk_time BETWEEN 1201 AND 1500 THEN '20-25'
WHEN talk_time BETWEEN 1501 AND 1800 THEN '25-30'
WHEN talk_time >= 1801 THEN '>30' END AS call_duration,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3
)

, answered_by_duration_ytd AS (
SELECT
year,
month,
month_order,
call_duration,
answered_calls,
SUM(answered_calls) OVER (PARTITION BY year, call_duration ORDER BY year, month_order) AS answered_ytd
FROM answered_by_duration
GROUP BY year, month, month_order, call_duration, answered_calls
)

SELECT
year, 
month, 
call_duration,
answered_ytd,
LAG(answered_ytd, 12) OVER (PARTITION BY call_duration ORDER BY year, month_order) AS answered_ytd_prev_year,
ROUND(100.0*(answered_ytd - LAG(answered_ytd, 12) OVER (PARTITION BY call_duration ORDER BY year, month_order)) / LAG(answered_ytd, 12) OVER (PARTITION BY call_duration ORDER BY year, month_order), 1) AS pct_change_ytd
FROM answered_by_duration_ytd
ORDER BY year, 
		 ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month),
		 ARRAY_POSITION(ARRAY['<3', '3-5', '5-10', '10-15', '15-20', '20-25', '25-30', '>30'], call_duration)
LIMIT 20
OFFSET 100

-- 4.1 Call Duration by Customer
-- percent change vs previous month (MoM, Month-over-Month)
-- percent of total for the "current" month

WITH answered_by_duration_and_customer AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
customer,
CASE WHEN talk_time <= 180 THEN '<3'
WHEN talk_time BETWEEN 181 AND 300 THEN '3-5'
WHEN talk_time BETWEEN 301 AND 600 THEN '5-10'
WHEN talk_time BETWEEN 601 AND 900 THEN '10-15'
WHEN talk_time BETWEEN 901 AND 1200 THEN '15-20'
WHEN talk_time BETWEEN 1201 AND 1500 THEN '20-25'
WHEN talk_time BETWEEN 1501 AND 1800 THEN '25-30'
WHEN talk_time >= 1801 THEN '>30' END AS call_duration,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3,4
)

, call_durations_only AS (
SELECT
DISTINCT call_duration
FROM answered_by_duration_and_customer
)

, date_and_customers_and_durations AS (
SELECT * FROM customers_only
CROSS JOIN date_scaffolding
CROSS JOIN call_durations_only
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer, 
call_duration
FROM date_and_customers_and_durations
)

, answered_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
month_order,
t1.customer,
t1.call_duration,
answered_calls
FROM all_months t1
LEFT JOIN answered_by_duration_and_customer t2 ON t1.year = t2.year 
					  AND t1.month = t2.month_order
					  AND t1.customer = t2.customer
					  AND t1.call_duration=t2.call_duration
)

SELECT
year, 
month, 
customer,
call_duration,
answered_calls,
LAG(answered_calls, 1) OVER (PARTITION BY customer, call_duration ORDER BY year, month) AS answered_mom,
ROUND(100.0*(answered_calls - LAG(answered_calls, 1) OVER (PARTITION BY customer, call_duration ORDER BY year, month)) / LAG(answered_calls, 1) OVER (PARTITION BY customer, call_duration ORDER BY year, month), 1) AS pct_change_mom
FROM answered_with_missing_months_added
ORDER BY year, 
		 month,
		 customer,
		 ARRAY_POSITION(ARRAY['<3', '3-5', '5-10', '10-15', '15-20', '20-25', '25-30', '>30'], call_duration)
LIMIT 20
OFFSET 100

-- 4.2 Call Duration by Customer
-- percent change vs same month, previous year (SMPY)
-- percent of total for the "current" month

WITH answered_by_duration_and_customer AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
customer,
CASE WHEN talk_time <= 180 THEN '<3'
WHEN talk_time BETWEEN 181 AND 300 THEN '3-5'
WHEN talk_time BETWEEN 301 AND 600 THEN '5-10'
WHEN talk_time BETWEEN 601 AND 900 THEN '10-15'
WHEN talk_time BETWEEN 901 AND 1200 THEN '15-20'
WHEN talk_time BETWEEN 1201 AND 1500 THEN '20-25'
WHEN talk_time BETWEEN 1501 AND 1800 THEN '25-30'
WHEN talk_time >= 1801 THEN '>30' END AS call_duration,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3,4
)

, call_durations_only AS (
SELECT
DISTINCT call_duration
FROM answered_by_duration_and_customer
)

, date_and_customers_and_durations AS (
SELECT * FROM customers_only
CROSS JOIN date_scaffolding
CROSS JOIN call_durations_only
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer, 
call_duration
FROM date_and_customers_and_durations
)

, answered_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
month_order,
t1.customer,
t1.call_duration,
answered_calls
FROM all_months t1
LEFT JOIN answered_by_duration_and_customer t2 ON t1.year = t2.year 
					  AND t1.month = t2.month_order
					  AND t1.customer = t2.customer
					  AND t1.call_duration=t2.call_duration
)

SELECT
year, 
month, 
customer,
call_duration,
answered_calls,
LAG(answered_calls, 12) OVER (PARTITION BY customer, call_duration ORDER BY year, month) AS answered_smpy,
ROUND(100.0*(answered_calls - LAG(answered_calls, 12) OVER (PARTITION BY customer, call_duration ORDER BY year, month)) / LAG(answered_calls, 12) OVER (PARTITION BY customer, call_duration ORDER BY year, month), 1) AS pct_change_smpy
FROM answered_with_missing_months_added
ORDER BY year, 
		 month,
		 customer,
		 ARRAY_POSITION(ARRAY['<3', '3-5', '5-10', '10-15', '15-20', '20-25', '25-30', '>30'], call_duration)
LIMIT 20
OFFSET 1500

-- 4.3 Call Duration by Customer (percent change Year-to-Date)

WITH answered_by_duration_and_customer AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
customer,
CASE WHEN talk_time <= 180 THEN '<3'
WHEN talk_time BETWEEN 181 AND 300 THEN '3-5'
WHEN talk_time BETWEEN 301 AND 600 THEN '5-10'
WHEN talk_time BETWEEN 601 AND 900 THEN '10-15'
WHEN talk_time BETWEEN 901 AND 1200 THEN '15-20'
WHEN talk_time BETWEEN 1201 AND 1500 THEN '20-25'
WHEN talk_time BETWEEN 1501 AND 1800 THEN '25-30'
WHEN talk_time >= 1801 THEN '>30' END AS call_duration,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3,4
)

, call_durations_only AS (
SELECT
DISTINCT call_duration
FROM answered_by_duration_and_customer
)

, date_and_customers_and_durations AS (
SELECT * FROM customers_only
CROSS JOIN date_scaffolding
CROSS JOIN call_durations_only
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer, 
call_duration
FROM date_and_customers_and_durations
)

, answered_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
month_order,
t1.customer,
t1.call_duration,
answered_calls
FROM all_months t1
LEFT JOIN answered_by_duration_and_customer t2 ON t1.year = t2.year 
					  AND t1.month = t2.month_order
					  AND t1.customer = t2.customer
					  AND t1.call_duration=t2.call_duration
)

, answered_by_duration_and_customer_ytd AS (
SELECT
year,
month,
customer,
call_duration,
answered_calls,
SUM(answered_calls) OVER (PARTITION BY year, customer, call_duration ORDER BY year, month) AS answered_calls_ytd
FROM answered_with_missing_months_added
)

SELECT
year, 
month, 
customer,
call_duration,
answered_calls_ytd,
LAG(answered_calls_ytd, 12) OVER (PARTITION BY customer, call_duration ORDER BY year, month) AS answered_pytd,
ROUND(100.0*(answered_calls_ytd - LAG(answered_calls_ytd, 12) OVER (PARTITION BY customer, call_duration ORDER BY year, month)) / LAG(answered_calls_ytd, 12) OVER (PARTITION BY customer, call_duration ORDER BY year, month), 1) AS pct_change_ytd
FROM answered_by_duration_and_customer_ytd
ORDER BY year, 
		 month,
		 customer,
		 ARRAY_POSITION(ARRAY['<3', '3-5', '5-10', '10-15', '15-20', '20-25', '25-30', '>30'], call_duration)
LIMIT 20
OFFSET 1500

-- 5.1 Customers by TIC (Total Incoming Calls) + percent change MoM (Month-over-Month)

WITH customer_tic AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
COUNT(call_id) AS tic
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, tic_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
tic
FROM all_months t1
LEFT JOIN customer_tic t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_tic_mom AS (
SELECT 
year,
month,
customer,
tic,
LAG(tic, 1) OVER (PARTITION BY customer ORDER BY year, month) AS tic_pm,
ROUND(100.0*(tic - LAG(tic, 1) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(tic, 1) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change_mom,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY tic DESC) AS customer_rank
FROM tic_with_missing_months_added
)

SELECT
*
FROM customer_tic_mom
ORDER BY 1,2,4 DESC
LIMIT 20

-- 5.2 Customers by TIC (Total Incoming Calls) + percent change SMPY (same month, previous year)

WITH customer_tic AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
COUNT(call_id) AS tic
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, tic_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
tic
FROM all_months t1
LEFT JOIN customer_tic t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_tic_smpy AS (
SELECT 
year,
month,
customer,
tic,
LAG(tic, 12) OVER (PARTITION BY customer ORDER BY year, month) AS tic_smpy,
ROUND(100.0*(tic - LAG(tic, 12) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(tic, 12) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change_smpy,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY tic DESC) AS customer_rank
FROM tic_with_missing_months_added
)

SELECT
*
FROM customer_tic_smpy
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 200

-- 5.3 Customers by TIC (Total Incoming Calls) + percent change YTD (Year-to-Date)

WITH customer_tic AS (
SELECT
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
customer,
COUNT(call_id) AS tic
FROM calls_data
GROUP BY 1,2,3
)

, customer_tic_ytd AS (
SELECT
year,
month,
customer,
tic,
SUM(tic) OVER (PARTITION BY year, customer ORDER BY year, month) AS tic_ytd
FROM customer_tic
)

, customer_tic_ytd_rank AS (
SELECT 
year,
month,
customer,
tic_ytd,
LAG(tic_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month) AS tic_pytd,
ROUND(100.0*(tic_ytd - LAG(tic_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(tic_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY tic_ytd DESC) AS customer_rank
FROM customer_tic_ytd
) 

SELECT
*
FROM customer_tic_ytd_rank
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 200


-- 6.1 Customers by CSAT
-- percent change vs previous month (MoM, Month-Over-Month)

WITH customer_csat AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(AVG(customer_satisfaction), 2) AS csat
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, csat_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
csat
FROM all_months t1
LEFT JOIN customer_csat t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_csat_mom AS (
SELECT 
year,
month,
customer,
csat,
LAG(csat, 1) OVER (PARTITION BY customer ORDER BY year, month) AS csat_pm,
ROUND(100.0*(csat - LAG(csat, 1) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(csat, 1) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change_mom,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY csat DESC NULLS LAST) AS customer_rank
FROM csat_with_missing_months_added
)

SELECT
*
FROM customer_csat_mom
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 20

-- 6.2 Customers by CSAT
-- percent change vs same month, previous year (SMPY)

WITH customer_csat AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(AVG(customer_satisfaction), 2) AS csat
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, csat_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
csat
FROM all_months t1
LEFT JOIN customer_csat t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_csat_smpy AS (
SELECT 
year,
month,
customer,
csat,
LAG(csat, 12) OVER (PARTITION BY customer ORDER BY year, month) AS csat_smpy,
ROUND(100.0*(csat - LAG(csat, 12) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(csat, 12) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change_smpy,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY csat DESC NULLS LAST) AS customer_rank
FROM csat_with_missing_months_added
)

SELECT
*
FROM customer_csat_smpy
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 20
OFFSET 200

-- 6.3 Customers by CSAT (percent change Year-to-Date)

WITH customer_csat AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(AVG(customer_satisfaction), 2) AS avg_csat
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, csat_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
avg_csat
FROM all_months t1
LEFT JOIN customer_csat t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_csat_ytd AS (
SELECT
year,
month,
customer,
avg_csat,
ROUND(AVG(avg_csat) OVER (PARTITION BY year, customer ORDER BY year, month), 2) AS avg_csat_ytd
FROM csat_with_missing_months_added
)

, customer_csat_ytd_rank AS (
SELECT 
year,
month,
customer,
avg_csat_ytd,
LAG(avg_csat_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month) AS avg_csat_pytd,
ROUND(100.0*(avg_csat_ytd - LAG(avg_csat_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month)) / LAG(avg_csat_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month), 1) AS pct_change,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY avg_csat_ytd DESC NULLS LAST) AS customer_rank
FROM customer_csat_ytd
) 

SELECT
*
FROM customer_csat_ytd_rank
ORDER BY 1,2,4 DESC
LIMIT 20
OFFSET 200


-- 7.1 Customers by FCR % + percent change MoM (Month-over-Month)

WITH customer_fcr_pct AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(100.0*COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true)/COUNT(call_id), 1) AS fcr_pct
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, fcr_pct_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
fcr_pct
FROM all_months t1
LEFT JOIN customer_fcr_pct t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_fcr_pct_mom AS (
SELECT 
year,
month,
customer,
fcr_pct,
LAG(fcr_pct, 1) OVER (PARTITION BY customer ORDER BY year, month) AS fcr_pct_pm,
ROUND(100.0*(fcr_pct - LAG(fcr_pct, 1) OVER (PARTITION BY customer ORDER BY year, month)) / NULLIF(LAG(fcr_pct, 1) OVER (PARTITION BY customer ORDER BY year, month), 0), 1) AS pct_change_mom,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY fcr_pct DESC NULLS LAST) AS customer_rank
FROM fcr_pct_with_missing_months_added
)

SELECT
*
FROM customer_fcr_pct_mom
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 20

-- 7.2 Customers by FCR % + percent change SMPY (same month, previous year)

WITH customer_fcr_pct AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(100.0*COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true)/COUNT(call_id), 1) AS fcr_pct
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, fcr_pct_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
fcr_pct
FROM all_months t1
LEFT JOIN customer_fcr_pct t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_fcr_pct_smpy AS (
SELECT 
year,
month,
customer,
fcr_pct,
LAG(fcr_pct, 12) OVER (PARTITION BY customer ORDER BY year, month) AS fcr_pct_smpy,
ROUND(100.0*(fcr_pct - LAG(fcr_pct, 12) OVER (PARTITION BY customer ORDER BY year, month)) / NULLIF(LAG(fcr_pct, 12) OVER (PARTITION BY customer ORDER BY year, month), 0), 1) AS pct_change_smpy,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY fcr_pct DESC NULLS LAST) AS customer_rank
FROM fcr_pct_with_missing_months_added
)

SELECT
*
FROM customer_fcr_pct_smpy
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 20
OFFSET 200

-- 7.3 Customers by FCR % + percent change YTD (Year-to-Date)

WITH customer_fcr_pct AS (
SELECT
customer,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(100.0*COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true)/COUNT(call_id), 1) AS fcr_pct
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
customer
FROM date_and_customers
)

, fcr_pct_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.customer,
fcr_pct
FROM all_months t1
LEFT JOIN customer_fcr_pct t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.customer = t2.customer
)

, customer_fcr_pct_ytd AS (
SELECT
year,
month,
customer,
fcr_pct,
ROUND(AVG(fcr_pct) OVER (PARTITION BY year, customer ORDER BY year, month), 2) AS fcr_pct_ytd
FROM fcr_pct_with_missing_months_added
)

, customer_fcr_pct_ytd_rank AS (
SELECT 
year,
month,
customer,
fcr_pct_ytd,
LAG(fcr_pct_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month) AS fcr_pct_pytd,
ROUND(100.0*(fcr_pct_ytd - LAG(fcr_pct_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month)) / NULLIF(LAG(fcr_pct_ytd, 12) OVER (PARTITION BY customer ORDER BY year, month), 0), 1) AS pct_change,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY fcr_pct_ytd DESC NULLS LAST) AS customer_rank
FROM customer_fcr_pct_ytd
) 

SELECT
*
FROM customer_fcr_pct_ytd_rank
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 20
OFFSET 200


/*=========================
 AGENT ANALYSIS
=========================*/

-- 1.1 Agents by Answered Calls
-- percent change vs previous month (MoM, Month-over-Month) + agent rank

WITH agent_answered AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, answered_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
answered_calls
FROM all_months t1
LEFT JOIN agent_answered t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_answered_mom AS (
SELECT 
year,
month,
agent,
answered_calls,
LAG(answered_calls, 1) OVER (PARTITION BY agent ORDER BY year, month) AS answered_calls_pm,
ROUND(100.0*(answered_calls - LAG(answered_calls, 1) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(answered_calls, 1) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change_mom,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY answered_calls DESC NULLS LAST) AS agent_rank
FROM answered_with_missing_months_added
)

SELECT
*
FROM agent_answered_mom
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 10
OFFSET 200

-- 1.2 Agents by Answered Calls
-- percent change vs same month, previous year (SMPY) + agent rank

WITH agent_answered AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, answered_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
answered_calls
FROM all_months t1
LEFT JOIN agent_answered t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_answered_smpy AS (
SELECT 
year,
month,
agent,
answered_calls,
LAG(answered_calls, 12) OVER (PARTITION BY agent ORDER BY year, month) AS answered_calls_smpy,
ROUND(100.0*(answered_calls - LAG(answered_calls, 12) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(answered_calls, 12) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change_smpy,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY answered_calls DESC NULLS LAST) AS agent_rank
FROM answered_with_missing_months_added
)

SELECT
*
FROM agent_answered_smpy
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 10
OFFSET 400

-- 1.3 Agents by Answered Calls (percent change Year-to-Date) + agent rank

WITH agent_answered AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, answered_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
answered_calls
FROM all_months t1
LEFT JOIN agent_answered t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_answered_ytd AS (
SELECT
year,
month,
agent,
answered_calls,
SUM(answered_calls) OVER (PARTITION BY year, agent ORDER BY year, month) AS answered_calls_ytd
FROM answered_with_missing_months_added
)

, agent_answered_ytd_rank AS (
SELECT 
year,
month,
agent,
answered_calls_ytd,
LAG(answered_calls_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month) AS answered_calls_pytd,
ROUND(100.0*(answered_calls_ytd - LAG(answered_calls_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(answered_calls_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY answered_calls_ytd DESC NULLS LAST) AS agent_rank
FROM agent_answered_ytd
) 

SELECT
*
FROM agent_answered_ytd_rank
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 10
OFFSET 400


-- 2.1 Agents by FCR %
-- percent change vs previous month (MoM, Month-over-Month) + agent rank

WITH agent_fcr_pct AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(100.0*COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true)/COUNT(call_id), 1) AS fcr_pct
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, fcr_pct_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
fcr_pct
FROM all_months t1
LEFT JOIN agent_fcr_pct t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_fcr_pct_mom AS (
SELECT 
year,
month,
agent,
fcr_pct,
LAG(fcr_pct, 1) OVER (PARTITION BY agent ORDER BY year, month) AS fcr_pct_pm,
ROUND(100.0*(fcr_pct - LAG(fcr_pct, 1) OVER (PARTITION BY agent ORDER BY year, month)) / NULLIF(LAG(fcr_pct, 1) OVER (PARTITION BY agent ORDER BY year, month), 0), 1) AS pct_change_mom,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY fcr_pct DESC NULLS LAST) AS agent_rank
FROM fcr_pct_with_missing_months_added
)

SELECT
*
FROM agent_fcr_pct_mom
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 10
OFFSET 400

-- 2.2 Agents by FCR %
-- percent change vs same month, previous year (SMPY) + agent rank

WITH agent_fcr_pct AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(100.0*COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true)/COUNT(call_id), 1) AS fcr_pct
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, fcr_pct_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
fcr_pct
FROM all_months t1
LEFT JOIN agent_fcr_pct t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_fcr_pct_smpy AS (
SELECT 
year,
month,
agent,
fcr_pct,
LAG(fcr_pct, 12) OVER (PARTITION BY agent ORDER BY year, month) AS fcr_pct_smpy,
ROUND(100.0*(fcr_pct - LAG(fcr_pct, 12) OVER (PARTITION BY agent ORDER BY year, month)) / NULLIF(LAG(fcr_pct, 12) OVER (PARTITION BY agent ORDER BY year, month), 0), 1) AS pct_change_smpy,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY fcr_pct DESC NULLS LAST) AS agent_rank
FROM fcr_pct_with_missing_months_added
)

SELECT
*
FROM agent_fcr_pct_smpy
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 10
OFFSET 400

-- 2.3 Agents by FCR % (percent change Year-to-Date) + agent rank

WITH agent_fcr_pct AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(100.0*COUNT(call_id) FILTER (WHERE first_contact_resolution_flag = true)/COUNT(call_id), 1) AS fcr_pct
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, fcr_pct_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
fcr_pct
FROM all_months t1
LEFT JOIN agent_fcr_pct t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_fcr_pct_ytd AS (
SELECT
year,
month,
agent,
fcr_pct,
ROUND(AVG(fcr_pct) OVER (PARTITION BY year, agent ORDER BY year, month), 2) AS fcr_pct_ytd
FROM fcr_pct_with_missing_months_added
)

, agent_fcr_pct_ytd_rank AS (
SELECT 
year,
month,
agent,
fcr_pct_ytd,
LAG(fcr_pct_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month) AS fcr_pct_pytd,
ROUND(100.0*(fcr_pct_ytd - LAG(fcr_pct_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month)) / NULLIF(LAG(fcr_pct_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month), 0), 1) AS pct_change,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY fcr_pct_ytd DESC NULLS LAST) AS agent_rank
FROM agent_fcr_pct_ytd
) 

SELECT
*
FROM agent_fcr_pct_ytd_rank
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 10
OFFSET 400


-- 3.1 Agents by CSAT
-- percent change vs previous month (MoM, Month-over-Month) + agent rank

WITH agent_csat AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(AVG(customer_satisfaction), 2) AS csat
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, csat_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
csat
FROM all_months t1
LEFT JOIN agent_csat t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_csat_mom AS (
SELECT 
year,
month,
agent,
csat,
LAG(csat, 1) OVER (PARTITION BY agent ORDER BY year, month) AS csat_pm,
ROUND(100.0*(csat - LAG(csat, 1) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(csat, 1) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change_mom,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY csat DESC NULLS LAST) AS agent_rank
FROM csat_with_missing_months_added
)

SELECT
*
FROM agent_csat_mom
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 10
OFFSET 400

-- 3.2 Agents by CSAT
-- percent change vs same month, previous year (SMPY) + agent rank

WITH agent_csat AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(AVG(customer_satisfaction), 2) AS csat
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, csat_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
csat
FROM all_months t1
LEFT JOIN agent_csat t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_csat_smpy AS (
SELECT 
year,
month,
agent,
csat,
LAG(csat, 12) OVER (PARTITION BY agent ORDER BY year, month) AS csat_smpy,
ROUND(100.0*(csat - LAG(csat, 12) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(csat, 12) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change_smpy,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY csat DESC NULLS LAST) AS agent_rank
FROM csat_with_missing_months_added
)

SELECT
*
FROM agent_csat_smpy
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 10
OFFSET 400

-- 3.3 Agents by CSAT (percent change Year-to-Date) + agent rank

WITH agent_csat AS (
SELECT
agent,
DATE_PART('year', call_datetime) AS year,
DATE_PART('month', call_datetime) AS month,
ROUND(AVG(customer_satisfaction), 2) AS avg_csat
FROM calls_data
GROUP BY 1,2,3
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent
FROM date_and_agents
)

, csat_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
t1.agent,
avg_csat
FROM all_months t1
LEFT JOIN agent_csat t2 ON t1.year = t2.year 
					  AND t1.month = t2.month
					  AND t1.agent = t2.agent
)

, agent_csat_ytd AS (
SELECT
year,
month,
agent,
avg_csat,
ROUND(AVG(avg_csat) OVER (PARTITION BY year, agent ORDER BY year, month), 2) AS avg_csat_ytd
FROM csat_with_missing_months_added
)

, agent_csat_ytd_rank AS (
SELECT 
year,
month,
agent,
avg_csat_ytd,
LAG(avg_csat_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month) AS avg_csat_pytd,
ROUND(100.0*(avg_csat_ytd - LAG(avg_csat_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month)) / LAG(avg_csat_ytd, 12) OVER (PARTITION BY agent ORDER BY year, month), 1) AS pct_change,
DENSE_RANK() OVER (PARTITION BY year, month ORDER BY avg_csat_ytd DESC NULLS LAST) AS agent_rank
FROM agent_csat_ytd
) 

SELECT
*
FROM agent_csat_ytd_rank
ORDER BY 1,2,4 DESC NULLS LAST
LIMIT 10
OFFSET 400


-- 4.1 Answered Calls by Issue and by Agent
-- percent change vs previous month (MoM, Month-over-Month)
-- percent of total for the "current" month

WITH answered_by_reason_and_agent AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
call_reason,
agent,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3,4
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent, 
call_reason
FROM date_and_agents_and_reasons
)

, answered_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
month_order,
t1.agent,
t1.call_reason,
answered_calls
FROM all_months t1
LEFT JOIN answered_by_reason_and_agent t2 ON t1.year = t2.year 
					  AND t1.month = t2.month_order
					  AND t1.agent = t2.agent
					  AND t1.call_reason=t2.call_reason
)

SELECT
year,
month,
agent,
call_reason,
answered_calls,
LAG(answered_calls, 1) OVER (PARTITION BY agent, call_reason ORDER BY year, month) AS answered_mom,
ROUND(100.0*(answered_calls - LAG(answered_calls, 1) OVER (PARTITION BY agent, call_reason ORDER BY year, month)) / NULLIF(LAG(answered_calls, 1) OVER (PARTITION BY agent, call_reason ORDER BY year, month), 0), 2) AS pct_change_mom,
SUM(answered_calls) OVER (PARTITION BY year, month, agent) AS answered_total,
ROUND(100.0*answered_calls/SUM(answered_calls) OVER (PARTITION BY year, month, agent), 2)  AS pct_of_total
FROM answered_with_missing_months_added
ORDER BY year, 
		 month,
		 agent,
		 call_reason
LIMIT 10
OFFSET 1000

-- 4.2 Answered Calls by Issue and by Agent
-- percent change vs same month, previous year (SMPY)
-- percent of total for the "current" month

WITH answered_by_reason_and_agent AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
call_reason,
agent,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3,4
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent, 
call_reason
FROM date_and_agents_and_reasons
)

, answered_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
month_order,
t1.agent,
t1.call_reason,
answered_calls
FROM all_months t1
LEFT JOIN answered_by_reason_and_agent t2 ON t1.year = t2.year 
					  AND t1.month = t2.month_order
					  AND t1.agent = t2.agent
					  AND t1.call_reason=t2.call_reason
)

SELECT
year,
month,
agent,
call_reason,
answered_calls,
LAG(answered_calls, 12) OVER (PARTITION BY agent, call_reason ORDER BY year, month) AS answered_smpy,
ROUND(100.0*(answered_calls - LAG(answered_calls, 12) OVER (PARTITION BY agent, call_reason ORDER BY year, month)) / NULLIF(LAG(answered_calls, 12) OVER (PARTITION BY agent, call_reason ORDER BY year, month), 0), 2) AS pct_change_smpy,
SUM(answered_calls) OVER (PARTITION BY year, month, agent) AS answered_total,
ROUND(100.0*answered_calls/SUM(answered_calls) OVER (PARTITION BY year, month, agent), 2)  AS pct_of_total
FROM answered_with_missing_months_added
ORDER BY year, 
		 month,
		 agent,
		 call_reason
LIMIT 10
OFFSET 1000

-- 4.3 Answered Calls by Issue and by Agent (percent change Year-to-Date)

WITH answered_by_reason_and_agent AS (
SELECT 
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
call_reason,
agent,
COUNT(call_id) FILTER (WHERE abandon_flag = false) AS answered_calls,
ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], TRIM(TO_CHAR(call_datetime, 'Month'))) AS month_order
FROM calls_data
GROUP BY 1,2,3,4
)

, all_months AS (
SELECT DISTINCT
DATE_PART('year', date_time) AS year,
DATE_PART('month', date_time) AS month,
agent, 
call_reason
FROM date_and_agents_and_reasons
)

, answered_with_missing_months_added AS (
SELECT
t1.year,
t1.month,
month_order,
t1.agent,
t1.call_reason,
answered_calls
FROM all_months t1
LEFT JOIN answered_by_reason_and_agent t2 ON t1.year = t2.year 
					  AND t1.month = t2.month_order
					  AND t1.agent = t2.agent
					  AND t1.call_reason=t2.call_reason
)

, answered_by_reason_and_agent_ytd AS (
SELECT
year,
month,
agent,
call_reason,
answered_calls,
SUM(answered_calls) OVER (PARTITION BY year, agent, call_reason ORDER BY year, month) AS answered_calls_ytd
FROM answered_with_missing_months_added
)

SELECT
year,
month,
agent,
call_reason,
answered_calls_ytd,
LAG(answered_calls_ytd, 12) OVER (PARTITION BY agent, call_reason ORDER BY year, month) AS answered_calls_pytd,
ROUND(100.0*(answered_calls_ytd - LAG(answered_calls_ytd, 12) OVER (PARTITION BY agent, call_reason ORDER BY year, month)) / NULLIF(LAG(answered_calls_ytd, 12) OVER (PARTITION BY agent, call_reason ORDER BY year, month), 0), 1) AS pct_change,
SUM(answered_calls_ytd) OVER (PARTITION BY year, month, agent) AS answered_ytd_total,
ROUND(100.0*answered_calls_ytd/SUM(answered_calls_ytd) OVER (PARTITION BY year, month, agent), 2)  AS pct_of_total
FROM answered_by_reason_and_agent_ytd
ORDER BY year, 
		 month,
		 agent,
		 call_reason
LIMIT 10
OFFSET 3000

-- 5.1 Incoming Calls by Business Day and Hour, by Agent

WITH day_hour_cte AS (
SELECT 
*,
DATE_TRUNC('hour', call_datetime) AS call_datetime_2,
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
TRIM(TO_CHAR(call_datetime, 'Day')) AS day,
DATE_PART('hour', DATE_TRUNC('hour', call_datetime)) AS hour
FROM calls_data
)

, day_hour_cte_2 AS (
SELECT
call_datetime_2,
year,
month,
day,
hour,
agent,
COUNT(*) AS calls
FROM day_hour_cte
GROUP BY 1,2,3,4,5,6
)

, day_hour_cte_3 AS (
SELECT
year, 
month,
day,
hour,
agent,
ROUND(AVG(calls)) As avg_calls
FROM day_hour_cte_2
GROUP BY 1,2,3,4,5
)

SELECT
year,
month,
day,
agent,
MAX(CASE WHEN hour = 7 THEN avg_calls END) AS "7",
MAX(CASE WHEN hour = 8 THEN avg_calls END) AS "8",
MAX(CASE WHEN hour = 9 THEN avg_calls END) AS "9",
MAX(CASE WHEN hour = 10 THEN avg_calls END) AS "10",
MAX(CASE WHEN hour = 11 THEN avg_calls END) AS "11",
MAX(CASE WHEN hour = 12 THEN avg_calls END) AS "12",
MAX(CASE WHEN hour = 13 THEN avg_calls END) AS "13",
MAX(CASE WHEN hour = 14 THEN avg_calls END) AS "14",
MAX(CASE WHEN hour = 15 THEN avg_calls END) AS "15",
MAX(CASE WHEN hour = 16 THEN avg_calls END) AS "16",
MAX(CASE WHEN hour = 17 THEN avg_calls END) AS "17",
MAX(CASE WHEN hour = 18 THEN avg_calls END) AS "18",
MAX(CASE WHEN hour = 19 THEN avg_calls END) AS "19",
MAX(CASE WHEN hour = 20 THEN avg_calls END) AS "20"
FROM day_hour_cte_3
GROUP by year, month, day, agent
ORDER BY year, 
		 ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month),
		 ARRAY_POSITION(ARRAY['Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], day), 
		 agent
LIMIT 10

-- 5.2 Incoming Calls by Business Day and Hour, by Agent (Year-to-Date)

WITH day_hour_cte AS (
SELECT 
*,
DATE_TRUNC('hour', call_datetime) AS call_datetime_2,
DATE_PART('year', call_datetime) AS year,
TRIM(TO_CHAR(call_datetime, 'Month')) AS month,
TRIM(TO_CHAR(call_datetime, 'Day')) AS day,
DATE_PART('hour', DATE_TRUNC('hour', call_datetime)) AS hour
FROM calls_data
)

, day_hour_cte_2 AS (
SELECT
call_datetime_2,
year,
month,
day,
hour,
agent,
COUNT(*) AS calls
FROM day_hour_cte
GROUP BY 1,2,3,4,5,6
)

, day_hour_cte_3 AS (
SELECT
year, 
month,
day,
hour,
agent,
ROUND(AVG(calls)) As avg_calls
FROM day_hour_cte_2
GROUP BY 1,2,3,4,5
)

, day_hour_ytd AS (
SELECT
year, 
month,
day,
hour,
agent,
avg_calls,
ROUND(AVG(avg_calls) OVER (PARTITION BY day, hour, year, agent ORDER BY year, ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month))) AS avg_calls_ytd
FROM day_hour_cte_3
ORDER BY year, 
		 ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month),
		 ARRAY_POSITION(ARRAY['Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], day),
		 hour,
		 agent
)

SELECT
year,
month,
day,
agent,
MAX(CASE WHEN hour = 7 THEN avg_calls_ytd END) AS "7",
MAX(CASE WHEN hour = 8 THEN avg_calls_ytd END) AS "8",
MAX(CASE WHEN hour = 9 THEN avg_calls_ytd END) AS "9",
MAX(CASE WHEN hour = 10 THEN avg_calls_ytd END) AS "10",
MAX(CASE WHEN hour = 11 THEN avg_calls_ytd END) AS "11",
MAX(CASE WHEN hour = 12 THEN avg_calls_ytd END) AS "12",
MAX(CASE WHEN hour = 13 THEN avg_calls_ytd END) AS "13",
MAX(CASE WHEN hour = 14 THEN avg_calls_ytd END) AS "14",
MAX(CASE WHEN hour = 15 THEN avg_calls_ytd END) AS "15",
MAX(CASE WHEN hour = 16 THEN avg_calls_ytd END) AS "16",
MAX(CASE WHEN hour = 17 THEN avg_calls_ytd END) AS "17",
MAX(CASE WHEN hour = 18 THEN avg_calls_ytd END) AS "18",
MAX(CASE WHEN hour = 19 THEN avg_calls_ytd END) AS "19",
MAX(CASE WHEN hour = 20 THEN avg_calls_ytd END) AS "20"
FROM day_hour_ytd
GROUP by year, month, day, agent
ORDER BY year, 
		 ARRAY_POSITION(ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], month),
		 ARRAY_POSITION(ARRAY['Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], day),
		 agent
LIMIT 10

