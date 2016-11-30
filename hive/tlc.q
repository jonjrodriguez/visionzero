USE vision_zero;

-- Monthly averages and min/max
DROP TABLE IF EXISTS tlc_by_month;
CREATE TABLE tlc_by_month
    AS
SELECT
    year(pickup) as year,
	month(pickup) as month,
    count(1) as trip_count,
    round(avg(trip_minutes), 2) as avg_trip_minutes,
    min(trip_minutes) as min_trip_minutes,
    max(trip_minutes) as max_trip_minutes,
    round(avg(trip_miles), 2) as avg_trip_miles,
    min(trip_miles) as min_trip_miles,
    max(trip_miles) as max_trip_miles,
    round(avg(trip_mph), 2) as avg_trip_mph,
    min(trip_mph) as min_trip_mph,
    max(trip_mph) as max_trip_mph,
    round(avg(trip_fare), 2) as avg_trip_fare,
    min(trip_fare) as min_trip_fare,
    max(trip_fare) as max_trip_fare
from tlc
group by year(pickup), month(pickup);

-- TLC 3/6/12 month running averages
DROP TABLE IF EXISTS tlc_running_avg;
CREATE TABLE tlc_running_avg
    AS
SELECT
    year,
	month,
    round(avg(avg_trip_minutes) over w3, 2) as 3mo_trip_minutes,
    round(avg(avg_trip_miles) over w3, 2) as 3mo_trip_miles,
    round(avg(avg_trip_mph) over w3, 2) as 3mo_trip_mph,
    round(avg(avg_trip_fare) over w3, 2) as 3mo_trip_fare,
    round(avg(avg_trip_minutes) over w6, 2) as 6mo_trip_minutes,
    round(avg(avg_trip_miles) over w6, 2) as 6mo_trip_miles,
    round(avg(avg_trip_mph) over w6, 2) as 6mo_trip_mph,
    round(avg(avg_trip_fare) over w6, 2) as 6mo_trip_fare,
    round(avg(avg_trip_minutes) over w12, 2) as 12mo_trip_minutes,
    round(avg(avg_trip_miles) over w12, 2) as 12mo_trip_miles,
    round(avg(avg_trip_mph) over w12, 2) as 12mo_trip_mph,
    round(avg(avg_trip_fare) over w12, 2) as 12mo_trip_fare
from tlc_by_month
order by year, month
WINDOW
w3 AS (
    ORDER BY year, month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
),
w6 AS (
    ORDER BY year, month
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
),
w12 AS (
    ORDER BY year, month
    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
);

-- Monthly percent change
DROP TABLE IF EXISTS tlc_change_by_month;
CREATE TABLE tlc_change_by_month
    AS
SELECT 
    year,
    month,
    avg_trip_minutes,
    round((1 - (avg_trip_minutes / lag(avg_trip_minutes) over w)) * -100, 2) as minutes_percent_change,
    avg_trip_miles,
    round((1 - (avg_trip_miles / lag(avg_trip_miles) over w)) * -100, 2) as miles_percent_change,
    avg_trip_mph,
    round((1 - (avg_trip_mph / lag(avg_trip_mph) over w)) * -100, 2) as mph_percent_change,
    avg_trip_fare,
    round((1 - (avg_trip_fare / lag(avg_trip_fare) over w)) * -100, 2) as fare_percent_change
from tlc_by_month
ORDER BY year, month
WINDOW w as (
    order by year, month
);