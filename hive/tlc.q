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

-- 3 month running averages
DROP TABLE IF EXISTS tlc_3mo_avg;
CREATE TABLE tlc_3mo_avg
    AS
SELECT
    year,
	month,
    round(avg(avg_trip_minutes) over w, 2) as trip_minutes,
    round(avg(avg_trip_miles) over w, 2) as trip_miles,
    round(avg(avg_trip_mph) over w, 2) as trip_mph,
    round(avg(avg_trip_fare) over w, 2) as trip_fare
from tlc_by_month
order by year, month
WINDOW w AS (
    ORDER BY year, month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
);

-- 6 month running averages
DROP TABLE IF EXISTS tlc_6mo_avg;
CREATE TABLE tlc_6mo_avg
    AS
SELECT
    year,
	month,
    round(avg(avg_trip_minutes) over w, 2) as trip_minutes,
    round(avg(avg_trip_miles) over w, 2) as trip_miles,
    round(avg(avg_trip_mph) over w, 2) as trip_mph,
    round(avg(avg_trip_fare) over w, 2) as trip_fare
from tlc_by_month
order by year, month
WINDOW w AS (
    ORDER BY year, month
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
);

-- 12 month running averages
DROP TABLE IF EXISTS tlc_12mo_avg;
CREATE TABLE tlc_12mo_avg
    AS
SELECT
    year,
	month,
    round(avg(avg_trip_minutes) over w, 2) as trip_minutes,
    round(avg(avg_trip_miles) over w, 2) as trip_miles,
    round(avg(avg_trip_mph) over w, 2) as trip_mph,
    round(avg(avg_trip_fare) over w, 2) as trip_fare
from tlc_by_month
order by year, month
WINDOW w AS (
    ORDER BY year, month
    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
);