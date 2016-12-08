USE vision_zero;

-- TLC 3/6/12 month running averages
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