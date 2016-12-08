USE vision_zero;

-- Monthly percent change
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