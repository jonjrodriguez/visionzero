USE vision_zero_fa16;

-- Totals by month
SELECT
	month(pickup) as month,
    year(pickup) as year,
    count(1) as trip_count,
    ROUND(avg(trip_minutes), 2) as avg_trip_minutes,
    ROUND(min(trip_minutes), 2) as min_trip_minutes,
    ROUND(max(trip_minutes), 2) as max_trip_minutes,
    ROUND(avg(trip_miles), 2) as avg_trip_miles,
    ROUND(min(trip_miles), 2) as min_trip_miles,
    ROUND(max(trip_miles), 2) as max_trip_miles,
    ROUND(avg(trip_mph), 2) as avg_trip_mph,
    ROUND(min(trip_mph), 2) as min_trip_mph,
    ROUND(max(trip_mph), 2) as max_trip_mph,
    ROUND(avg(trip_fare), 2) as avg_trip_fare,
    ROUND(min(trip_fare), 2) as min_trip_fare,
    ROUND(max(trip_fare), 2) as max_trip_fare
from tlc
group by month(pickup), year(pickup);