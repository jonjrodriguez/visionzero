USE vision_zero_fa16;

-- Totals by month
SELECT
	month(pickup) as month,
    year(pickup) as year,
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
group by month(pickup), year(pickup);