-- Totals by month
DROP TABLE IF EXISTS vision_zero_fa16.tlc_by_month;

create table vision_zero_fa16.tlc_by_month (
    month int,
    year int,
    trip_count bigint,
    avg_trip_length double,
    min_trip_length double,
    max_trip_length double,
    avg_trip_distance double,
    min_trip_distance double,
    max_trip_distance double,
    avg_trip_mph double,
    min_trip_mph double,
    max_trip_mph double,
    avg_trip_fare double,
    min_trip_fare double,
    max_trip_fare double
)
row format delimited fields terminated by ','
STORED AS TEXTFILE
location '/user/jr187/visionzero/data/analytics/tlc_by_month';

INSERT OVERWRITE TABLE vision_zero_fa16.tlc_by_month
SELECT
	month(pickup) as month,
    year(pickup) as year,
    count(1) as trip_count,
    avg(trip_length) as avg_trip_length,
    min(trip_length) as min_trip_length,
    max(trip_length) as max_trip_length,
    avg(trip_distance) as avg_trip_distance,
    min(trip_distance) as min_trip_distance,
    max(trip_distance) as max_trip_distance,
    avg(trip_mph) as avg_trip_mph,
    min(trip_mph) as min_trip_mph,
    max(trip_mph) as max_trip_mph,
    avg(fare_amount) as avg_trip_fare,
    min(fare_amount) as min_trip_fare,
    max(fare_amount) as max_trip_fare
from vision_zero_fa16.tlc
group by month(pickup), year(pickup);