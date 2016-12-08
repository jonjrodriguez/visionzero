USE vision_zero;

DROP TABLE IF EXISTS tlc;
create external table tlc (
    vendor_id STRING,
    pickup TIMESTAMP,
    dropoff TIMESTAMP,
    trip_minutes DECIMAL(10,2),
    trip_miles DECIMAL(10,2),
    trip_mph DECIMAL(10,2),
    trip_fare DECIMAL(10,2)
)
row format delimited fields terminated by ','
location '{base_path}/visionzero/data/formatted/tlc';