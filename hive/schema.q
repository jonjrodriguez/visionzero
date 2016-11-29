CREATE DATABASE IF NOT EXISTS vision_zero_fa16;
USE vision_zero_fa16;

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

DROP TABLE IF EXISTS violations;
create external table violations (
    precinct INT,
    month INT,
    year INT,
    violation STRING,
    amount FLOAT
)
row format delimited fields terminated by ','
location '{base_path}/visionzero/data/formatted/violations';