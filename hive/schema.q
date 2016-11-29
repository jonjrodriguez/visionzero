CREATE DATABASE IF NOT EXISTS vision_zero_fa16;
USE vision_zero_fa16;

DROP TABLE IF EXISTS vision_zero_fa16.tlc;
create external table vision_zero_fa16.tlc (vendor_id STRING, pickup TIMESTAMP, dropoff TIMESTAMP, trip_length DOUBLE, trip_distance DOUBLE, trip_mph DOUBLE, fare_amount DOUBLE)
row format delimited fields terminated by ','
location '{base_path}/visionzero/data/formatted/tlc';

DROP TABLE IF EXISTS vision_zero_fa16.violations;
create external table vision_zero_fa16.violations (precinct INT, month INT, year INT, violation STRING, amount FLOAT)
row format delimited fields terminated by ','
location '{base_path}/visionzero/data/formatted/violations';