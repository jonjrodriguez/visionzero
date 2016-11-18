CREATE DATABASE IF NOT EXISTS vision_zero_fa16;
USE vision_zero_fa16;

DROP TABLE IF EXISTS tlc;

create external table tlc (vendor_id INT, pickup TIMESTAMP, dropoff TIMESTAMP, trip_length DOUBLE, trip_distance DOUBLE, trip_mph DOUBLE, fare_amount DOUBLE)
row format delimited fields terminated by ','
location '';