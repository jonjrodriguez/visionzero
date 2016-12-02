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

DROP TABLE IF EXISTS collisions;
CREATE EXTERNAL TABLE collisions (
    Borough INT,
    Month INT,
    Year INT,
    Precinct INT,
    CollisionCount INT,
    CollisionInjuredCount INT,
    CollisionKilledCount INT,
    PersonsInjured INT,
    PersonsKilled INT,
    MotoristsInjured INT,
    MotoristsKilled INT,
    PassengersInjured INT,
    PassengersKilled INT,
    CyclistsInjured INT,
    CyclistsKilled INT,
    PedestriansInjured INT,
    PedestriansKilled INT,
    InjuryOrFatal INT,
    Bicycle INT
)
row format delimited fields terminated by ','
location '{base_path}/visionzero/data/formatted/collisions';
