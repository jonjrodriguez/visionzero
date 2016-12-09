USE vision_zero;

DROP TABLE IF EXISTS collisions;
CREATE EXTERNAL TABLE collisions (
    borough STRING,
    month INT,
    year INT,
    precinct INT,
    collision_count FLOAT,
    collision_injured_count FLOAT,
    collision_killed_count FLOAT,
    motorists_injured FLOAT,
    motorists_killed FLOAT,
    passengers_injured FLOAT,
    passengers_killed FLOAT,
    cyclists_injured FLOAT,
    cyclists_killed FLOAT,
    pedestrians_injured FLOAT,
    pedestrians_killed FLOAT
)
row format delimited fields terminated by ','
location '{base_path}/visionzero/data/formatted/collisions';