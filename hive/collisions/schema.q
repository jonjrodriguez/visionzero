USE vision_zero;

DROP TABLE IF EXISTS collisions;
CREATE EXTERNAL TABLE collisions (
    borough STRING,
    month INT,
    year INT,
    precinct INT,
    collision_count INT,
    collision_injured_count INT,
    collision_killed_count INT,
    motorists_injured INT,
    motorists_killed INT,
    passengers_injured INT,
    passengers_killed INT,
    cyclists_injured INT,
    cyclists_killed INT,
    pedestrians_injured INT,
    pedestrians_killed INT
)
row format delimited fields terminated by ','
location '{base_path}/visionzero/data/formatted/collisions';