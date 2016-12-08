USE vision_zero;

DROP TABLE IF EXISTS boroughs_list;
CREATE EXTERNAL TABLE boroughs_list (
    borough string,
    precinct int
) 
row format delimited fields terminated by ',' 
location '{base_path}/visionzero/data/precincts' 
tblproperties ("skip.header.line.count"="1");

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
