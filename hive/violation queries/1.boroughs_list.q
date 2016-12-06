--Loading a csv file for borough and precincts into table :
USE vision_zero;
DROP TABLE IF EXISTS boroughs_list;

create table boroughs_list (
borough string,
precinct int
) 
row format delimited fields terminated by ',' 
location '/user/ss9711/hiveInput/' 
tblproperties ("skip.header.line.count"="1");