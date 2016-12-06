--violation table precincts combined with borough name
USE vision_zero;

DROP TABLE IF EXISTS violation_per_borough_by_year;

CREATE TABLE violation_per_borough_by_year AS 
SELECT sum(v.amount) AS number_of_violations,
         b.borough,
         v.year
FROM violations v
INNER JOIN boroughs_list b
    ON v.precinct = b.precinct
GROUP BY  year, borough;