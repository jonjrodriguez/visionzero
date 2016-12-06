-- Violation data for each borough according to month
use vision_zero;

DROP TABLE IF EXISTS violations_per_borough_by_month;

 create table violations_per_borough_by_month AS
 SELECT year,
         month,
         borough,
         sum(amount) AS number_of_violations
FROM violations
JOIN boroughs_list
    ON violations.precinct = boroughs_list.precinct
GROUP BY  year, month, borough;