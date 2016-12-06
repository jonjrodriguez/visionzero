-- Calculating running average for speeding violations for NYC per year per month

use vision_zero;
DROP TABLE IF EXISTS nyc_violations_running_average;

create table nyc_violations_running_average AS
 
SELECT
    year,
	month,
	number_of_violations,
    round(avg(number_of_violations) over w3, 2) as 3mo_violations,
    round(avg(number_of_violations) over w6, 2) as 6mo_violations,
    round(avg(number_of_violations) over w12, 2) as 12mo_violations
from nyc_violations_by_month
group by year, month, number_of_violations
order by year, month
WINDOW
w3 AS (
    ORDER BY year, month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
),
w6 AS (
    ORDER BY year, month
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
),
w12 AS (
    ORDER BY year, month
    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
);