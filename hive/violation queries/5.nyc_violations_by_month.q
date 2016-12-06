-- Calculating number_of_violations in NYC per year per month

use vision_zero;
DROP TABLE IF EXISTS nyc_violations_by_month;

create table nyc_violations_by_month AS


SELECT year,
        month,
        sum(number_of_violations) AS number_of_violations
FROM violations_per_borough_by_month
GROUP BY  year, month
ORDER BY  year,month;