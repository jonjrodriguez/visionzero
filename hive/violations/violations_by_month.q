USE vision_zero;

-- Monthly totals and min/max by borough and all NYC
DROP TABLE IF EXISTS violations_by_month;
CREATE TABLE violations_by_month
    AS
SELECT
    year,
    month,
    borough,
    sum(amount) AS number_of_violations,
    min(amount) AS min_violations,
    max(amount) AS max_violations
FROM violations
JOIN boroughs_list
ON violations.precinct = boroughs_list.precinct
GROUP BY year, month, borough
    UNION ALL
SELECT
    year,
    month,
    'NYC' as borough,
    sum(amount) AS number_of_violations,
    min(amount) AS min_violations,
    max(amount) AS max_violations
FROM violations
GROUP BY year, month;