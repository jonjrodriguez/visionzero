-- Calculating monthly percent change in the number of violations for nyc by month

use vision_zero;
DROP TABLE IF EXISTS violations_percent_change_nyc;
CREATE TABLE violations_percent_change_nyc
    AS
SELECT 
    year,
    month,
    number_of_violations,
    round((1 - (number_of_violations / lag(number_of_violations) over w)) * -100, 2) as violations_percent_change
from nyc_violations_by_month
ORDER BY year, month
WINDOW w as (
    order by year, month
    );