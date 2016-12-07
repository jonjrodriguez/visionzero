-- Calculating monthly percent change in the number of violations per borought by month

use vision_zero;
DROP TABLE IF EXISTS violations_percent_change_borough;
CREATE TABLE violations_percent_change_borough
    AS
SELECT 
    year,
    month,
    borough,
    number_of_violations,
    round((1 - (number_of_violations / lag(number_of_violations) over w)) * -100, 2) as violations_percent_change
from violations_per_borough_by_month
ORDER BY year, month
WINDOW w as (
    partition by borough
    order by year, month
    );