USE vision_zero;

-- Monthly violations percent change per borough/NYC
SELECT 
    year,
    month,
    borough,
    number_of_violations,
    round((1 - (number_of_violations / lag(number_of_violations) over w)) * -100, 2) as violations_percent_change
from violations_by_month
ORDER BY borough, year, month
WINDOW w as (
    partition by borough
    order by year, month
);