USE vision_zero;

-- Monthly collisions percent change per borough/NYC
SELECT
  year,
  month,
  borough,
  collision_count,
  ROUND((1 - (collision_count / LAG(collision_count) OVER W)) * -100, 2) AS collision_percent_change
  killed_count,
  ROUND((1 - (killed_count / LAG(killed_count) OVER W)) * -100, 2) AS killed_percent_change
  injured_count,
  ROUND((1 - (injured_count / LAG(injured_count) OVER W)) * -100, 2) AS injured_percent_change
FROM collisions_by_month
ORDER BY borough, year, month
WINDOW W AS (
  PARTITION BY borough
  ORDER BY year, month
);