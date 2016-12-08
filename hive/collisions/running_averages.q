USE vision_zero;

-- Collisions 3/6/12 month running averages for borough/NYC
SELECT
  year,
  month,
  borough,
  collision_count,
  ROUND(AVG(collision_count) OVER W3, 2) AS 3mo_collision_count,
  ROUND(AVG(collision_count) OVER W6, 2) AS 6mo_collision_count,
  ROUND(AVG(collision_count) OVER W12, 2) AS 12mo_collision_count,
  injured_count,
  ROUND(AVG(injured_count) OVER W3, 2) AS 3mo_injured_count,
  ROUND(AVG(injured_count) OVER W6, 2) AS 6mo_injured_count,
  ROUND(AVG(injured_count) OVER W12, 2) AS 12mo_injured_count
  killed_count,
  ROUND(AVG(killed_count) OVER W3, 2) AS 3mo_killed_count,
  ROUND(AVG(killed_count) OVER W6, 2) AS 6mo_killed_count,
  ROUND(AVG(killed_count) OVER W12, 2) AS 12mo_killed_count
FROM collisions_by_month
ORDER BY borough, year, month
WINDOW
W3 AS (
  PARTITION BY borough
  ORDER BY year, month
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
),
W6 AS (
  PARTITION BY borough
  ORDER BY year, month
  ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
),
W12 AS (
  PARTITION BY borough
  ORDER BY year, month
  ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
);
