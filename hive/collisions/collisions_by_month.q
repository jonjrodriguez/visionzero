USE vision_zero;

-- Monthly totals and min/max by borough/NYC
DROP TABLE IF EXISTS collisions_by_month;
CREATE TABLE collisions_by_month
    AS
SELECT
  year,
  month,
  borough,
  sum(collision_count) as collision_count,
  min(collision_count) as min_collision_count,
  max(collision_count) as max_collision_count,
  sum(collision_injured_count) as injured_count,
  min(collision_injured_count) as min_injured_count,
  max(collision_injured_count) as max_injured_count,
  sum(collision_killed_count) as killed_count,
  min(collision_killed_count) as min_killed_count,
  max(collision_killed_count) as max_killed_count
FROM collisions
GROUP BY year, month, borough
    UNION ALL
SELECT
  year,
  month,
  'NYC' as borough,
  sum(collision_count) as collision_count,
  min(collision_count) as min_collision_count,
  max(collision_count) as max_collision_count,
  sum(collision_injured_count) as injured_count,
  min(collision_injured_count) as min_injured_count,
  max(collision_injured_count) as max_injured_count,
  sum(collision_killed_count) as killed_count,
  min(collision_killed_count) as min_killed_count,
  max(collision_killed_count) as max_killed_count
FROM collisions
GROUP BY year, month;
