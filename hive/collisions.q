USE vision_zero;

-- Number of collisions in every month
DROP TABLE IF EXISTS CollisionsByMonth;
CREATE TABLE CollisionsByMonth
AS
SELECT
  Year as Year,
  Month as Month,
  count(*) as CollisionCount,
FROM collisions
GROUP BY Year, Month;

-- Number of Injuries and Deaths in every month
DROP TABLE IF EXISTS InjuriesByMonth;
CREATE TABLE InjuriesByMonth
AS
SELECT
  year as year,
  month as month,
  SUM(CollisionInjuredCount) as InjuredCount,
  SUM(CollisionKilledCount) as KilledCount,
FROM collisions
GROUP BY year, month

-- Number of collisions per borough
DROP TABLE IF EXISTS BoroughCollisionsByMonth;
CREATE TABLE BoroughCollisionsByMonth
AS
SELECT
  Borough as Borough
  count(*) as CollisionCount
FROM collisions
GROUP BY Borough
