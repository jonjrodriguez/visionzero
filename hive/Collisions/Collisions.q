-- Total number of collisions per year
DROP TABLE IF EXISTS TotalCollisionsPerYear;
CREATE TABLE TotalCollisionsPerYear
AS
  SELECT Year, SUM(CollisionCount) AS CollisionCount
  FROM collisions
  GROUP BY Year;

-- Total number of collisions per year by borough
DROP TABLE IF EXISTS TotalCollisionsPerYearByBorough;
CREATE TABLE TotalCollisionsPerYearByBorough
AS
  SELECT Borough, Year, SUM(CollisionCount) AS CollisionCount
  FROM collisions
  GROUP BY Borough, Year;

-- Total number of collisions per month
DROP TABLE IF EXISTS TotalCollisionsPerMonth;
CREATE TABLE TotalCollisionsPerMonth
AS
  SELECT Year, Month, SUM(CollisionCount) AS CollisionCount
  FROM collisions
  GROUP BY Year, Month;

-- Total number of collisions per month by borough
DROP TABLE IF EXISTS TotalCollisionsPerMonthByBorough;
CREATE TABLE TotalCollisionsPerMonthByBorough
AS
  SELECT Borough, Year, Month, SUM(CollisionCount) AS CollisionCount
  FROM collisions
  GROUP BY Borough, Year, Month;

-- Running average of collisions
DROP TABLE IF EXISTS CollisionsRunningAverage;
CREATE TABLE CollisionsRunningAverage
AS
  SELECT Year, Month, CollisionCount,
         ROUND(AVG(CollisionCount) OVER W3, 2) AS 3moCollisions,
         ROUND(AVG(CollisionCount) OVER W6, 2) AS 6moCollisions,
         ROUND(AVG(CollisionCount) OVER W12, 2) AS 12moCollisions
  FROM TotalCollisionsPerMonth
  GROUP BY Year, Month, CollisionCount
  ORDER BY Year, Month
  WINDOW
  W3 AS (
    ORDER BY Year, Month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ),
  W6 AS (
    ORDER BY Year, Month
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
  ),
  W12 AS (
      ORDER BY Year, Month
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
  );

-- Running average of collisions by borough
DROP TABLE IF EXISTS CollisionsRunningAverageByBorough;
CREATE TABLE CollisionsRunningAverageByBorough
AS
  SELECT Borough, Year, Month, CollisionCount,
         ROUND(AVG(CollisionCount) OVER W3, 2) AS 3moCollisions,
         ROUND(AVG(CollisionCount) OVER W6, 2) AS 6moCollisions,
         ROUND(AVG(CollisionCount) OVER W12, 2) AS 12moCollisions
  FROM TotalCollisionsPerMonthByBorough
  ORDER BY Borough, Year, Month
  WINDOW
  W3 AS (
    PARTITION BY Borough
    ORDER BY Year, Month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ),
  W6 AS (
    PARTITION BY Borough
    ORDER BY Year, Month
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
  ),
  W12 AS (
      PARTITION BY Borough
      ORDER BY Year, Month
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
  );

-- Monthly percentage change in number of collisions
DROP TABLE IF EXISTS CollisionsPercentChange;
CREATE TABLE CollisionsPercentChange
AS
  SELECT Year, Month, CollisionCount,
         ROUND((1 - (CollisionCount / LAG(CollisionCount) OVER W)) * -100, 2) AS CollisionsPercentChange
  FROM TotalCollisionsPerMonth
  ORDER BY Year, Month
  WINDOW W AS (
    ORDER BY Year, Month
  );

-- Monthly percentage change in number of collisions by borough
DROP TABLE IF EXISTS CollisionsPercentChangeByBorough;
CREATE TABLE CollisionsPercentChangeByBorough
AS
  SELECT Borough, Year, Month, CollisionCount,
         ROUND((1 - (CollisionCount / LAG(CollisionCount) OVER W)) * -100, 2) AS CollisionsPercentChange
  FROM TotalCollisionsPerMonthByBorough
  ORDER BY Year, Month
  WINDOW W AS (
    PARTITION BY Borough
    ORDER BY Year, Month
  );
