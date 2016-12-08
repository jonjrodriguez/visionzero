-- Total number of deaths per year
DROP TABLE IF EXISTS TotalKilledPerYear;
CREATE TABLE TotalKilledPerYear
AS
  SELECT Year, SUM(CollisionKilledCount) AS CollisionKilledCount
  FROM collisions
  GROUP BY Year;

-- Total number of deaths per year by borough
DROP TABLE IF EXISTS TotalKilledPerYearByBorough;
CREATE TABLE TotalKilledPerYearByBorough
AS
  SELECT Borough, Year, SUM(CollisionKilledCount) AS CollisionKilledCount
  FROM collisions
  GROUP BY Borough, Year;

-- Total number of deaths per month
DROP TABLE IF EXISTS TotalKilledPerMonth;
CREATE TABLE TotalKilledPerMonth
AS
  SELECT Year, Month, SUM(CollisionKilledCount) AS CollisionKilledCount
  FROM collisions
  GROUP BY Year, Month;

-- Total number of deaths per month by borough
DROP TABLE IF EXISTS TotalKilledPerMonthByBorough;
CREATE TABLE TotalKilledPerMonthByBorough
AS
  SELECT Borough, Year, Month, SUM(CollisionKilledCount) AS CollisionKilledCount
  FROM collisions
  GROUP BY Borough, Year, Month;

-- Running average of deaths
DROP TABLE IF EXISTS KilledRunningAverage;
CREATE TABLE KilledRunningAverage
AS
  SELECT Year, Month, CollisionKilledCount,
         ROUND(AVG(CollisionKilledCount) OVER W3, 2) AS 3moKilled,
         ROUND(AVG(CollisionKilledCount) OVER W6, 2) AS 6moKilled,
         ROUND(AVG(CollisionKilledCount) OVER W12, 2) AS 12moKilled
  FROM TotalKilledPerMonth
  GROUP BY Year, Month, CollisionKilledCount
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

-- Running average of deaths by borough
DROP TABLE IF EXISTS KilledRunningAverageByBorough;
CREATE TABLE KilledRunningAverageByBorough
AS
  SELECT Borough, Year, Month, CollisionKilledCount,
         ROUND(AVG(CollisionKilledCount) OVER W3, 2) AS 3moKilled,
         ROUND(AVG(CollisionKilledCount) OVER W6, 2) AS 6moKilled,
         ROUND(AVG(CollisionKilledCount) OVER W12, 2) AS 12moKilled
  FROM TotalKilledPerMonthByBorough
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

-- Monthly percentage change in number of deaths
DROP TABLE IF EXISTS KilledPercentChange;
CREATE TABLE KilledPercentChange
AS
  SELECT Year, Month, CollisionKilledCount,
         ROUND((1 - (CollisionKilledCount / LAG(CollisionKilledCount) OVER W)) * -100, 2) AS KilledPercentChange
  FROM TotalKilledPerMonth
  ORDER BY Year, Month
  WINDOW W AS (
    ORDER BY Year, Month
  );

-- Monthly percentage change in number of deaths by borough
DROP TABLE IF EXISTS KilledPercentChangeByBorough;
CREATE TABLE KilledPercentChangeByBorough
AS
  SELECT Borough, Year, Month, CollisionKilledCount,
         ROUND((1 - (CollisionKilledCount / LAG(CollisionKilledCount) OVER W)) * -100, 2) AS KilledPercentChange
  FROM TotalKilledPerMonthByBorough
  ORDER BY Year, Month
  WINDOW W AS (
    PARTITION BY Borough
    ORDER BY Year, Month
  );
