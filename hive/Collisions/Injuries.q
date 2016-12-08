-- Total number of injuries per year
DROP TABLE IF EXISTS TotalInjuriesPerYear;
CREATE TABLE TotalInjuriesPerYear
AS
  SELECT Year, SUM(CollisionInjuredCount) AS CollisionInjuredCount
  FROM collisions
  GROUP BY Year;

-- Total number of injuries per year by borough
DROP TABLE IF EXISTS TotalInjuriesPerYearByBorough;
CREATE TABLE TotalInjuriesPerYearByBorough
AS
  SELECT Borough, Year, SUM(CollisionInjuredCount) AS CollisionInjuredCount
  FROM collisions
  GROUP BY Borough, Year;

-- Total number of injuries per month
DROP TABLE IF EXISTS TotalInjuriesPerMonth;
CREATE TABLE TotalInjuriesPerMonth
AS
  SELECT Year, Month, SUM(CollisionInjuredCount) AS CollisionInjuredCount
  FROM collisions
  GROUP BY Year, Month;

-- Total number of injuries per month by borough
DROP TABLE IF EXISTS TotalInjuriesPerMonthByBorough;
CREATE TABLE TotalInjuriesPerMonthByBorough
AS
  SELECT Borough, Year, Month, SUM(CollisionInjuredCount) AS CollisionInjuredCount
  FROM collisions
  GROUP BY Borough, Year, Month;

  -- Running average of injuries
  DROP TABLE IF EXISTS InjuriesRunningAverage;
  CREATE TABLE InjuriesRunningAverage
  AS
    SELECT Year, Month, CollisionInjuredCount,
           ROUND(AVG(CollisionInjuredCount) OVER W3, 2) AS 3moInjuries,
           ROUND(AVG(CollisionInjuredCount) OVER W6, 2) AS 6moInjuries,
           ROUND(AVG(CollisionInjuredCount) OVER W12, 2) AS 12moInjuries
    FROM TotalInjuriesPerMonth
    GROUP BY Year, Month, CollisionInjuredCount
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

  -- Running average of injuries by borough
  DROP TABLE IF EXISTS InjuriesRunningAverageByBorough;
  CREATE TABLE InjuriesRunningAverageByBorough
  AS
    SELECT Borough, Year, Month, CollisionInjuredCount,
           ROUND(AVG(CollisionInjuredCount) OVER W3, 2) AS 3moInjuries,
           ROUND(AVG(CollisionInjuredCount) OVER W6, 2) AS 6moInjuries,
           ROUND(AVG(CollisionInjuredCount) OVER W12, 2) AS 12moInjuries
    FROM TotalInjuriesPerMonthByBorough
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

  -- Monthly percentage change in number of injuries
  DROP TABLE IF EXISTS InjuriesPercentChange;
  CREATE TABLE InjuriesPercentChange
  AS
    SELECT Year, Month, CollisionInjuredCount,
           ROUND((1 - (CollisionInjuredCount / LAG(CollisionInjuredCount) OVER W)) * -100, 2) AS InjuriesPercentChange
    FROM TotalInjuriesPerMonth
    ORDER BY Year, Month
    WINDOW W AS (
      ORDER BY Year, Month
    );

  -- Monthly percentage change in number of injuries by borough
  DROP TABLE IF EXISTS InjuriesPercentChangeByBorough;
  CREATE TABLE InjuriesPercentChangeByBorough
  AS
    SELECT Borough, Year, Month, CollisionInjuredCount,
           ROUND((1 - (CollisionInjuredCount / LAG(CollisionInjuredCount) OVER W)) * -100, 2) AS InjuriesPercentChange
    FROM TotalInjuriesPerMonthByBorough
    ORDER BY Year, Month
    WINDOW W AS (
      PARTITION BY Borough
      ORDER BY Year, Month
    );
