def get_impala_query(q):
    if q == "violations_by_month":
        return """
            SELECT *
            from violations_by_month
            ORDER BY borough, year, month;
        """

    if q == "violations_percent_change":
        return """
            SELECT
                year,
                month,
                borough,
                number_of_violations,
                round((1 - (number_of_violations / (lag(number_of_violations) over (
                    partition by borough
                    order by year, month
                )))) * -100, 2) as violations_percent_change
            from violations_by_month
            ORDER BY borough, year, month;
        """

    if q == "violations_running_avg":
        return """
            SELECT
                year,
                month,
                borough,
                number_of_violations,
                round(avg(number_of_violations) over (
                    PARTITION BY borough
                    ORDER BY year, month
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW    
                ), 2) as 3mo_violations,
                round(avg(number_of_violations) over (
                    PARTITION BY borough
                    ORDER BY year, month
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                ), 2) as 6mo_violations,
                round(avg(number_of_violations) over (
                    PARTITION BY borough
                    ORDER BY year, month
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ), 2) as 12mo_violations
            from violations_by_month 
            order by borough, year, month;
        """

    if q == "collisions_by_month":
        return """
            SELECT *
            from collisions_by_month
            ORDER BY borough, year, month;
        """

    if q == "collisions_percent_change":
        return """
            SELECT
                year,
                month,
                borough,
                collision_count,
                ROUND((1 - (collision_count / (LAG(collision_count) OVER (
                    PARTITION BY borough
                    ORDER BY year, month    
                )))) * -100, 2) AS collision_percent_change,
                killed_count,
                ROUND((1 - (killed_count / (LAG(killed_count) OVER (
                    PARTITION BY borough
                    ORDER BY year, month    
                )))) * -100, 2) AS killed_percent_change,
                injured_count,
                ROUND((1 - (injured_count / (LAG(injured_count) OVER (
                    PARTITION BY borough
                    ORDER BY year, month    
                )))) * -100, 2) AS injured_percent_change
            FROM collisions_by_month
            ORDER BY borough, year, month;
        """

    if q == "collisions_running_avg":
        return """
            SELECT
                year,
                month,
                borough,
                collision_count,
                ROUND(AVG(collision_count) OVER (
                    PARTITION BY borough
                    ORDER BY year, month
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ), 2) AS 3mo_collision_count,
                ROUND(AVG(collision_count) OVER (
                    PARTITION BY borough
                    ORDER BY year, month
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                ), 2) AS 6mo_collision_count,
                ROUND(AVG(collision_count) OVER (
                    PARTITION BY borough
                    ORDER BY year, month
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ), 2) AS 12mo_collision_count,
                injured_count,
                ROUND(AVG(injured_count) OVER (
                     PARTITION BY borough
                    ORDER BY year, month
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW   
                ), 2) AS 3mo_injured_count,
                ROUND(AVG(injured_count) OVER (
                    PARTITION BY borough
                    ORDER BY year, month
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                ), 2) AS 6mo_injured_count,
                ROUND(AVG(injured_count) OVER (
                    PARTITION BY borough
                    ORDER BY year, month
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ), 2) AS 12mo_injured_count,
                killed_count,
                ROUND(AVG(killed_count) OVER (
                    PARTITION BY borough
                    ORDER BY year, month
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW    
                ), 2) AS 3mo_killed_count,
                ROUND(AVG(killed_count) OVER (
                    PARTITION BY borough
                    ORDER BY year, month
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                ), 2) AS 6mo_killed_count,
                ROUND(AVG(killed_count) OVER (
                    PARTITION BY borough
                    ORDER BY year, month
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ), 2) AS 12mo_killed_count
            FROM collisions_by_month
            ORDER BY borough, year, month;
        """

    if q == "tlc_by_month":
        return """
            SELECT *
            from tlc_by_month
            ORDER BY year, month;
        """

    if q == "tlc_percent_change":
        return """
            SELECT 
                year,
                month,
                avg_trip_minutes,
                round((1 - (avg_trip_minutes / (lag(avg_trip_minutes) over (
                    order by year, month    
                )))) * -100, 2) as minutes_percent_change,
                avg_trip_miles,
                round((1 - (avg_trip_miles / (lag(avg_trip_miles) over (
                    order by year, month
                )))) * -100, 2) as miles_percent_change,
                avg_trip_mph,
                round((1 - (avg_trip_mph / (lag(avg_trip_mph) over (
                    order by year, month
                )))) * -100, 2) as mph_percent_change,
                avg_trip_fare,
                round((1 - (avg_trip_fare / (lag(avg_trip_fare) over (
                    order by year, month    
                )))) * -100, 2) as fare_percent_change
            from tlc_by_month
            ORDER BY year, month;
        """

    if q == "tlc_running_avg":
        return """
            SELECT
                year,
                month,
                round(avg(avg_trip_minutes) over (
                    ORDER BY year, month
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ), 2) as 3mo_trip_minutes,
                round(avg(avg_trip_miles) over (
                    ORDER BY year, month
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ), 2) as 3mo_trip_miles,
                round(avg(avg_trip_mph) over (
                    ORDER BY year, month
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ), 2) as 3mo_trip_mph,
                round(avg(avg_trip_fare) over (
                    ORDER BY year, month
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ), 2) as 3mo_trip_fare,
                round(avg(avg_trip_minutes) over (
                    ORDER BY year, month
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW    
                ), 2) as 6mo_trip_minutes,
                round(avg(avg_trip_miles) over (
                    ORDER BY year, month
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW    
                ), 2) as 6mo_trip_miles,
                round(avg(avg_trip_mph) over (
                    ORDER BY year, month
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW    
                ), 2) as 6mo_trip_mph,
                round(avg(avg_trip_fare) over (
                    ORDER BY year, month
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW    
                ), 2) as 6mo_trip_fare,
                round(avg(avg_trip_minutes) over (
                    ORDER BY year, month
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW    
                ), 2) as 12mo_trip_minutes,
                round(avg(avg_trip_miles) over (
                    ORDER BY year, month
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW    
                ), 2) as 12mo_trip_miles,
                round(avg(avg_trip_mph) over (
                    ORDER BY year, month
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW    
                ), 2) as 12mo_trip_mph,
                round(avg(avg_trip_fare) over (
                    ORDER BY year, month
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW    
                ), 2) as 12mo_trip_fare
            from tlc_by_month
            order by year, month;
        """
