W = "           ROWS BETWEEN %d PRECEDING AND CURRENT ROW"
W3 = W % 2
W6 = W % 5
W12 = W % 11

BOR_PART = "           PARTITION BY borough"
OYM = "           ORDER BY year, month"

VIOLATIONS = """(
                SELECT
                    year,
                    month,
                    borough,
                    sum(amount) AS number_of_violations,
                    min(amount) AS min_violations,
                    max(amount) AS max_violations
                FROM violations
                JOIN boroughs_list
                ON violations.precinct = boroughs_list.precinct
                GROUP BY year, month, borough
                    UNION ALL
                SELECT
                    year,
                    month,
                    'NYC' as borough,
                    sum(amount) AS number_of_violations,
                    min(amount) AS min_violations,
                    max(amount) AS max_violations
                FROM violations
                GROUP BY year, month
            ) as violations_by_month"""

COLLISIONS = """(
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
                GROUP BY year, month
            ) as collisions_by_month"""

TLC = """(
                SELECT
                    year(pickup) as year,
                    month(pickup) as month,
                    count(1) as trip_count,
                    round(avg(trip_minutes), 2) as avg_trip_minutes,
                    min(trip_minutes) as min_trip_minutes,
                    max(trip_minutes) as max_trip_minutes,
                    round(avg(trip_miles), 2) as avg_trip_miles,
                    min(trip_miles) as min_trip_miles,
                    max(trip_miles) as max_trip_miles,
                    round(avg(trip_mph), 2) as avg_trip_mph,
                    min(trip_mph) as min_trip_mph,
                    max(trip_mph) as max_trip_mph,
                    round(avg(trip_fare), 2) as avg_trip_fare,
                    min(trip_fare) as min_trip_fare,
                    max(trip_fare) as max_trip_fare
                FROM tlc
                GROUP BY year(pickup), month(pickup)
            ) as tlc_by_month"""

def get_impala_query(q, realtime=False):
    if not realtime or realtime == "false":
        violations_by_month = "violations_by_month"
        collisions_by_month = "collisions_by_month"
        tlc_by_month = "tlc_by_month"
    else:
        violations_by_month = VIOLATIONS
        collisions_by_month = COLLISIONS
        tlc_by_month = TLC

    if q == "violations_by_month":
        return """
            SELECT *
            from %s
            ORDER BY borough, year, month;
        """ % violations_by_month

    if q == "violations_percent_change":
        return """
            SELECT
                year,
                month,
                borough,
                number_of_violations,
                round((1 - (number_of_violations / (lag(number_of_violations) over (
                    %s
                    %s
                )))) * -100, 2) as violations_percent_change
            from %s
            ORDER BY borough, year, month;
        """ % (BOR_PART, OYM, violations_by_month)

    if q == "violations_running_avg":
        return """
            SELECT
                year,
                month,
                borough,
                number_of_violations,
                round(avg(number_of_violations) over (
                    %s
                    %s
                    %s
                ), 2) as 3mo_violations,
                round(avg(number_of_violations) over (
                    %s
                    %s
                    %s
                ), 2) as 6mo_violations,
                round(avg(number_of_violations) over (
                    %s
                    %s
                    %s
                ), 2) as 12mo_violations
            from %s 
            ORDER BY borough, year, month;
        """ % (BOR_PART, OYM, W3, BOR_PART, OYM, W6, BOR_PART, OYM, W12,
               violations_by_month)

    if q == "collisions_by_month":
        return """
            SELECT *
            from %s
            ORDER BY borough, year, month;
        """ % collisions_by_month

    if q == "collisions_percent_change":
        return """
            SELECT
                year,
                month,
                borough,
                collision_count,
                ROUND((1 - (collision_count / (LAG(collision_count) OVER (
                    %s
                    %s
                )))) * -100, 2) AS collision_percent_change,
                killed_count,
                ROUND((1 - (killed_count / (LAG(killed_count) OVER (
                    %s
                    %s
                )))) * -100, 2) AS killed_percent_change,
                injured_count,
                ROUND((1 - (injured_count / (LAG(injured_count) OVER (
                    %s
                    %s
                )))) * -100, 2) AS injured_percent_change
            FROM %s
            ORDER BY borough, year, month;
        """ % (
            BOR_PART, OYM,
            BOR_PART, OYM,
            BOR_PART, OYM,
            collisions_by_month)

    if q == "collisions_running_avg":
        return """
            SELECT
                year,
                month,
                borough,
                collision_count,
                ROUND(AVG(collision_count) OVER (
                    %s
                    %s
                    %s
                ), 2) AS 3mo_collision_count,
                ROUND(AVG(collision_count) OVER (
                    %s
                    %s
                    %s
                ), 2) AS 6mo_collision_count,
                ROUND(AVG(collision_count) OVER (
                    %s
                    %s
                    %s
                ), 2) AS 12mo_collision_count,
                injured_count,
                ROUND(AVG(injured_count) OVER (
                    %s
                    %s
                    %s
                ), 2) AS 3mo_injured_count,
                ROUND(AVG(injured_count) OVER (
                    %s
                    %s
                    %s
                ), 2) AS 6mo_injured_count,
                ROUND(AVG(injured_count) OVER (
                    %s
                    %s
                    %s
                ), 2) AS 12mo_injured_count,
                killed_count,
                ROUND(AVG(killed_count) OVER (
                    %s
                    %s
                    %s
                ), 2) AS 3mo_killed_count,
                ROUND(AVG(killed_count) OVER (
                    %s
                    %s
                    %s
                ), 2) AS 6mo_killed_count,
                ROUND(AVG(killed_count) OVER (
                    %s
                    %s
                    %s
                ), 2) AS 12mo_killed_count
            FROM %s
            ORDER BY borough, year, month;
        """ % (
            BOR_PART, OYM, W3, BOR_PART, OYM, W6, BOR_PART, OYM, W12,
            BOR_PART, OYM, W3, BOR_PART, OYM, W6, BOR_PART, OYM, W12,
            BOR_PART, OYM, W3, BOR_PART, OYM, W6, BOR_PART, OYM, W12,
            collisions_by_month)

    if q == "tlc_by_month":
        return """
            SELECT *
            from %s
            ORDER BY year, month;
        """ % tlc_by_month

    if q == "tlc_percent_change":
        return """
            SELECT 
                year,
                month,
                avg_trip_minutes,
                round((1 - (avg_trip_minutes / (lag(avg_trip_minutes) over (
                    %s
                )))) * -100, 2) as minutes_percent_change,
                avg_trip_miles,
                round((1 - (avg_trip_miles / (lag(avg_trip_miles) over (
                    %s
                )))) * -100, 2) as miles_percent_change,
                avg_trip_mph,
                round((1 - (avg_trip_mph / (lag(avg_trip_mph) over (
                    %s
                )))) * -100, 2) as mph_percent_change,
                avg_trip_fare,
                round((1 - (avg_trip_fare / (lag(avg_trip_fare) over (
                    %s
                )))) * -100, 2) as fare_percent_change
            from %s
            ORDER BY year, month;
        """ % (OYM, OYM, OYM, OYM, tlc_by_month)

    if q == "tlc_running_avg":
        return """
            SELECT
                year,
                month,
                round(avg(avg_trip_minutes) over (
                    %s
                    %s
                ), 2) as 3mo_trip_minutes,
                round(avg(avg_trip_miles) over (
                    %s
                    %s
                ), 2) as 3mo_trip_miles,
                round(avg(avg_trip_mph) over (
                    %s
                    %s
                ), 2) as 3mo_trip_mph,
                round(avg(avg_trip_fare) over (
                    %s
                    %s
                ), 2) as 3mo_trip_fare,
                round(avg(avg_trip_minutes) over (
                    %s
                    %s
                ), 2) as 6mo_trip_minutes,
                round(avg(avg_trip_miles) over (
                    %s
                    %s
                ), 2) as 6mo_trip_miles,
                round(avg(avg_trip_mph) over (
                    %s
                    %s
                ), 2) as 6mo_trip_mph,
                round(avg(avg_trip_fare) over (
                    %s
                    %s
                ), 2) as 6mo_trip_fare,
                round(avg(avg_trip_minutes) over (
                    %s
                    %s
                ), 2) as 12mo_trip_minutes,
                round(avg(avg_trip_miles) over (
                    %s
                    %s
                ), 2) as 12mo_trip_miles,
                round(avg(avg_trip_mph) over (
                    %s
                    %s
                ), 2) as 12mo_trip_mph,
                round(avg(avg_trip_fare) over (
                    %s
                    %s
                ), 2) as 12mo_trip_fare
            from %s
            order by year, month;
        """ % (OYM, W3, OYM, W3, OYM, W3, OYM, W3,
               OYM, W6, OYM, W6, OYM, W6, OYM, W6,
               OYM, W12, OYM, W12, OYM, W12, OYM, W12,
               tlc_by_month)
