# Build

1. javac -classpath `yarn classpath`:. *.java
2. jar -cvfe TlcEtlDriver.jar TlcEtlDriver *.class

# Profile

files: 89
size: 122G

fields:
	tpep_pickup_datetime - date and time when meter was engaged
	tpep_dropoff_datetime - date and time when meter was disengaged
	trip_distance - elapsed trip distance in miles
	fare_amount - time and distance far calculated by the meter

calculated:
	time_minutes: based on pickup and drop-off date time
	average_mph: based on trip distance and length


calculated_average_mph
	max: 3079127441.74; min: 0.00; all_valid_numbers: true
calculated_time_minutes
	max: 125373160.83; min: -61001725.97; all_valid_numbers: true
fare_amount
	max: 825998.61; min: -1430.00; all_valid_numbers: false
tpep_dropoff_datetime
	max: 2253-08-23 07:56:38; min: 1900-01-01 00:00:00; all_valid_dates: false
tpep_pickup_datetime
	max: 2016-06-30 23:59:59; min: 2012-01-01 00:00:01; all_valid_dates: false
trip_distance
	max: 198623013.60; min: -40840124.40; all_valid_numbers: false