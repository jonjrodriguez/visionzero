# Profile

### Build

1. javac -classpath `yarn classpath`:. *.java
2. jar -cvfe TlcProfilingDriver.jar TlcProfilingDriver *.class

### Data source

[TLC Trip Data](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)

files: 89
size: 121G
rows: 274,424,440

### Fields:

__tpep-pickup-datetime:__ date and time when meter was engaged
  max: 2016-06-30 23:59:59
  min: 2012-01-01 00:00:01
  type: dates (some invalid)

__tpep-dropoff-datetime:__ date and time when meter was disengaged
  max: 2253-08-23 07:56:38
  min: 1900-01-01 00:00:00
  type: dates (some invalid)

__trip-distance:__ elapsed trip distance in miles
  max: 198623013.60
  min: -40840124.40
  type: mix of string and numbers

__fare-amount:__ time and distance far calculated by the meter
  max: 825998.61
  min: -1430.00
  type: mix of string and numbers

__time-minutes:__ calculated value based on pickup and drop-off date time
  max: 125373160.83
  min: -61001725.97
  type: numbers

__average-mph:__ calculated value based on trip distance and length
  max: 3079127441.74
  min: 0.00
  type: numbers