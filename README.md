# Vision Zero

Github repo: https://github.com/thoughts1053/visionzero

## Directory Explanations
1. fetch: python code to programatically download our data sources:
  * [Motor Vehicle Collisions](http://www.nyc.gov/html/nypd/html/traffic_reports/motor_vehicle_collision_data.shtml)
  * [Moving Violation Summonses](http://www.nyc.gov/html/nypd/html/traffic_reports/traffic_summons_reports.shtml)
  * [TLC Trip Data](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)

2. profile: Java MapReduce code to profile each data source
  * classes and jar included for Java 1.7
  * Build instructions can be found inside corresponding etl directory

3. etl: Java MapReduce code to filter and clean each data source
  * classes and jar included for Java 1.7
  * Build instructions can be found inside corresponding etl directory

4. hive: hive analytic scripts for each data source
  * schema: setup schema for each data source
  * by_month: creates an aggregate table for each data source to help with speed and quick iterations
  * running_averages: Gets 3/6/12 month running averages for each field
  * percent_change: Gets the change for each field from the previous month

5. results
  * local downloads of the results from the hive scripts
  * includes screenshots of running the analytics and etl
  * includes tableau file and image exports of charts for data visualization

6. web: allows for realtime data access and visualization
  * Uses ssh and impyla to connect to Dumbo and query Impala
  * Option to run analytics in realtime and see output
  * Option to download results of analytics to csv

## Steps to Reproduce from Scratch
1. Fetch data
  * python fetch/collisions.py
  * python fetch/violations.py
  * python fetch/tlc.py

2. Put data into hdfs
  * hdfs dfs -mkdir visionzero
  * hdfs dfs -put data visionzero

3. Extract, transform, and load the data
  * Build jar files - look at etl directory for instructions
  * hadoop jar etl/tlc/TlcEtlDriver.jar visionzero/data/original/tlc visionzero/data/formatted/tlc
  * hadoop jar etl/violations/ViolationsEtlDriver.jar visionzero/data/original/violations visionzero/data/formatted/violations
  * hadoop jar etl/collisions/CollisionsEtlDriver.jar visionzero/data/original/collisions visionzero/data/formatted/collisions

4. Use Hive to Analyze the Data
  * Update {base_path} in the schema files
  * Connect to Hive
  * Create database "vision_zero"
  * Run script schema scripts:
    * hive/tlc/schema.q
	  * hive/violations/schema.q
	  * hive/collisions/schema.q
  * Run aggregate data scripts:
    * hive/tlc/tlc_by_month.q
	  * hive/violations/violations_by_month.q
	  * hive/collisions/collisions_by_month.q
  * View output and run other scripts

5. Realtime Web View
  * Follow README in web directory to setup server
  * Run local server
  * Download results via the browser or view the data in realtime