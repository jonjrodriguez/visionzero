# Vision Zero

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

4. Use Hive/Impala to Analyze the Data
  * Update {base_path} in hive/schema.q
  * Connect to Hive/Impala
  * Create database "vision_zero"
  * Run script schema scripts:
    * hive/tlc/schema.q
	* hive/violations/schema.q
	* hive/collisions/schema.q
  * Run aggregate data scripts:
    * hive/tlc/tlc_by_month.q
	* hive/violations/violations_by_month.q
	* hive/collisions/collisions_by_month.q
  * View output from all other scripts