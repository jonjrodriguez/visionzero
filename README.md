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

4. Create Hive schema (WIP)
	* Update {base_path} in hive/schema.q
	* Create database "vision_zero" in Hive
	* Hive: beeline -u jdbc:hive2://babar.es.its.nyu.edu:10000/ -f hive/schema.q
	* Hive: beeline -u jdbc:hive2://babar.es.its.nyu.edu:10000/ -f hive/tlc.q