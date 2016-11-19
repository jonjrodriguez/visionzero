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
	* hadoop jar etl/tlc/TlcEtlDriver.jar vizionzero/data/original/tlc vizionzero/data/formatted/tlc
	* hadoop jar etl/violations/ViolationsEtlDriver.jar vizionzero/data/original/violations vizionzero/data/formatted/violations

4. Create Impala/Hive schema
	* Update {base_path} in schema.q
	* Run either:
		* Impala: impala-shell -f schema.q
		* Hive: beeline -u jdbc:hive2://babar.es.its.nyu.edu:10000/ -f schema.q