# Vision Zero

1. Fetch data
	* python fetch/collisions.py
	* python fetch/violations.py
	* python fetch/tlc.py

2. Put data into hdfs
	* hdfs dfs -mkdir visionzero
	* hdfs dfs -put data visionzero

3. Extract, transform, and load the data
	* Build jar files if necessary (look at etl directory for instructions)
	* hadoop jar etl/tlc/TlcEtlDriver.jar vizionzero/data/tlc vizionzero/formatted/tlc

4. Create Impala/Hive schema
	* Update file locations in schema.q
	* beeline -u jdbc:hive2://babar.es.its.nyu.edu:10000/ -f schema.q