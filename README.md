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

	* export LIBJARS=etl/violations/poi/poi-3.15.jar,etl/violations/poi/poi-ooxml-3.15.jar,etl/violations/poi/poi-ooxml-schemas-3.15.jar,etl/violations/poi/ooxml-lib/xmlbeans-2.6.0.jar,etl/violations/poi/lib/commons-collections4-4.1.jar
	* export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
	* hadoop jar etl/violations/ViolationsEtlDriver.jar visionzero/data/original/violations visionzero/data/formatted/violations -libjars ${LIBJARS}

4. Create Impala/Hive schema
	* Update {base_path} in schema.q
	* Run either:
		* Impala: impala-shell -f schema.q
		* Hive: beeline -u jdbc:hive2://babar.es.its.nyu.edu:10000/ -f schema.q