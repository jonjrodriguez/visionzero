# Excel Parser resources

https://sreejithrpillai.wordpress.com/2014/11/06/excel-inputformat-for-hadoop-mapreduce/
http://stackoverflow.com/questions/32986678/xlsx-with-multiple-sheets-as-input-to-mapreduce

# Dependencies

[Apache POI](https://poi.apache.org/)

# Build

1. javac -classpath "`yarn classpath`:./poi/*:." *.java
2. jar -cvfm ViolationsEtlDriver.jar manifest.mf *.class