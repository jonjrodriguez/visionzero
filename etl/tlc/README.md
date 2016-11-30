# Build

1. javac -classpath `yarn classpath`:. *.java
2. jar -cvfe TlcEtlDriver.jar TlcEtlDriver *.class


# Filtering

1. Any records with bad data
2. Rides that are less than 2 miles
3. Rides less than $5 and more than a reasonable price (distance * $7)
4. Rides slower than 5mph or faster than 80mph
5. Rides longer than 1.5 hours (stay in NYC)