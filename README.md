## Build requirements

* Mac OS X or Linux
* Java 17.0.9+, 11.0.11+ 64-bit


## Building Trino VAST connector on Ubuntu/Debian
```
sudo apt install openjdk-17-jdk git
./mvnw -pl ndb:ndb-common,ndb:trino-vast clean package
```
The artifact should be at `plugin/trino-vast/target/trino-vast-*.zip`.


## Building Spark3.4 VAST connector on Ubuntu/Debian
```
sudo apt install openjdk-11-jdk git
./mvnw -pl '!ndb:trino-vast' clean package
```
The artifact should be at `plugin/spark3/spark34/target/spark-vast-spark34-*.jar`. 
Dependencies for `spark.jars` should be at plugin/spark3/spark34/target/dependencies



