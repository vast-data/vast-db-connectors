## Build requirements

* Mac OS X or Linux
* Java 23+ (for Trino) or JAVA 11 64-bit (for Spark)


## Building Trino VAST connector on Ubuntu/Debian
```
sudo apt install openjdk-23-jdk git
./mvnw -N clean install && ./mvnw -pl plugin/ndb-common clean install
 ./mvnw -f plugin/trino-vast/pom.xml clean package
```
The artifact should be at `plugin/trino-vast/target/trino-vast-*.zip`.


## Building Spark3.4 VAST connector on Ubuntu/Debian
```
sudo apt install openjdk-11-jdk git
./mvnw -pl plugin/spark3/spark34 -am clean package
```


The artifact should be at `plugin/spark3/spark34/target/spark-vast-spark34-*.jar`. 
Dependencies for `spark.jars` should be at plugin/spark3/spark34/target/dependencies



