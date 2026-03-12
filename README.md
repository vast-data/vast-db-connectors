# VAST DB Connectors

Connectors for **Trino** and **Apache Spark** to query VAST data.

---

## Contents

- [Build requirements](#build-requirements)
- [Trino connector](#building-trino-vast-connector)
- [Spark connector](#building-spark-vast-connector)

---

## Build requirements

- **OS:** macOS or Linux
- **Java:** Java 23+ (Trino) or Java 11 64-bit (Spark)
- **Build:** Git, Maven (or use the included `./mvnw`)

---

## Building Trino VAST connector

**Ubuntu/Debian:**

```bash
sudo apt install openjdk-23-jdk git
./mvnw -N clean install && ./mvnw -pl plugin/ndb-common clean install
./mvnw -f plugin/trino-vast/pom.xml clean package
```

**Output:** `plugin/trino-vast/target/trino-vast-*.zip`

---

## Building Spark VAST connector

**Ubuntu/Debian – install Java 11:**

```bash
sudo apt install openjdk-11-jdk git
```

Then build for your Spark/Scala version:

| Target | Command | Output | Dependencies (`spark.jars`) |
|--------|---------|--------|-----------------------------|
| **Spark 3.4** | `./mvnw -pl plugin/spark3/spark34 -am clean package` | `plugin/spark3/spark34/target/spark-vast-spark34-*.jar` | `plugin/spark3/spark34/target/dependencies` |
| **Spark 3.5** (Scala 2.13) | `./mvnw -pl plugin/spark3/spark35 -am clean package` | `plugin/spark3/spark35/target/spark-vast-spark35-*.jar` | `plugin/spark3/spark35/target/dependencies` |
| **Spark 3.5** (Scala 2.12) | `./mvnw -pl plugin/spark3/spark35-scala212 -am clean package` | `plugin/spark3/spark35-scala212/target/spark-vast-spark35-scala212-*.jar` | `plugin/spark3/spark35-scala212/target/dependencies` |
