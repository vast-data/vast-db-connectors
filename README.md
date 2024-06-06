## Build requirements

* Mac OS X or Linux
* Java 17.0.8+, 64-bit

## Building Trino VAST connector on Ubuntu/Debian

```
sudo apt install openjdk-17-jdk git
./mvnw package
```

The artifact should be at `plugin/trino-vast/target/trino-vast-*.zip`.
