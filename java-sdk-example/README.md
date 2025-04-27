# VAST Database Java SDK Example Project

## Declaring Dependency in pom.xml

### Using the s3 wagon

The maven repo with the Java SDK (and other VAST dependencies) is over s3.
An S3 wagon has to be declared (knows how to deal with maven repos using the s3 protocol (`s3://`)

```xml
<build>
    <extensions>
        <extension>
        <groupId>com.github.seahen</groupId>
        <artifactId>maven-s3-wagon</artifactId>
        <version>1.3.3</version>
        </extension>
    </extensions>
</build>
```

### VAST Database Maven Repo

Must be declared in addition to Maven Central (or other repository containing the non vast dependencies)

```xml
<repositories>
    <repository>
        <id>vastdb-maven-release</id>
        <url>s3://vast-maven-repo/release</url>
    </repository>
    <repository>
            <id>central</id>
            <name>Maven Central Repository</name>
            <url>https://repo.maven.apache.org/maven2</url>
    </repository>
</repositories>
```


### Java SDK Dep

```xml
<dependencies>
    <dependency>
        <groupId>com.vastdata.vdb</groupId>
        <artifactId>sdk</artifactId>
        <version>5.3.0</version>
    </dependency>
</dependencies>
```


## Using the SDK

#### 1. Instantiate VastSdk

Create a `VastSdkConfig` using your VAST endpoint URI and AWS credentials. Then, instantiate `VastSdk` using an `HttpClient` and the `VastSdkConfig`.

#### 2. Get a Table Object

Obtain a `Table` instance using `sdk.getTable(schemaName, tableName)`.

#### 3. Load Table Schema

The schema of a `Table` is loaded explicitly by calling `table.loadSchema()` once after creating a `Table` object. This retrieves column information from the VAST database.

#### 4. Table Operations

Use `table.put(recordBatch)` to insert rows and `table.get(columnNames, rowid)` to retrieve rows from the table.

