# VAST Database Java SDK Example Project

## Declaring Dependency in pom.xml

### VAST Database Maven Repo

Must be declared in addition to Maven Central (or other repository containing the non vast dependencies)

```xml
<repositories>
	<repository>
	    <id>vastdb-maven-release</id>
		<url>https://vast-maven-repo.s3.amazonaws.com/release</url>
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
        <version>5.3.0.1</version>
    </dependency>
</dependencies>
```




## Reference

This section provides a reference for key classes and methods in the VAST Database Java SDK.

### `com.vastdata.vdb.sdk.VastSdkConfig`

Configuration class for initializing the `VastSdk`. It holds connection details like endpoint URI and AWS credentials.

#### Constructor

```java
public VastSdkConfig(URI endpoint, String dataEndpoints, String awsAccessKeyId, String awsSecretAccessKey)
```

Creates a new `VastSdkConfig` instance.

*   `endpoint`: The main endpoint URI for the VAST cluster
*   `dataEndpoints`: A string representation of the data endpoints (CNodes URLs)
*   `awsAccessKeyId`: Your AWS access key ID for authentication.
*   `awsSecretAccessKey`: Your AWS secret access key for authentication.


### `com.vastdata.vdb.sdk.VastSdk`

The main entry point for interacting with the VAST database using the SDK.

#### Constructors

```java
public VastSdk(HttpClient httpClient, VastSdkConfig config)
```

Creates a new `VastSdk` instance with default retry settings.

*   `httpClient`: An instance of `io.airlift.http.client.HttpClient` (e.g., `JettyHttpClient`).
*   `config`: The `VastSdkConfig` containing connection details.

```java
public VastSdk(HttpClient httpClient, VastSdkConfig config, RetryConfig retryConfig)
```

Creates a new `VastSdk` instance with custom retry settings.

*   `httpClient`: An instance of `io.airlift.http.client.HttpClient`.
*   `config`: The `VastSdkConfig` containing connection details.
*   `retryConfig`: A `RetryConfig` instance specifying retry behavior for data operations.

#### Methods

```java
public Table getTable(String schemaName, String tableName)
```

Obtains a `Table` object representing a specific table in the VAST database. The table schema is NOT loaded upon creation; you must call `loadSchema()` on the returned `Table` object.

## `com.vastdata.vdb.sdk.Table`

#### Methods

```java
public void loadSchema() throws NoExternalRowIdColumnException, RuntimeException
```

Explicitly loads the schema for this table from the VAST database. This MUST be called before performing data operations like `get` or `put` that rely on the schema.

*   **Throws:**
    *   `NoExternalRowIdColumnException`: If the table schema does not contain the required external row ID column (`vastdb_rowid`).
    *   `RuntimeException`: If a `VastException` occurs during the schema lookup.

```java
public Schema getSchema() throws TableSchemaNotLoadedException
```

Returns the Apache Arrow Schema for the table, *excluding* the `vastdb_rowid` column. `loadSchema()` must have been called previously.

*   **Returns:** The table's Arrow `Schema`.
*   **Throws:** `TableSchemaNotLoadedException`: If `loadSchema()` has not been called.

```java
public VectorSchemaRoot get(ArrayList<String> columnNames, long rowid) throws TableSchemaNotLoadedException
```

Retrieves a single row from the table by its internal row ID.

*   `columnNames`: An optional `ArrayList` of column names to project. If `null`, all columns (including `vastdb_rowid`) are returned.
*   `rowid`: The internal row ID of the row to retrieve.
*   **Returns:** A `VectorSchemaRoot` containing the requested row data.
*   **Throws:** `TableSchemaNotLoadedException`: If `loadSchema()` has not been called.

```java
public VectorSchemaRoot put(VectorSchemaRoot recordBatch) throws VastException
```

Inserts a batch of records into the table. The schema of the input `VectorSchemaRoot` must match the table's schema (as returned by `getSchema()`).

*   `recordBatch`: The `VectorSchemaRoot` containing the data to insert.
*   **Returns:** A `VectorSchemaRoot` containing the internal row IDs assigned to the inserted rows. This `VectorSchemaRoot` contains a single column named `vastdb_rowid` of type `UInt8Vector`.
*   **Throws:** `VastException`: If an error occurs during the insert operation.

```java
public void delete(VectorSchemaRoot rowIds) throws VastException
```

Deletes rows from table using supplied rowIds. The schema of the input `VectorSchemaRoot` must be of a single vector of the row id type (Unsigned 64 bit Integer).

*   `rowIds`: The `VectorSchemaRoot` containing row ids of rows to be removed.
*   **Throws:** `VastException`: If an error occurs during the insert operation.

### Exceptions

#### `com.vastdata.vdb.sdk.NoExternalRowIdColumnException`

Extends `com.vastdata.client.error.VastUserException`. Thrown by `Table.loadSchema()` if the table does not have the required `vastdb_rowid` column. This column is necessary for row-level operations.

#### `com.vastdata.vdb.sdk.TableSchemaNotLoadedException`

Extends `java.lang.RuntimeException`. Thrown by `Table.getSchema()` or `Table.get()` if `Table.loadSchema()` has not been called yet.

## Basic Workflow

Basic workflow is as follows:

1. Instantiate VastSdk
   - Create a `VastSdkConfig` using your VAST endpoint URI and AWS credentials. Then, instantiate `VastSdk` using an `HttpClient` and the `VastSdkConfig`.
2. Get a Table Object
	- Obtain a `Table` instance using `sdk.getTable(schemaName, tableName)`.
3. Load Table Schema
	- The schema of a `Table` is loaded explicitly by calling `table.loadSchema()` once after creating a `Table` object. This retrieves column information from the VAST database.
4. Table Operations
	- Use `table.put(recordBatch)` to insert rows and `table.get(columnNames, rowid)` to retrieve rows from the table.


## Limitations

### Table Type

The table type must be **External RowID** enabled with **Internal RowID Allocation**

see https://support.vastdata.com/s/article/UUID-48d0a8cf-5786-5ef3-3fa3-9c64e63a0967 for more information

#### External RowID

Table has to be created with the `vastdb_rowid: int64` column.

#### Internal RowID Allocation

Internal RowID Allocation is determined by the first insertion into that table.

*   If the first record batch includes the `vastdb_rowid` column, the table will use External RowID Allocation.
*   If it *does not* include the `vastdb_rowid` column, the table will use Internal RowID Allocation.

### Update, Delete etc..

Only get and put are currently supported.
