/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.vdb;

import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.DropTableContext;
import com.vastdata.client.schema.VastMetadataUtils;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransactionFactory;
import com.vastdata.vdb.sdk.VastSdk;
import com.vastdata.vdb.sdk.VastSdkConfig;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.net.URI;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static java.time.ZoneOffset.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPredicateIntegration
{
    private static final Logger LOG = LoggerFactory.getLogger(TestPredicateIntegration.class);
    private static final String UTF_COLUMN = "utf_column";
    private static final String INT_COLUMN = "int_column";

    private String schemaName;
    private VastSdk sdk;
    private TransactionManager transactionsManager;
    private final Set<String> allTables = new HashSet<>();
    private URI uri;

    @BeforeClass
    public void setup()
            throws VastException
    {
        if (System.getenv("INTEG_TEST") == null) {
            throw new SkipException("Environment variable INTEG_TEST must be defined as these are integration tests");
        }
        String endpoint = "http://localhost:9090";
        String bucketName = System.getProperty("BUCKET_NAME");
        String awsAccessKeyId = System.getProperty("AWS_ACCESS_KEY_ID");
        String awsSecretAccessKey = System.getProperty("AWS_SECRET_ACCESS_KEY");

        if (bucketName == null || awsAccessKeyId == null || awsSecretAccessKey == null) {
            throw new IllegalArgumentException("AWS credentials must be provided");
        }
        schemaName = bucketName + "/" + "predicate_tests_" + System.currentTimeMillis();
        LOG.info("starting test with schema {}", schemaName);

        this.uri = URI.create(endpoint);

        VastSdkConfig config = new VastSdkConfig(uri,
                uri.toString(),
                awsAccessKeyId,
                awsSecretAccessKey);

        HttpClient httpClient = new JettyHttpClient();

        System.setProperty("arrow.memory.debug.allocator", "true");
        this.sdk = new VastSdk(httpClient, config);

        VastClient client = this.sdk.getVastClient();

        this.transactionsManager = new TransactionManager(client, new VastTransactionFactory());
        SimpleVastTransaction tx = transactionsManager.startTransaction(null);
        client.createSchema(tx, schemaName, new VastMetadataUtils().getPropertiesString(Collections.emptyMap()), null);

        transactionsManager.commit(tx, null);
    }

    private void buildMainTable(String tableName) throws VastException {
        RootAllocator allocator = new RootAllocator();
        List<Field> fields = List.of(
                Field.nullable(UTF_COLUMN, new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()),
                Field.nullable(INT_COLUMN, new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true))
        );
        createTable(tableName, fields);
        FieldVector c1Vector = buildVarcharVector(UTF_COLUMN, allocator, new String[] {"aaa", "bbb", "ccc"});
        FieldVector c2Vector = buildIntVector(INT_COLUMN, allocator, new int[] {1, 2, 3});
        VectorSchemaRoot root = VectorSchemaRoot.of(c1Vector, c2Vector);
        insertData(tableName, root);
    }

    @AfterMethod
    public void testCleanup()
            throws VastException
    {
        SimpleVastTransaction tx = transactionsManager.startTransaction(null);
        for (String tableName : allTables) {
            sdk.getVastClient().dropTable(tx, new DropTableContext(schemaName, tableName), null);
        }
        allTables.clear();
        transactionsManager.commit(tx, null);
    }

    @AfterClass
    public void teardown()
            throws VastException
    {
        SimpleVastTransaction tx = transactionsManager.startTransaction(null);
        sdk.getVastClient().dropSchema(tx, schemaName, null);
        transactionsManager.commit(tx, null);
    }

    @Test
    public void testFlushTableCache()
            throws Exception
    {
        String tableName = "test-flush-table-cache";
        buildMainTable(tableName);
        String sql = String.format("SELECT * FROM \"%s\".\"%s\" WHERE %s = 1", schemaName, tableName, INT_COLUMN);
        sdk.executeQuery(sql);
        assertTrue(sdk.flushTableCache(Optional.of(schemaName), Optional.of(tableName)));
    }

    @Test
    public void testSimpleFullScan()
            throws Exception
    {
        String tableName = "test-simple-full-scan";
        buildMainTable(tableName);
        VectorSchemaRoot vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\"", schemaName, tableName)).next();
        assertEquals(vectorSchemaRoot.getRowCount(), 3);
    }

    @Test
    public void testColumnOnlyInPredicate()
            throws Exception
    {
        String tableName = "test-column-only-in-predicate";
        buildMainTable(tableName);
        VectorSchemaRoot vectorSchemaRoot = sdk.executeQuery(String.format("select %s from \"%s\".\"%s\" WHERE %s = 'aaa'", INT_COLUMN, schemaName, tableName, UTF_COLUMN)).next();
        assertEquals(vectorSchemaRoot.getRowCount(), 1);
    }

    @Test
    public void testLimit()
            throws Exception
    {
        String tableName = "test-limit";
        buildMainTable(tableName);
        VectorSchemaRoot vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" LIMIT 1", schemaName, tableName)).next();
        assertTrue(vectorSchemaRoot.getRowCount() == 1);
    }

    @Test
    public void testSimplePredicate()
            throws VastException
    {
        String tableName = "test-simple-predicate";
        buildMainTable(tableName);
        VectorSchemaRoot vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s = 'aaa'", schemaName, tableName, UTF_COLUMN)).next();
        assertTrue(vectorSchemaRoot.getRowCount() == 1);
        vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s = 3", schemaName, tableName, INT_COLUMN)).next();
        assertTrue(vectorSchemaRoot.getRowCount() == 1);
    }

    @Test
    public void testNullPredicate()
            throws VastException
    {
        String tableName = "test-null-predicate";
        buildMainTable(tableName);
        VectorSchemaRoot vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s IS NULL", schemaName, tableName, UTF_COLUMN)).next();
        assertTrue(vectorSchemaRoot.getRowCount() == 0);
        vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s IS NOT NULL", schemaName, tableName, UTF_COLUMN)).next();
        assertTrue(vectorSchemaRoot.getRowCount() == 3);
    }

    @Test
    public void testBigDecimalPredicate()
            throws VastException
    {
        String tableName = "test-big-decimal-predicate";
        RootAllocator allocator = new RootAllocator();
        String columnName = "decimal_column";
        List<Field> fields = List.of(
                Field.nullable(columnName, new ArrowType.Decimal(38, 0, 128))
        );
        createTable(tableName, fields);
        Decimal256Vector decimalVector = new Decimal256Vector(columnName, fields.get(0).getFieldType(), allocator);
        BigDecimal decimal1 = BigDecimal.valueOf(3);
        BigDecimal decimal2 = BigDecimal.valueOf(4);
        decimalVector.allocateNew(2);
        decimalVector.setSafe(0, decimal1);
        decimalVector.setSafe(1, decimal2);
        decimalVector.setValueCount(2);
        insertData(tableName, VectorSchemaRoot.of(decimalVector));

        VectorSchemaRoot vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s = 3", schemaName, tableName, columnName)).next();
        assertTrue(vectorSchemaRoot.getRowCount() == 1);
    }

    @Test
    public void testDatePredicate()
            throws VastException
    {
        String tableName = "test-date-predicate";
        RootAllocator allocator = new RootAllocator();
        String columnName = "date_column";
        List<Field> fields = List.of(
                Field.nullable(columnName, new ArrowType.Date(DateUnit.DAY))
        );
        createTable(tableName, fields);
        DateDayVector dateVector = new DateDayVector(columnName, fields.get(0).getFieldType(), allocator);
        dateVector.allocateNew(2);
        dateVector.setSafe(0, (int) Date.valueOf("2000-01-01").toLocalDate().toEpochDay());
        dateVector.setSafe(1, (int) Date.valueOf("2025-01-01").toLocalDate().toEpochDay());
        dateVector.setValueCount(2);
        insertData(tableName, VectorSchemaRoot.of(dateVector));

        VectorSchemaRoot vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s = '2000-01-01'", schemaName, tableName, columnName)).next();
        assertTrue(vectorSchemaRoot.getRowCount() == 1);
    }

    @Test
    public void testTimestampPredicate()
            throws VastException
    {
        String tableName = "test-timestamp-predicate";
        RootAllocator allocator = new RootAllocator();
        String columnName = "timestamp_column";
        List<Field> fields = List.of(
                Field.nullable(columnName, new ArrowType.Timestamp(TimeUnit.SECOND, null))
        );
        createTable(tableName, fields);
        TimeStampVector timestampVector = new TimeStampSecVector(columnName, fields.get(0).getFieldType(), allocator);
        timestampVector.allocateNew(2);
        timestampVector.setSafe(0, LocalDateTime.parse("2024-11-16T12:34:56").toEpochSecond(UTC));
        timestampVector.setSafe(1, LocalDateTime.parse("2000-11-16T12:34:56").toEpochSecond(UTC));
        timestampVector.setValueCount(2);
        insertData(tableName, VectorSchemaRoot.of(timestampVector));

        VectorSchemaRoot vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s = '2024-11-16T12:34:56'", schemaName, tableName, columnName)).next();
        assertTrue(vectorSchemaRoot.getRowCount() == 1);
        vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s IN ('2024-11-16T12:34:56', '2000-11-16T12:34:56')", schemaName, tableName, columnName)).next();
        assertTrue(vectorSchemaRoot.getRowCount() == 2);
    }

    @Test
    public void testInPredicate()
            throws VastException
    {
        String tableName = "test_in_predicate";
        buildMainTable(tableName);
        VectorSchemaRoot vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s in (1,2,3)", schemaName, tableName, INT_COLUMN)).next();
        assertEquals(vectorSchemaRoot.getRowCount(), 3);
        vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s in ('aaa', 'bbb', 'ddd')", schemaName, tableName, UTF_COLUMN)).next();
        assertEquals(vectorSchemaRoot.getRowCount(), 2);
    }

    @Test
    public void testOrPredicate()
            throws VastException
    {
        String tableName = "test_or_predicate";
        buildMainTable(tableName);
        VectorSchemaRoot vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s = 1 OR %s = 2", schemaName, tableName, INT_COLUMN, INT_COLUMN)).next();
        assertEquals(vectorSchemaRoot.getRowCount(), 2);
    }

    @Test
    public void testAndPredicate()
            throws VastException
    {
        String tableName = "test_and_predicate";
        buildMainTable(tableName);
        VectorSchemaRoot vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s = 1 AND %s = 'aaa'", schemaName, tableName, INT_COLUMN, UTF_COLUMN)).next();
        assertEquals(vectorSchemaRoot.getRowCount(), 1);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testMultiColumnOrPredicate()
            throws VastException
    {
        String tableName = "test_multi_or";
        buildMainTable(tableName);
        VectorSchemaRoot vectorSchemaRoot = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s = 1 OR %s = 'aaa'", schemaName, tableName, INT_COLUMN, UTF_COLUMN)).next();
        assertEquals(vectorSchemaRoot.getRowCount(), 2);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testPreventFunctionPredicate()
            throws VastException
    {
        String tableName = "invalid_function_predicate";
        buildMainTable(tableName);
        sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE abs(%s) = 1", schemaName, tableName, INT_COLUMN)).next();
    }

    @Test
    public void testMultipleResultBatches()
            throws VastException
    {
        int batchTotalRows = 100_000;
        int numOfBatches = 6;
        String tableName = "test_multiple_result_batches";
        RootAllocator allocator = new RootAllocator();
        List<Field> fields = List.of(
                Field.nullable(INT_COLUMN, new ArrowType.Int(32, true))
        );
        createTable(tableName, fields);
        int[] values = IntStream.range(0, batchTotalRows).toArray();

        FieldVector c1Vector = buildIntVector(INT_COLUMN, allocator, values);
        VectorSchemaRoot root = VectorSchemaRoot.of(c1Vector);
        for (int i = 0; i < numOfBatches; i ++) { // more than 512k rows
            insertData(tableName, root);
        }

        Iterator<VectorSchemaRoot> iterator = sdk.executeQuery(String.format("select * from \"%s\".\"%s\" WHERE %s >= 0", schemaName, tableName, INT_COLUMN));
        int totalFetchedRows = 0;
        int numOfBatchesFetched = 0;
        while (iterator.hasNext()) {
            VectorSchemaRoot batch = iterator.next();
            LOG.info("fetched batch with {} rows", batch.getRowCount());
            totalFetchedRows += batch.getRowCount();
            numOfBatchesFetched++;
        }
        assertEquals(totalFetchedRows, (numOfBatches * batchTotalRows));
        assertTrue(numOfBatchesFetched > 1);
    }

    private void insertData(String tableName, VectorSchemaRoot root)
            throws VastException
    {
        SimpleVastTransaction tx = transactionsManager.startTransaction(null);
        sdk.getVastClient().insertRows(tx, schemaName, tableName, root, uri, Optional.empty(), null);
        transactionsManager.commit(tx, null);
    }

    private void createTable(String tableName, List<Field> columns)
            throws VastException
    {
        SimpleVastTransaction tx = transactionsManager.startTransaction(null);
        sdk.getVastClient().createTable(tx, new CreateTableContext(
                schemaName, tableName, columns,
                null, Collections.emptyMap()), null);
        transactionsManager.commit(tx, null);
        this.allTables.add(tableName);
        LOG.info("table {}.{} was created", schemaName, tableName);
    }

    private IntVector buildIntVector(String name, RootAllocator allocator, int[] values)
    {
        IntVector intVector = new IntVector(name, allocator);
        intVector.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            intVector.set(i, values[i]);
        }
        intVector.setValueCount(values.length);
        return intVector;
    }

    private VarCharVector buildVarcharVector(String name, RootAllocator allocator, String[] values)
    {
        VarCharVector varcharVector = new VarCharVector(name, allocator);
        varcharVector.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            varcharVector.setSafe(i, values[i].getBytes());
        }
        varcharVector.setValueCount(values.length);
        return varcharVector;
    }
}
