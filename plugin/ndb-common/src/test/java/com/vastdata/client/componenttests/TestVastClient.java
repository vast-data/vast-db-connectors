/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.componenttests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.vastdata.client.ArrowQueryDataSchemaHelper;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.QueryDataResponseHandler;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.VastSplitContext;
import com.vastdata.client.error.VastException;
import com.vastdata.client.executor.VastRetryConfig;
import com.vastdata.client.rowid.TableType;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.DropTableContext;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.schema.VastMetadataUtils;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransactionFactory;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.testng.SkipException;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestVastClient
{
    static String schemaName = "vastdb/s2";

    private String endUser;
    private URI endpoint;

    private VastClient client;

    @BeforeSuite
    public void setup()
            throws VastException
    {
        if (System.getenv("INTEG_TEST") == null) {
            throw new SkipException("Environment variable INTEG_TEST must be defined as these are integration tests");
        }

        String endpoint = "http://localhost:9090";
        String awsAccessKeyId = System.getProperty("AWS_ACCESS_KEY_ID");
        String awsSecretAccessKey = System.getProperty("AWS_SECRET_ACCESS_KEY");

        if (awsAccessKeyId == null || awsSecretAccessKey == null) {
            throw new IllegalArgumentException("AWS credentials must be provided");
        }

        this.endUser = null;

        URI uri = URI.create(endpoint);
        this.endpoint = uri;

        VastConfig config = new VastConfig();
        config.setEndpoint(uri)
                .setDataEndpoints(uri.toString())
                .setAccessKeyId(awsAccessKeyId)
                .setSecretAccessKey(awsSecretAccessKey);

        HttpClient httpClient = new JettyHttpClient();

        this.client = new VastClient(httpClient, config, new DummyDependenciesFactory(config));

        TransactionManager transactionsManager = new TransactionManager(this.client, new VastTransactionFactory());
        SimpleVastTransaction tx = transactionsManager.startTransaction(this.endUser);

        if (this.client.schemaExists(tx, schemaName, this.endUser)) {
            Stream<String> tables = client.listTables(tx, schemaName, 20, this.endUser);
            Iterable<String> tableIterable = tables::iterator;
            for (String tableName : tableIterable) {
                client.dropTable(tx, new DropTableContext(schemaName, tableName), this.endUser);
            }

            this.client.dropSchema(tx, schemaName, this.endUser);
        }

        this.client.createSchema(tx, schemaName, new VastMetadataUtils().getPropertiesString(Collections.emptyMap()),
                this.endUser);

        transactionsManager.commit(tx, this.endUser);
    }

    private void testInsertAndQueryWideTable(String tableName, List<String> sortedBy, TableType tableType)
            throws VastException
    {
        TransactionManager transactionsManager = new TransactionManager(this.client, new VastTransactionFactory());

        // 1. Create Table
        SimpleVastTransaction tx = transactionsManager.startTransaction(this.endUser);
        ArrayList<Field> fields = new ArrayList<>();
        int numCols = 2000;
        for (int i = 0; i < numCols; i++) {
            fields.add(new Field(format("col%d", i), FieldType.nullable(new ArrowType.Int(32, true)), null));
        }
        Schema arrowSchema = new Schema(fields);

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        if (!sortedBy.isEmpty()) {
            properties.put("sorted_by", sortedBy);
        }
        CreateTableContext tableContext = new CreateTableContext(schemaName, tableName, fields, null, properties.build());
        this.client.createTable(tx, tableContext, this.endUser);
        transactionsManager.commit(tx, this.endUser);

        // 2. Insert Data
        SimpleVastTransaction tx2 = transactionsManager.startTransaction(this.endUser);
        int rowCount = 100;

        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            root.setRowCount(rowCount);
            for (int colIdx = 0; colIdx < numCols; colIdx++) {
                IntVector col = (IntVector) root.getVector(format("col%d", colIdx));
                for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                    col.set(rowIdx, rowIdx);
                }
            }

            try (VectorSchemaRoot insertedRowIds = this.client.insertRowsByColumnBatches(tx2, schemaName, tableName, root,
                    this.endpoint, Optional.of(1000), new HashSet<>(sortedBy), tableType, true, this.endUser)) {
                assertEquals(insertedRowIds.getRowCount(), rowCount);
            }
        }
        transactionsManager.commit(tx2, this.endUser);

        // 3. Query and Verify
        SimpleVastTransaction tx3 = transactionsManager.startTransaction(this.endUser);
        VastTraceToken token = new VastTraceToken(Optional.of(format("%s/%s", schemaName, tableName)), tx3.getId(), 3);
        final AtomicReference<QueryDataResponseParser> result = new AtomicReference<>();
        try (RootAllocator queryAllocator = new RootAllocator()) {
            Supplier<QueryDataResponseHandler> handlerSupplier = () -> {
                ArrowQueryDataSchemaHelper schemaHelper = ArrowQueryDataSchemaHelper.deconstruct(token, arrowSchema, new EmptyVectorAdaptorFactory());
                QueryDataResponseParser parser = new QueryDataResponseParser(token, schemaHelper, VastDebugConfig.DEFAULT, new QueryDataPagination(1), Optional.empty(), queryAllocator);
                result.set(parser);
                return new QueryDataResponseHandler(parser::parse, token);
            };

            ProjectionSerializer projections = new ProjectionSerializer(arrowSchema, new EnumeratedSchema(fields));

            client.queryData(tx3, token, schemaName, tableName, arrowSchema, projections,
                    new EmptyPredicateSerializer(), handlerSupplier, null,
                    new VastSplitContext(0, 1, 1, 1), null, ImmutableList.of(this.endpoint),
                    new VastRetryConfig(3, 3), Optional.empty(), Optional.empty(),
                    new QueryDataPagination(1), false, 0, Collections.emptyMap(), this.endUser);

            // 4. Assert results
            QueryDataResponseParser parser = result.get();
            try (VectorSchemaRoot queryResultRoot = parser.next()) {
                assertEquals(queryResultRoot.getRowCount(), rowCount, "Unexpected number of rows returned");
                for (int colIdx = 0; colIdx < numCols; colIdx++) {
                    IntVector col = (IntVector) queryResultRoot.getVector(format("col%d", colIdx));
                    for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                        assertFalse(col.isNull(rowIdx), format("Value at col%d, row%d should not be null", colIdx, rowIdx));
                        assertEquals(col.get(rowIdx), rowIdx, format("Mismatch at col%d, row%d", colIdx, rowIdx));
                    }
                }
            }
        }
        finally {
            transactionsManager.commit(tx3, this.endUser);
        }
    }

    @Test
    public void testInsertByColumnWithSorted()
            throws VastException
    {
        String tableName = "wide-rows-sorted-table";
        List<String> sortedBy = ImmutableList.of("col1", "col100", "col1000");
        testInsertAndQueryWideTable(tableName, sortedBy, TableType.SORTED);
    }

    @Test
    public void testInsertByColumnWithNotSorted()
            throws VastException
    {
        String tableName = "wide-rows-unsorted-table";
        testInsertAndQueryWideTable(tableName, Collections.emptyList(), TableType.REGULAR);
    }
}
