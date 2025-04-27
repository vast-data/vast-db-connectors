package com.vastdata.vdb;

import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.schema.ArrowSchemaUtils;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.schema.VastMetadataUtils;
import com.vastdata.client.tx.SimpleVastTransaction;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import com.vastdata.vdb.sdk.NoExternalRowIdColumnException;
import com.vastdata.vdb.sdk.Table;
import com.vastdata.vdb.sdk.VastSdk;
import com.vastdata.vdb.sdk.VastSdkConfig;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIntegration
{
    static String schemaName = "vastdb/s2";
    static Field x = new Field("x", FieldType.nullable(new ArrowType.Utf8()), null);
    static Field y = new Field("y", FieldType.nullable(new ArrowType.Utf8()), null);

    private VastSdk sdk;

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

        URI uri = URI.create(endpoint);

        VastSdkConfig config = new VastSdkConfig(uri,
                uri.toString(),
                awsAccessKeyId,
                awsSecretAccessKey);

        HttpClient httpClient = new JettyHttpClient();

        this.sdk = new VastSdk(httpClient, config);

        VastClient client = this.sdk.getVastClient();

        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        SimpleVastTransaction tx = transactionsManager.startTransaction(new StartTransactionContext(false, false));

        client.createSchema(tx, schemaName, new VastMetadataUtils().getPropertiesString(Collections.emptyMap()));

        transactionsManager.commit(tx);

        System.out.println("Database connection established.");
    }

    @Test(expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = ".*VastUserException: Failed to execute request LIST_COLUMNS, path /vastdb/s/non-existent-table not found.*")
    public void testNonExistingTable()
            throws NoExternalRowIdColumnException, RuntimeException
    {
        Table table = sdk.getTable("vastdb/s", "non-existent-table");
        table.loadSchema();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*VastUserException: Failed to execute request LIST_COLUMNS, path /vastdb/non-existent-schema/non-existent-table not found.*")
    public void testNonExistingSchema()
            throws NoExternalRowIdColumnException, RuntimeException
    {
        Table table = sdk.getTable("vastdb/non-existent-schema", "non-existent-table");
        table.loadSchema();
    }

    @Test
    public void testRowsReturningSanity()
            throws VastException
    {
        VastClient client = sdk.getVastClient();
        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        SimpleVastTransaction tx = transactionsManager.startTransaction(new StartTransactionContext(false, false));
        client.createTable(tx, new CreateTableContext(
                schemaName, "tab1", List.of(ArrowSchemaUtils.VASTDB_ROW_ID_FIELD, x, y),
                null, null));
        transactionsManager.commit(tx);

        Table table = sdk.getTable(schemaName, "tab1");
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        VarCharVector xVector = new VarCharVector("x", allocator);
        xVector.allocateNew(2);
        xVector.set(0, new Text("x0"));
        xVector.set(1, new Text("x1"));
        xVector.setValueCount(2);

        VarCharVector yVector = new VarCharVector("y", allocator);
        yVector.allocateNew(2);
        yVector.set(0, new Text("y0"));
        yVector.set(1, new Text("y1"));
        yVector.setValueCount(2);

        ArrayList<FieldVector> fieldVectors = new ArrayList<>();
        fieldVectors.add(xVector);
        fieldVectors.add(yVector);

        VectorSchemaRoot vsr = new VectorSchemaRoot(table.getSchema(), fieldVectors, 2);
        final VectorSchemaRoot insertedRowIds = table.put(vsr);

        final UInt8Vector vec = (UInt8Vector) insertedRowIds.getVector(0);

        Assert.assertEquals(vec.get(0), 0);
        Assert.assertEquals(vec.get(1), 1);

        VectorSchemaRoot row0 = table.get(null, 0);
        VectorSchemaRoot row1 = table.get(null, 1);

        VarCharVector x0 = (VarCharVector) row0.getVector("x");
        VarCharVector x1 = (VarCharVector) row1.getVector("x");
        VarCharVector y0 = (VarCharVector) row0.getVector("y");
        VarCharVector y1 = (VarCharVector) row1.getVector("y");

        Assert.assertEquals(x0.getObject(0).toString(), "x0");
        Assert.assertEquals(x1.getObject(0).toString(), "x1");
        Assert.assertEquals(y0.getObject(0).toString(), "y0");
        Assert.assertEquals(y1.getObject(0).toString(), "y1");
    }

    @Test
    public void testRowsReturningTwoPuts()
            throws VastException
    {
        VastClient client = sdk.getVastClient();
        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        SimpleVastTransaction tx = transactionsManager.startTransaction(new StartTransactionContext(false, false));
        client.createTable(tx, new CreateTableContext(
                schemaName, "tab2", List.of(ArrowSchemaUtils.VASTDB_ROW_ID_FIELD, x, y),
                null, null));
        transactionsManager.commit(tx);

        Table table = sdk.getTable(schemaName, "tab2");
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        VarCharVector xVector = new VarCharVector("x", allocator);
        xVector.allocateNew(2);
        xVector.set(0, "x0".getBytes());
        xVector.set(1, "x1".getBytes());
        xVector.setValueCount(2);

        VarCharVector yVector = new VarCharVector("y", allocator);
        yVector.allocateNew(2);
        yVector.set(0, "y0".getBytes());
        yVector.set(1, "y1".getBytes());
        yVector.setValueCount(2);

        ArrayList<FieldVector> fieldVectors = new ArrayList<>();
        fieldVectors.add(xVector);
        fieldVectors.add(yVector);

        VectorSchemaRoot vsr = new VectorSchemaRoot(table.getSchema(), fieldVectors, 2);
        final VectorSchemaRoot insertedRowIds1 = table.put(vsr);

        final UInt8Vector vec1 = (UInt8Vector) insertedRowIds1.getVector(0);

        Assert.assertEquals(vec1.get(0), 0);
        Assert.assertEquals(vec1.get(1), 1);

        final VectorSchemaRoot insertedRowIds2 = table.put(vsr);

        final UInt8Vector vec2 = (UInt8Vector) insertedRowIds2.getVector(0);

        Assert.assertEquals(vec2.get(0), 2);
        Assert.assertEquals(vec2.get(1), 3);
    }

    @DataProvider(name = "testRowsInsertedData")
    public Object[] testRowsInsertedData()
    {
        return new Object[] {1, 2, 10, 50, 100};
    }

    @Test(dataProvider = "testRowsInsertedData")
    public void testRowsInserted(Integer nRows)
            throws VastException
    {
        VastClient client = sdk.getVastClient();
        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        SimpleVastTransaction tx = transactionsManager.startTransaction(new StartTransactionContext(false, false));
        client.createTable(tx, new CreateTableContext(
                schemaName, "test" + nRows + "RowInserted", List.of(ArrowSchemaUtils.VASTDB_ROW_ID_FIELD, x, y),
                null, null));
        transactionsManager.commit(tx);

        Table table = sdk.getTable(schemaName, "test" + nRows + "RowInserted");
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        VarCharVector xVector = new VarCharVector("x", allocator);
        xVector.allocateNew(nRows);
        for (int i = 0; i < nRows; i++) {
            xVector.set(i, ("x" + i).getBytes());
        }
        xVector.setValueCount(nRows);

        VarCharVector yVector = new VarCharVector("y", allocator);
        yVector.allocateNew(nRows);
        for (int i = 0; i < nRows; i++) {
            yVector.set(i, ("y" + i).getBytes());
        }
        yVector.setValueCount(nRows);

        ArrayList<FieldVector> fieldVectors = new ArrayList<>();
        fieldVectors.add(xVector);
        fieldVectors.add(yVector);

        VectorSchemaRoot vsr = new VectorSchemaRoot(table.getSchema(), fieldVectors, nRows);

        table.put(vsr);

        for (int i = 0; i < nRows; i++) {
            VectorSchemaRoot res = table.get(null, i);

            Assert.assertEquals(res.getRowCount(), 1);
            VarCharVector xs = (VarCharVector) res.getVector("x");
            VarCharVector ys = (VarCharVector) res.getVector("y");

            Assert.assertEquals(xs.get(0), ("x" + i).getBytes());
            Assert.assertEquals(ys.get(0), ("y" + i).getBytes());
        }
    }

    @Test
    public void testNested1()
            throws VastException
    {
        VastClient client = sdk.getVastClient();
        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        SimpleVastTransaction tx = transactionsManager.startTransaction(new StartTransactionContext(false, false));
        client.createTable(tx, new CreateTableContext(
                schemaName, "nested1",
                List.of(ArrowSchemaUtils.VASTDB_ROW_ID_FIELD,
                        new Field("str", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("nested", FieldType.nullable(new ArrowType.List()),
                                List.of(
                                        new Field("item", FieldType.nullable(new ArrowType.Struct()),
                                                List.of(
                                                        new Field("a", FieldType.nullable(new ArrowType.Int(16, true)), null),
                                                        new Field("b", FieldType.nullable(new ArrowType.Int(32, true)), null),
                                                        new Field("c", FieldType.nullable(new ArrowType.Int(64, true)), null),
                                                        new Field("d", FieldType.nullable(new ArrowType.Utf8()), null)))))),
                null, null));
        transactionsManager.commit(tx);

        Table table = sdk.getTable(schemaName, "nested1");
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        VarCharVector stringVector = (VarCharVector) root.getVector("str");
        ListVector nestedListVector = (ListVector) root.getVector("nested");
        StructVector nestedStructVector = (StructVector) nestedListVector.getDataVector();

        root.setRowCount(1);

        stringVector.setSafe(0, "somevalue".getBytes());

        UnionListWriter listWriter = nestedListVector.getWriter();
        listWriter.startList();
        BaseWriter.StructWriter structWriter = listWriter.struct();

        structWriter.start();
        structWriter.smallInt("a").writeSmallInt((short) 1);
        structWriter.integer("b").writeInt(2);
        structWriter.bigInt("c").writeBigInt(3);
        ArrowBuf buffer = allocator.buffer(10);
        buffer.writeBytes("helloworld".getBytes());
        structWriter.varChar("d").writeVarChar(0, 10, buffer);
        structWriter.end();
        nestedStructVector.setValueCount(1);
        listWriter.endList();
        listWriter.setPosition(1);

        table.put(root);

        VectorSchemaRoot res = table.get(null, 0);

        VarCharVector strVector = (VarCharVector) res.getVector("str");
        ListVector nestedListVectorRes = (ListVector) res.getVector("nested");

        StructVector nestedStructVectorRes = (StructVector) nestedListVectorRes.getDataVector();
        SmallIntVector aVector = (SmallIntVector) nestedStructVectorRes.getChild("a");
        IntVector bVector = (IntVector) nestedStructVectorRes.getChild("b");
        BigIntVector cVector = (BigIntVector) nestedStructVectorRes.getChild("c");
        VarCharVector dVector = (VarCharVector) nestedStructVectorRes.getChild("d");

        Assert.assertEquals(strVector.get(0), "somevalue".getBytes());
        Assert.assertEquals(aVector.get(0), (short) 1);
        Assert.assertEquals(bVector.get(0), 2);
        Assert.assertEquals(cVector.get(0), 3);
        Assert.assertEquals(dVector.get(0), "helloworld".getBytes());
    }

    @Test(expectedExceptions = NoExternalRowIdColumnException.class)
    public void testNoExternalRowIdColumn()
            throws VastException
    {

        VastClient client = sdk.getVastClient();
        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        SimpleVastTransaction tx = transactionsManager.startTransaction(new StartTransactionContext(false, false));
        client.createTable(tx, new CreateTableContext(
                schemaName, "no-external-rowid-column", List.of(x),
                null, null));
        transactionsManager.commit(tx);

        Table table = sdk.getTable(schemaName, "no-external-rowid-column");
        table.loadSchema();
    }

    static String createSingleColumnTable(VastClient client, String schema, String table, ArrowType type)
            throws VastException
    {
        String columnName = "col";

        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        SimpleVastTransaction tx = transactionsManager.startTransaction(new StartTransactionContext(false, false));
        client.createTable(tx, new CreateTableContext(schema,
                table,
                List.of(ArrowSchemaUtils.VASTDB_ROW_ID_FIELD,
                        new Field(columnName, FieldType.nullable(type), null)), null, null));
        transactionsManager.commit(tx);
        return columnName;
    }

    @Test
    public void testInsertBool()
            throws VastException
    {
        String tableName = "types_bool";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Bool());

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        BitVector boolVector = (BitVector) root.getVector(columnName);
        root.setRowCount(1);
        boolVector.setSafe(0, 1);
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((BitVector) result.getVector(columnName)).get(0), 1);
    }

    @Test
    public void testInsertInt8()
            throws VastException
    {
        String tableName = "types_int8";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Int(8, true));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        TinyIntVector int8Vector = (TinyIntVector) root.getVector(columnName);
        root.setRowCount(1);
        int8Vector.setSafe(0, (byte) 127);
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((TinyIntVector) result.getVector(columnName)).get(0), 127);
    }

    @Test
    public void testInsertInt16()
            throws VastException
    {
        String tableName = "types_int16";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Int(16, true));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        SmallIntVector int16Vector = (SmallIntVector) root.getVector(columnName);
        root.setRowCount(1);
        int16Vector.setSafe(0, (short) 32767);
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((SmallIntVector) result.getVector(columnName)).get(0), 32767);
    }

    @Test
    public void testInsertInt32()
            throws VastException
    {
        String tableName = "types_int32";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Int(32, true));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        IntVector int32Vector = (IntVector) root.getVector(columnName);
        root.setRowCount(1);
        int32Vector.setSafe(0, 2147483647);
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((IntVector) result.getVector(columnName)).get(0), 2147483647);
    }

    @Test
    public void testInsertInt64()
            throws VastException
    {
        String tableName = "types_int64";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Int(64, true));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        BigIntVector int64Vector = (BigIntVector) root.getVector(columnName);
        root.setRowCount(1);
        int64Vector.setSafe(0, 9223372036854775807L);
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((BigIntVector) result.getVector(columnName)).get(0), 9223372036854775807L);
    }

    @Test
    public void testInsertUInt8()
            throws VastException
    {
        String tableName = "types_uint8";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Int(8, false));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        UInt1Vector uint8Vector = (UInt1Vector) root.getVector(columnName);
        root.setRowCount(1);
        uint8Vector.setSafe(0, 255);
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals((short) ((UInt1Vector) result.getVector(columnName)).getObjectNoOverflow(0), (short) 255);
    }

    @Test
    public void testInsertUInt16()
            throws VastException
    {
        String tableName = "types_uint16";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Int(16, false));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        UInt2Vector uint16Vector = (UInt2Vector) root.getVector(columnName);
        root.setRowCount(1);
        uint16Vector.setSafe(0, 65535);
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((UInt2Vector) result.getVector(columnName)).get(0), UInt2Vector.MAX_UINT2);
    }

    @Test
    public void testInsertUInt32()
            throws VastException
    {
        String tableName = "types_uint32";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Int(32, false));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        UInt4Vector uint32Vector = (UInt4Vector) root.getVector(columnName);
        root.setRowCount(1);
        uint32Vector.setSafe(0, UInt4Vector.MAX_UINT4);
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals((long) ((UInt4Vector) result.getVector(columnName)).getObjectNoOverflow(0), Integer.toUnsignedLong(UInt4Vector.MAX_UINT4));
    }

    @Test
    public void testInsertUInt64()
            throws VastException
    {
        String tableName = "types_uint64";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Int(64, false));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        UInt8Vector uint64Vector = (UInt8Vector) root.getVector(columnName);
        root.setRowCount(1);
        uint64Vector.setSafe(0, UInt8Vector.MAX_UINT8);
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((UInt8Vector) result.getVector(columnName)).get(0), UInt8Vector.MAX_UINT8);
    }

    @Test
    public void testInsertFloat32()
            throws VastException
    {
        String tableName = "types_float32";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        Float4Vector float32Vector = (Float4Vector) root.getVector(columnName);
        root.setRowCount(1);
        float32Vector.setSafe(0, 3.14f);
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((Float4Vector) result.getVector(columnName)).get(0), 3.14f);
    }

    @Test
    public void testInsertFloat64()
            throws VastException
    {
        String tableName = "types_float64";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        Float8Vector float64Vector = (Float8Vector) root.getVector(columnName);
        root.setRowCount(1);
        float64Vector.setSafe(0, 3.14159265359);
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((Float8Vector) result.getVector(columnName)).get(0), 3.14159265359);
    }

    @Test
    public void testInsertDecimal128()
            throws VastException
    {
        String tableName = "types_decimal128";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Decimal(12, 2));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        DecimalVector decimal128Vector = (DecimalVector) root.getVector(columnName);
        root.setRowCount(1);
        decimal128Vector.set(0, new java.math.BigDecimal("12345.67").unscaledValue().longValue());
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((DecimalVector) result.getVector(columnName)).getObject(0), new java.math.BigDecimal("12345.67"));
    }

    @Test
    public void testInsertTimestampSecond()
            throws VastException
    {
        String tableName = "types_timestamp_second";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Timestamp(TimeUnit.SECOND, null));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        TimeStampSecVector timestampSecondVector = (TimeStampSecVector) root.getVector(columnName);
        root.setRowCount(1);
        timestampSecondVector.setSafe(0, 1672531200L); // 2023-01-01T00:00:00Z
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((TimeStampSecVector) result.getVector(columnName)).get(0), 1672531200L);
    }

    @Test
    public void testInsertTimestampMillisecond()
            throws VastException
    {
        String tableName = "types_timestamp_millisecond";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        TimeStampMilliVector timestampMilliVector = (TimeStampMilliVector) root.getVector(columnName);
        root.setRowCount(1);
        timestampMilliVector.setSafe(0, 1672531200000L); // 2023-01-01T00:00:00Z in ms
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((TimeStampMilliVector) result.getVector(columnName)).get(0), 1672531200000L);
    }

    @Test
    public void testInsertTimestampMicrosecond()
            throws VastException
    {
        String tableName = "types_timestamp_microsecond";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        TimeStampMicroVector timestampMicroVector = (TimeStampMicroVector) root.getVector(columnName);
        root.setRowCount(1);
        timestampMicroVector.setSafe(0, 1672531200000000L); // in µs
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((TimeStampMicroVector) result.getVector(columnName)).get(0), 1672531200000000L);
    }

    @Test
    public void testInsertTimestampNanosecond()
            throws VastException
    {
        String tableName = "types_timestamp_nanosecond";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Timestamp(TimeUnit.NANOSECOND, null));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        TimeStampNanoVector timestampNanoVector = (TimeStampNanoVector) root.getVector(columnName);
        root.setRowCount(1);
        timestampNanoVector.setSafe(0, 1672531200000000000L); // in ns
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((TimeStampNanoVector) result.getVector(columnName)).get(0), 1672531200000000000L);
    }

    @Test
    public void testInsertTime64Microsecond()
            throws VastException
    {
        String tableName = "types_time64_microsecond";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Time(TimeUnit.MICROSECOND, 64));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        TimeMicroVector time64MicroVector = (TimeMicroVector) root.getVector(columnName);
        root.setRowCount(1);
        time64MicroVector.setSafe(0, 3600000000L); // 1 hour in µs
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((TimeMicroVector) result.getVector(columnName)).get(0), 3600000000L);
    }

    @Test
    public void testInsertDate32()
            throws VastException
    {
        String tableName = "types_date32";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Date(DateUnit.DAY));

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        DateDayVector date32Vector = (DateDayVector) root.getVector(columnName);
        root.setRowCount(1);
        date32Vector.setSafe(0, 19358); // 2023-01-01
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(((DateDayVector) result.getVector(columnName)).get(0), 19358);
    }

    @Test
    public void testInsertString()
            throws VastException
    {
        String tableName = "types_string";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Utf8());

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarCharVector stringVector = (VarCharVector) root.getVector(columnName);
        root.setRowCount(1);
        stringVector.setSafe(0, "hello".getBytes());
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(new String(((VarCharVector) result.getVector(columnName)).get(0)), "hello");
    }

    @Test
    public void testInsertBinary()
            throws VastException
    {
        String tableName = "types_binary";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Binary());

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarBinaryVector binaryVector = (VarBinaryVector) root.getVector(columnName);
        root.setRowCount(1);
        binaryVector.setSafe(0, "binarydata".getBytes());
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(new String(((VarBinaryVector) result.getVector(columnName)).get(0)), "binarydata");
    }

    @Test
    public void testInsertUtf8()
            throws VastException
    {
        String tableName = "types_utf8";
        String columnName = createSingleColumnTable(sdk.getVastClient(), schemaName, tableName, new ArrowType.Utf8());

        Table table = sdk.getTable(schemaName, tableName);
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarCharVector utf8Vector = (VarCharVector) root.getVector(columnName);
        root.setRowCount(1);
        utf8Vector.setSafe(0, "hello-utf8".getBytes());
        table.put(root);

        VectorSchemaRoot result = table.get(null, 0);
        Assert.assertEquals(new String(((VarCharVector) result.getVector(columnName)).get(0)), "hello-utf8");
    }

    @Test
    public void testTimeTypes()
            throws VastException
    {
        VastClient client = sdk.getVastClient();
        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        SimpleVastTransaction tx = transactionsManager.startTransaction(new StartTransactionContext(false, false));
        client.createTable(tx, new CreateTableContext(
                schemaName, "time_types",
                List.of(ArrowSchemaUtils.VASTDB_ROW_ID_FIELD,
                        new Field("time32_second", FieldType.nullable(new ArrowType.Time(TimeUnit.SECOND, 32)), null),
                        new Field("time32_millisecond", FieldType.nullable(new ArrowType.Time(TimeUnit.MILLISECOND, 32)), null),
                        new Field("time64_nanosecond", FieldType.nullable(new ArrowType.Time(TimeUnit.NANOSECOND, 64)), null)),
                null, null));
        transactionsManager.commit(tx);

        Table table = sdk.getTable(schemaName, "time_types");
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        TimeSecVector timeSecond = (TimeSecVector) root.getVector("time32_second");
        TimeMilliVector timeMilli = (TimeMilliVector) root.getVector("time32_millisecond");
        TimeNanoVector timeNano = (TimeNanoVector) root.getVector("time64_nanosecond");
        root.setRowCount(1);

        // Insert values
        timeSecond.setSafe(0, 86399);
        timeMilli.setSafe(0, 86399999);
        timeNano.setSafe(0, 86399999999L);

        table.put(root);

        // Query and validate values
        VectorSchemaRoot result = table.get(null, 0);

        Assert.assertEquals(((TimeSecVector) result.getVector("time32_second")).get(0), 86399);
        Assert.assertEquals(((TimeMilliVector) result.getVector("time32_millisecond")).get(0), 86399999);
        Assert.assertEquals(((TimeNanoVector) result.getVector("time64_nanosecond")).get(0), 86399999999L);
    }

    @Test
    public void testNulls()
            throws VastException
    {
        VastClient client = sdk.getVastClient();
        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        SimpleVastTransaction tx = transactionsManager.startTransaction(new StartTransactionContext(false, false));
        client.createTable(tx, new CreateTableContext(
                schemaName, "nulls",
                List.of(ArrowSchemaUtils.VASTDB_ROW_ID_FIELD,
                        new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null),
                        new Field("y", FieldType.nullable(new ArrowType.Int(32, true)), null)),
                null, null));
        transactionsManager.commit(tx);

        Table table = sdk.getTable(schemaName, "nulls");
        table.loadSchema();

        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(table.getSchema().getFields());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        IntVector x = (IntVector) root.getVector("x");
        IntVector y = (IntVector) root.getVector("y");
        root.setRowCount(4);

        // Insert values
        x.setSafe(0, 1);
        y.setSafe(0, 10);

        x.setSafe(1, 2);
        y.setNull(1);

        x.setSafe(2, 3);
        y.setSafe(2, 30);

        x.setNull(3);
        y.setNull(3);

        table.put(root);

        // Query and validate values
        VectorSchemaRoot row0 = table.get(null, 0);

        Assert.assertEquals(((IntVector) row0.getVector("x")).get(0), 1);
        Assert.assertEquals(((IntVector) row0.getVector("y")).get(0), 10);

        VectorSchemaRoot row1 = table.get(null, 1);

        Assert.assertEquals(((IntVector) row1.getVector("x")).get(0), 2);
        Assert.assertTrue(((IntVector) row1.getVector("y")).isNull(0));

        VectorSchemaRoot row2 = table.get(null, 2);

        Assert.assertEquals(((IntVector) row2.getVector("x")).get(0), 3);
        Assert.assertEquals(((IntVector) row2.getVector("y")).get(0), 30);

        VectorSchemaRoot row3 = table.get(null, 3);

        Assert.assertTrue(((IntVector) row3.getVector("x")).isNull(0));
        Assert.assertTrue(((IntVector) row3.getVector("y")).isNull(0));
    }
}
