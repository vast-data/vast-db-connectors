/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.amazonaws.http.HttpMethodName;
import com.google.common.collect.ImmutableSet;
import com.sun.net.httpserver.HttpExchange;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastConflictException;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.stats.VastStatistics;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.client.tx.ka.JobEventService;
import com.vastdata.mockserver.MockMapSchema;
import com.vastdata.mockserver.MockUtils;
import com.vastdata.mockserver.VastMockS3Server;
import com.vastdata.mockserver.VastRootHandler;
import com.vastdata.spark.SparkTestUtils;
import com.vastdata.spark.VastArrowAllocator;
import com.vastdata.spark.VastTable;
import com.vastdata.spark.statistics.SparkPersistentStatistics;
import com.vastdata.spark.statistics.SparkVastStatisticsManager;
import com.vastdata.spark.statistics.SparkVastStatisticsManagerTestUtil;
import ndb.NDB;
import ndb.ka.NDBJobsListener;
import org.apache.spark.scheduler.SparkListenerInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeMap;
import org.apache.spark.sql.catalyst.expressions.AttributeMap$;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.AttributeSet;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.FilterExec;
import org.apache.spark.sql.execution.ProjectExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.mockito.Mock;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Seq;
import scala.collection.mutable.Builder;
import scala.math.BigInt;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.amazonaws.http.HttpMethodName.DELETE;
import static com.amazonaws.http.HttpMethodName.GET;
import static com.amazonaws.http.HttpMethodName.POST;
import static com.amazonaws.http.HttpMethodName.PUT;
import static com.vastdata.client.VastClient.AUDIT_LOG_BUCKET_NAME;
import static com.vastdata.client.VastClient.BIG_CATALOG_BUCKET_NAME;
import static java.lang.String.format;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestVastCatalog
{
    private static VastMockS3Server mockServer;
    @Mock private VastClient mockClient;
    private static final VastRootHandler handler = new VastRootHandler();
    private int testPort;
    @Mock VastTransaction mockTransactionHandle;
    private AutoCloseable autoCloseable;

    @BeforeClass
    public void startMockServer()
            throws IOException
    {
        NDB.clearConfig();
        SparkVastStatisticsManagerTestUtil.initInMemoryStatsInstance();
        mockServer = new VastMockS3Server(0, handler);
        testPort = mockServer.start();
    }

    @AfterClass
    public void stopServer()
            throws Exception
    {
        if (Objects.nonNull(mockServer)) {
            mockServer.close();
        }
        autoCloseable.close();
    }

    @BeforeMethod
    public void clearMockServer()
    {
        Map<String, Set<MockMapSchema>> testMockServerSchema = new HashMap<>(1);
        testMockServerSchema.put(AUDIT_LOG_BUCKET_NAME, ImmutableSet.of());
        testMockServerSchema.put(BIG_CATALOG_BUCKET_NAME, ImmutableSet.of());
        handler.setSchema(testMockServerSchema);
        autoCloseable = openMocks(this);
        when(mockTransactionHandle.getId()).thenReturn(Long.parseUnsignedLong("514026084031791104"));
    }

    @Test
    public void testAlterTableAddColumn()
            throws IOException, NoSuchTableException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem");
            session.sql("create table ndb.buck.schem.tab (b boolean, i integer)");
            VastCatalog unit = new VastCatalog();
            unit.initialize("", CaseInsensitiveStringMap.empty());
            Identifier tableIdent = Identifier.of(new String[] {"buck", "schem"}, "tab");
            String[] colName = new String[] {"s"};
            DataType colType = DataTypes.StringType;
            TableChange addColumn = TableChange.addColumn(colName, colType);
            Table tableAfterChange = unit.alterTable(tableIdent, addColumn);
            StructType schema = tableAfterChange.schema();
            StructType expectedSchema = new StructType(new StructField[] {
                    createStructField("b", DataTypes.BooleanType, true),
                    createStructField("i", DataTypes.IntegerType, true),
                    createStructField("s", DataTypes.StringType, true),
            });
            assertEquals(schema, expectedSchema);
        }
    }

    @Test
    public void testShowColumns()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab " +
                    "(b boolean, i integer, r STRUCT<a: INTEGER, b: STRING>, v varchar(30), c char(40), d DATE, t timestamp)").show();
            session.sql("show columns from ndb.buck.schem.tab").show();
            session.sql("insert into ndb.buck.schem.tab(b, i, r, v, c, d, t) values " +
                    "(FALSE, 321, (3, 'structstr'), 'varcharstr', 'charstr', date '2008-11-11', timestamp '2008-11-09 15:45:21')").show();
            session.sql("show schemas from ndb.buck.schem").show();
        }
    }

    @Test
    public void testTransactions()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            try {
                session.sql("select ndb.create_tx()").show();
                fail("expected failure - has already open transaction");
            }
            catch (Exception ignored) {
            }
            session.sql("select ndb.commit_tx()").show();
            try {
                session.sql("select ndb.commit_tx()").show();
                fail("expected failure - no open transaction");
            }
            catch (Exception ignored) {
            }
            try {
                session.sql("select ndb.rollback_tx()").show();
                fail("expected failure - no open transaction");
            }
            catch (Exception ignored) {
            }
            session.sql("select ndb.create_tx()").show();
            session.sql("select ndb.rollback_tx()").show();
            try {
                session.sql("select ndb.no_such_function()").show();
                fail("expected failure - no such function exist");
            }
            catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testTransactionsCommandsErrors()
    {
        String message = "Some bad request";
        Consumer<HttpExchange> action = httpExchange -> {
            try {
                httpExchange.sendResponseHeaders(400, message.length());
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(message.getBytes(StandardCharsets.UTF_8));
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        handler.setHook("/", PUT, action);
        handler.setHook("/", DELETE, action);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            assertTransactionCommandError(session, "select ndb.commit_tx()");
            assertTransactionCommandError(session, "select ndb.rollback_tx()");
        }
    }

    private static void assertTransactionCommandError(SparkSession session, String sqlText)
    {
        session.sql("select ndb.create_tx()").show();
        try {
            session.sql(sqlText).show();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(session.sparkContext().executorEnvs().get("tx").isEmpty());
    }

    @Test(enabled = false)
    public void testImportDataViaInsert()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            try {
                session.sql("create table ndb.buck.schem.`tab vast.import_data`(b boolean, i integer)").show();
                fail("expected failure - creating vast.import_data");
            }
            catch (Exception ignored) {
            }
            session.sql("create table ndb.buck.schem.tab(b boolean, i integer)").show();
            session.sql("insert into ndb.buck.schem.tab(b, i) values (FALSE, 321)").show();
            String valuesStr = "(TRUE, 123, 'file1/file')";
            String sql = format("insert into ndb.buck.schem.`tab vast.import_data(b, i)` " +
                    "(b, i, `$parquet_file_path`) values %s", valuesStr);
            System.out.println(sql);
            session.sql(sql).show();
        }
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testNotNullColumnFails()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab (b boolean, i integer not null)").show();
        }
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = TypeUtil.NDB_CATALOG_DOES_NOT_SUPPORT_TYPES +
            " \\[i: Interval\\(YEAR_MONTH\\), d: Duration\\(MICROSECOND\\)]")
    public void testUnsupportedColumns()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab (b boolean, i interval year, d interval second)").show();
        }
    }

    @Test(enabled = false)
    public void testInsertDf()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab (a string, b integer, c double)").show();
            Dataset<Row> df = session.sql("select * from ndb.buck.schem.tab").select("*");
            df.writeTo("ndb.buck.schem.tab2").create();
            session.sql("show tables from ndb.buck.schem").show();
            session.sql("select ndb.commit_tx()").show();
        }
        finally {
            assertEquals(VastArrowAllocator.writeAllocator().getAllocatedMemory(), 0);
        }
    }

    @Test(enabled = false)
    public void testInsert()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        int noOfVals = 120000;

        String[] valuesArray = IntStream.range(0, noOfVals).mapToObj(i -> format("(%s, %s, 'c%s', 's%s', %s.01)", i % 2 == 0, i, i, i, i)).toArray(String[]::new);
        String valuesStr = String.join(",", valuesArray);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab (b boolean, i integer, c char(20), s varchar(20), d decimal(10,2))").show();
            String format = format("insert into ndb.buck.schem.tab values %s", valuesStr);
            session.sql(format).show();
            String path = "buck/schem/tab";
            testGracefulVastException(session, format, path, POST, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidBucketState</Code><Message>The request is not valid with the current state of the bucket.</Message><Resource>aresource</Resource><RequestId>a00100000006</RequestId></Error>", 409, VastConflictException.class);
            testGracefulVastException(session, format, path, POST, "Forbidden", 403, VastUserException.class);
        }
        finally {
            assertEquals(VastArrowAllocator.writeAllocator().getAllocatedMemory(), 0);
        }
    }

    private static void testGracefulVastException(SparkSession session, String sql, String tablePath, HttpMethodName method, String message, int rc, Class<?> expectedException)
    {
        Consumer<HttpExchange> action = httpExchange -> {
            try {
                httpExchange.sendResponseHeaders(rc, message.length());
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(message.getBytes(StandardCharsets.UTF_8));
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        handler.setHook(format("/%s", tablePath), method, action);
        try {
            session.sql(sql).show();
            fail("Expected to fail");
        }
        catch (Throwable any) {
            Throwable cause = any.getCause();
            boolean foundCause = false;
            while (cause != null) {
                if (expectedException.isAssignableFrom(cause.getClass())) {
                    foundCause = true;
                    break;
                }
                cause = cause.getCause();
            }
            assertTrue(foundCause, format("Expected exception with cause of type %s, but got %s", expectedException.getSimpleName(), any));
        }
        finally {
            assertEquals(VastArrowAllocator.writeAllocator().getAllocatedMemory(), 0);
        }
    }

    @Test(enabled = false)
    public void testDropPartition()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab (b boolean, i integer)").show();
            session.sql("alter table ndb.buck.schem.tab drop partition (i = 5)").show();
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Row level delete is not supported for required filters.*")
    public void testDelete()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab (b boolean, i integer, s string, d decimal(10,2))").show();
            session.sql("delete from ndb.buck.schem.tab where i < 20 or b").explain(false);
        }
    }

    @Test
    public void testORION_120730()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab (i integer, b boolean)").show();
            testProjectOptimizationOutput(
                    session.sql("SELECT DISTINCT i + 1 from ndb.buck.schem.tab where i > 126 and b is not null").queryExecution().executedPlan(),
                    "(i + 1)", false, Optional.empty());

            testProjectOptimizationOutput(
                    session.sql("SELECT DISTINCT i as alias1 from ndb.buck.schem.tab where i > 126").queryExecution().executedPlan(),
                    "alias1", false, Optional.empty());
        }
    }

    @Test
    public void testORION_120730_withFilter()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab (i integer, b boolean)").show();
            Optional<Set<String>> filterColumnNames = Optional.of(ImmutableSet.of("i"));
            testProjectOptimizationOutput(
                    session.sql("SELECT DISTINCT i + 1 from ndb.buck.schem.tab where cos(i) > 0.3 and b is not null").queryExecution().executedPlan(),
                    "(i + 1)", true, filterColumnNames);

            testProjectOptimizationOutput(
                    session.sql("SELECT DISTINCT i as alias1 from ndb.buck.schem.tab where cos(i) > 0.3").queryExecution().executedPlan(),
                    "alias1", true, filterColumnNames);
        }
    }

    private static void testProjectOptimizationOutput(SparkPlan sparkPlan, String expectedAttributeName, boolean expectFilter, Optional<Set<String>> filterColumnNames)
    {
        Seq<Attribute> finalOutput = sparkPlan.output();
        assertEquals(finalOutput.length(), 1);
        Attribute att = finalOutput.head();
        assertEquals(att.name(), expectedAttributeName);
        SparkPlan last = sparkPlan.collectLeaves().last();
        assertTrue(last instanceof AdaptiveSparkPlanExec, format("last was of class: %s", last.getClass()));
        AdaptiveSparkPlanExec a = (AdaptiveSparkPlanExec) last;
        SparkPlan project = a.inputPlan().children().head().children().head();
        assertTrue(project instanceof ProjectExec, format("Expected a project node, but node was of class: %s", last.getClass()));
        assertEquals(project.output(), finalOutput);
        SparkPlan projectChild = ((ProjectExec) project).child();
        if (expectFilter) {
            assertTrue(projectChild instanceof FilterExec, format("Expected a filter node, but node was of class: %s", projectChild.getClass()));
            if (filterColumnNames.isPresent()) {
                FilterExec filter = (FilterExec) projectChild;
                AttributeSet references = filter.condition().references();
                ImmutableSet.Builder<String> builder = ImmutableSet.builder();
                references.foreach(filterCol -> {
                    builder.add(filterCol.name());
                    return null;
                });
                assertEquals(builder.build(), filterColumnNames.get(), "Filtered columns do not match expectations");
            }
        }
        else {
            assertTrue(projectChild instanceof BatchScanExec, format("Expected a scan node, but node was of class: %s", projectChild.getClass()));
        }
        assertEquals(projectChild.output().length(), 1);
        assertEquals(projectChild.output().head().name(), "i");
    }

    @Test
    public void testInPostFilterPushdown() // tpcds query 8
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab (b boolean, i integer, s string, d decimal(10,2))").show();
            session.sql("select s from ndb.buck.schem.tab where substr(s, 1, 3) in ('aa', 'bb', 'cc')").explain("cost");
        }
    }

    @Test
    public void testPostfilterProjections()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem");
            session.sql("create table ndb.buck.schem.tab (b boolean, i integer, d double)");
            session.sql("select * from ndb.buck.schem.tab where i > 0").explain("cost");
            session.sql("select i as i_alias, b as b_alias from ndb.buck.schem.tab where i > 0").explain("cost");
            session.sql("select count(*) from ndb.buck.schem.tab where i > 0").explain("cost");
            session.sql("select i, i + 1, 0.0 as proj1, (i > 10 and b) as proj2 from ndb.buck.schem.tab where i > 0").explain("cost");
        }
    }

    @Test
    public void testPostfilterProjectionsMultiplePostfilterSameColumn()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem");
            session.sql("create table ndb.buck.schem.tab (b boolean, i integer, d double)");
            SparkPlan sparkPlan = session.sql("select i, i + 1, 0.0 as proj1, (i > 10 and b) as proj2 from ndb.buck.schem.tab where i > 0")
                    .queryExecution().executedPlan();
            SparkPlan last = sparkPlan.collectLeaves().last();
            assertTrue(last instanceof BatchScanExec, format("last was of class: %s", last.getClass()));
            scala.collection.Seq<Attribute> scanOutput = last.output();
            assertEquals(scanOutput.size(), 2, scanOutput.toString());
            HashSet<String> expectedNames = new HashSet<>();
            expectedNames.add("i");
            expectedNames.add("b");
            assertTrue(expectedNames.remove(scanOutput.apply(0).name()));
            assertTrue(expectedNames.remove(scanOutput.apply(1).name()));
            SparkPlan head = sparkPlan.children().head();
            while (!(head instanceof ProjectExec)) {
                head = head.children().head();
                if (head.children().isEmpty()) {
                    fail("Could not find project node in plan");
                }
            }
            ProjectExec project = (ProjectExec) head;
            scala.collection.Seq<Attribute> projectOutput = project.output();
            assertEquals(projectOutput.size(), 4);
        }
    }

    @Test
    public void testPostfilterProjectionsCountWithPostfilter()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem");
            session.sql("create table ndb.buck.schem.tab (b boolean, i integer, s string)");
            scala.collection.Seq<SparkPlan> sparkPlanSeq = session.sql("select count(*) from ndb.buck.schem.tab where (i%2 > 0 or b) and s is not null")
                    .queryExecution().executedPlan().collectLeaves();
            SparkPlan head = sparkPlanSeq.head();
            assertTrue(head instanceof AdaptiveSparkPlanExec);
            AdaptiveSparkPlanExec adaptiveSparkPlanExec = (AdaptiveSparkPlanExec) head;
            head = adaptiveSparkPlanExec.inputPlan();
            while (!(head.children().head() instanceof ProjectExec)) {
                head = head.children().head();
                if (head.children().isEmpty()) {
                    fail("Could not find project node in plan");
                }
            }
            ProjectExec project = (ProjectExec) head.children().head();
            assertEquals(project.output().size(), 0);
            SparkPlan child = project.child();
            assertTrue(child instanceof FilterExec, format("Expected FilterExec node, but child was actually: %s", child.getClass()));
            FilterExec filter = (FilterExec) child;
            child = filter.child();
            assertTrue(child instanceof BatchScanExec, format("Expected BatchScanExec node, but child was actually: %s", child.getClass()));
            scala.collection.Seq<Attribute> output = child.output();
            assertEquals(output.size(), 2);
            HashSet<String> expectedNames = new HashSet<>();
            expectedNames.add("i");
            expectedNames.add("b");
            assertTrue(expectedNames.remove(output.apply(0).name()));
            assertTrue(expectedNames.remove(output.apply(1).name()));
        }
    }

    @Test(expectedExceptions = VastRuntimeException.class, expectedExceptionsMessageRegExp = ".*QueryData.*")
    public void testDeleteCharNException() // ORION-107547
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem");
            session.sql("create table ndb.buck.schem.tab (b boolean, c char(5))");
            session.sql("delete from ndb.buck.schem.tab where c is not null").explain();
        }
    }

    @Test(enabled = false)
    public void testAndPredicate()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem");
            session.sql("create table ndb.buck.schem.tab (b boolean, i integer)");
            session.sql("select count(b) from ndb.buck.schem.tab where i = 0 or i > 1 and i <= 3 ").show();
        }
    }

    @Test
    public void testBinaryPredicate()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem");
            session.sql("create table ndb.buck.schem.tab (b BINARY)");
            session.sql("select * from ndb.buck.schem.tab where b = 'abcd'").explain("cost");
        }
    }

    @Test
    public void testPredicatePushdownOr()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem");
            session.sql("create table ndb.buck.schem.tab (a integer, b integer)");
            session.sql("select * from ndb.buck.schem.tab where a in (4, 7, 9) and b in (1, 3, 5)").explain("cost");
        }
    }

    @Test(invocationCount = 1)
    public void testPredicatePushdownTPCDS_Q41()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem");
            session.sql("create table ndb.buck.schem.item (i_product_name string, i_manufact_id integer, i_manufact string, i_size string, i_units string, i_color string, i_category string)");
            session.sql("SELECT DISTINCT i_product_name\n" +
                    "FROM\n" +
                    "  ndb.buck.schem.item i1\n" +
                    "WHERE (i_manufact_id BETWEEN 738 AND (738 + 40))\n" +
                    "   AND ((\n" +
                    "      SELECT count(*) item_cnt\n" +
                    "      FROM\n" +
                    "        ndb.buck.schem.item\n" +
                    "      WHERE ((i_manufact = i1.i_manufact)\n" +
                    "            AND (((i_category = 'Women')\n" +
                    "                  AND ((i_color = 'powder')\n" +
                    "                     OR (i_color = 'khaki'))\n" +
                    "                  AND ((i_units = 'Ounce')\n" +
                    "                     OR (i_units = 'Oz'))\n" +
                    "                  AND ((i_size = 'medium')\n" +
                    "                     OR (i_size = 'extra large')))\n" +
                    "               OR ((i_category = 'Women')\n" +
                    "                  AND ((i_color = 'brown')\n" +
                    "                     OR (i_color = 'honeydew'))\n" +
                    "                  AND ((i_units = 'Bunch')\n" +
                    "                     OR (i_units = 'Ton'))\n" +
                    "                  AND ((i_size = 'N/A')\n" +
                    "                     OR (i_size = 'small')))\n" +
                    "               OR ((i_category = 'Men')\n" +
                    "                  AND ((i_color = 'floral')\n" +
                    "                     OR (i_color = 'deep'))\n" +
                    "                  AND ((i_units = 'N/A')\n" +
                    "                     OR (i_units = 'Dozen'))\n" +
                    "                  AND ((i_size = 'petite')\n" +
                    "                     OR (i_size = 'large')))\n" +
                    "               OR ((i_category = 'Men')\n" +
                    "                  AND ((i_color = 'light')\n" +
                    "                     OR (i_color = 'cornflower'))\n" +
                    "                  AND ((i_units = 'Box')\n" +
                    "                     OR (i_units = 'Pound'))\n" +
                    "                  AND ((i_size = 'medium')\n" +
                    "                     OR (i_size = 'extra large')))))\n" +
                    "         OR ((i_manufact = i1.i_manufact)\n" +
                    "            AND (((i_category = 'Women')\n" +
                    "                  AND ((i_color = 'midnight')\n" +
                    "                     OR (i_color = 'snow'))\n" +
                    "                  AND ((i_units = 'Pallet')\n" +
                    "                     OR (i_units = 'Gross'))\n" +
                    "                  AND ((i_size = 'medium')\n" +
                    "                     OR (i_size = 'extra large')))\n" +
                    "               OR ((i_category = 'Women')\n" +
                    "                  AND ((i_color = 'cyan')\n" +
                    "                     OR (i_color = 'papaya'))\n" +
                    "                  AND ((i_units = 'Cup')\n" +
                    "                     OR (i_units = 'Dram'))\n" +
                    "                  AND ((i_size = 'N/A')\n" +
                    "                     OR (i_size = 'small')))\n" +
                    "               OR ((i_category = 'Men')\n" +
                    "                  AND ((i_color = 'orange')\n" +
                    "                     OR (i_color = 'frosted'))\n" +
                    "                  AND ((i_units = 'Each')\n" +
                    "                     OR (i_units = 'Tbl'))\n" +
                    "                  AND ((i_size = 'petite')\n" +
                    "                     OR (i_size = 'large')))\n" +
                    "               OR ((i_category = 'Men')\n" +
                    "                  AND ((i_color = 'forest')\n" +
                    "                     OR (i_color = 'ghost'))\n" +
                    "                  AND ((i_units = 'Lb')\n" +
                    "                     OR (i_units = 'Bundle'))\n" +
                    "                  AND ((i_size = 'medium')\n" +
                    "                     OR (i_size = 'extra large')))))\n" +
                    "   ) > 0)\n" +
                    "ORDER BY i_product_name ASC\n" +
                    "LIMIT 100").explain("cost");
        }
    }

    @Test
    public void testPushdownUntranslatable()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab (s string)").show();
            session.sql("select * from ndb.buck.schem.tab where instr(s, 'bla') > 0").explain("cost");
        }
    }

    @Test
    public void testPushdownPredicatesFromPlan()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab (x integer)").show();
            String nodePlan = session.sql("select * from ndb.buck.schem.tab where x > 5").queryExecution().executedPlan()
                    .collectLeaves().head().toString();
            assertTrue(nodePlan.contains("x > 5"));
            nodePlan = session.sql("select * from ndb.buck.schem.tab where x in (1, 2, 3)").queryExecution().executedPlan()
                    .collectLeaves().head().toString();
            assertTrue(nodePlan.contains("x = 1") && nodePlan.contains("x = 2") && nodePlan.contains("x = 3"));
        }
    }

    @Test
    public void testStatisticsInjection()
            throws IOException
    {
        StructField x1 = new StructField("x1", IntegerType$.MODULE$, true, Metadata.empty());
        StructField x2 = new StructField("x2", IntegerType$.MODULE$, true, Metadata.empty());
        StructType schema1 = new StructType(new StructField[] {x1, x2});
        VastTable t1 = new VastTable("buck/schem", "t1", "id", schema1, null, false);
        StructField y1 = new StructField("y1", IntegerType$.MODULE$, true, Metadata.empty());
        StructField y2 = new StructField("y2", IntegerType$.MODULE$, true, Metadata.empty());
        StructType schema2 = new StructType(new StructField[] {y1, y2});
        VastTable t2 = new VastTable("buck/schem", "t2", "id", schema2, null, false);
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.conf().set("spark.sql.cbo.enabled", true);
            session.conf().set("spark.sql.cbo.joinReorder.enabled", true);
            session.conf().set("spark.sql.cbo.planStats.enabled", true);
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.t1 (x1 integer, x2 integer)").show();
            session.sql("create table ndb.buck.schem.t2 (y1 integer, y2 integer)").show();
            SparkVastStatisticsManager.getInstance().deleteTableStatistics(t1);
            SparkVastStatisticsManager.getInstance().deleteTableStatistics(t2);
            Tuple2<Attribute, ColumnStat> x1stat = getColumnStats(0, 9, 10, 0, "x1", IntegerType$.MODULE$, 0);
            Tuple2<Attribute, ColumnStat> x2stat = getColumnStats(0, 9, 10, 0, "x2", IntegerType$.MODULE$, 1);
            Tuple2<Attribute, ColumnStat> y1stat = getColumnStats(0, 99, 100, 0, "y1", IntegerType$.MODULE$, 0);
            Tuple2<Attribute, ColumnStat> y2stat = getColumnStats(0, 99, 100, 0, "y2", IntegerType$.MODULE$, 1);
            AttributeMap<ColumnStat> t1Stats = getColumnStatsAttrMap(ImmutableSet.of(x1stat, x2stat));
            AttributeMap<ColumnStat> t2Stats = getColumnStatsAttrMap(ImmutableSet.of(y1stat, y2stat));
//            Statistics t1MockStats = new Statistics(BigInt.apply(40), Option.apply(BigInt.apply(10)), t1Stats, false);
//            Statistics t2MockStats = new Statistics(BigInt.apply(400), Option.apply(BigInt.apply(100)), t2Stats, false);
            Statistics t1MockStats = new Statistics(BigInt.apply(40), Option.empty(), t1Stats, false);
            Statistics t2MockStats = new Statistics(BigInt.apply(400), Option.empty(), t2Stats, false);
            SparkVastStatisticsManager.getInstance().setTableStatistics(t1, t1MockStats);
            SparkVastStatisticsManager.getInstance().setTableStatistics(t2, t2MockStats);
//            session.sql("select * from ndb.buck.schem.t1 a where a.x1 <= 2").explain("cost");
//            session.sql("select a.x1 from ndb.buck.schem.t1 a where a.x1 <= 2").explain("cost");
//            session.sql("select a.x2 from ndb.buck.schem.t1 a where a.x1 <= 2").explain("cost");
//            session.sql("select t1.x1 from ndb.buck.schem.t1 join ndb.buck.schem.t2 on t1.x1 = t2.y1 where cos(t1.x2) == 1.0 and (t1.x1 > 1 or t1.x2 > 1) and t2.y1 > 6 and t2.y2 <= 2").explain("cost");
            session.sql("select t1.x1 from ndb.buck.schem.t1 join ndb.buck.schem.t2 on t1.x1 = t2.y1").explain("cost");
        }
    }

    private static AttributeMap<ColumnStat> getColumnStatsAttrMap(ImmutableSet<Tuple2<Attribute, ColumnStat>> list)
    {
        Builder<Tuple2<Attribute, ColumnStat>, List<Tuple2<Attribute, ColumnStat>>> objectSeqBuilder = List$.MODULE$.newBuilder();
        list.forEach(objectSeqBuilder::$plus$eq);
        Seq<Tuple2<Attribute, ColumnStat>> seq = objectSeqBuilder.result();
        return AttributeMap$.MODULE$.apply(seq);
    }

    private Tuple2<Attribute, ColumnStat> getColumnStats(Object min, Object max, Integer distinctCount, Integer nullCount, String name, DataType type, int fieldIndex)
    {
        StructField field = new StructField(name, type, true, Metadata.empty());
        return buildTup(fieldIndex, field, min, max, distinctCount, nullCount);
    }

    private Tuple2<Attribute, ColumnStat> buildTup(
            int fieldIndex,
            StructField field,
            Object minValue,
            Object maxValue,
            Integer distinctC,
            Integer nullC)
    {
        Option<BigInt> distinctCount = Option.apply(BigInt.apply(distinctC));
        Option<BigInt> nullCount = Option.apply(BigInt.apply(nullC));
        Option<Object> avgLen = Option.apply(4L);
        Option<Object> maxLen = Option.apply(4L);
        ColumnStat colStats = new ColumnStat(distinctCount, Option.apply(minValue), Option.apply(maxValue), nullCount, avgLen, maxLen, Option.empty(), 0);
        Attribute attribute = new AttributeReference(field.name(),
                field.dataType(), field.nullable(), field.metadata(), ExprId.apply(fieldIndex),
                (scala.collection.immutable.Seq<String>) scala.collection.immutable.Seq$.MODULE$.<String>empty());
        return Tuple2.apply(attribute, colStats);
    }

    @Test
    public void testRuntimeFiltering()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").collect();
            session.sql("create table ndb.buck.schem.tab (k integer, d date)").collect();
            session.sql("select * from ndb.buck.schem.tab t1 JOIN ndb.buck.schem.tab t2 ON t1.k = t2.k WHERE t2.d BETWEEN '2020-02-02' AND '2020-02-22'").explain("cost");
            session.sql("select * from ndb.buck.schem.tab t1 JOIN ndb.buck.schem.tab t2 ON t1.k = t2.k WHERE t2.k in (1,2,3)").explain("cost");
        }
    }

    @Test
    public void testFilterCompaction()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").collect();
            session.sql("create table ndb.buck.schem.tabdate (x date)").collect();
            java.util.List<String> dates = IntStream.range(10, 31).mapToObj(i -> format("CAST('1970-08-%s' AS DATE)", i)).collect(Collectors.toList());
            String values = String.join(",", dates);
            String sql = format("select count(*) from ndb.buck.schem.tabdate where x in (%s)", values);
            String expectedPushDown = "pushed_down_predicates=[[(x >= 221) AND (x <= 241)]]"; // 221 = 10-08-1970, 241 = 30-08-1970
            testCompactFilterPushdown(session, sql, expectedPushDown);

            session.sql("create table ndb.buck.schem.tab (x bigint)").collect();
            java.util.List<String> zeros = Collections.nCopies(60, "0");
            values = String.join(",", zeros) + ",7, 0, 62";
            sql = format("select count(*) from ndb.buck.schem.tab where x in (%s)", values);
            expectedPushDown = "pushed_down_predicates=[[x = 0, x = 7, x = 62]]";
            testCompactFilterPushdown(session, sql, expectedPushDown);

            sql = "select count(*) from ndb.buck.schem.tab where x >= 0 and x <= 99";
            expectedPushDown = "pushed_down_predicates=[[x IS NOT NULL], [x >= 0], [x <= 99]]";
            testCompactFilterPushdown(session, sql, expectedPushDown);

            values = IntStream.range(0, 60).mapToObj(i -> format("%s", i)).collect(Collectors.joining(","));
            sql = format("select count(*) from ndb.buck.schem.tab where x in (%s)", values);
            expectedPushDown = "pushed_down_predicates=[[(x >= 0) AND (x <= 59)]]";
            testCompactFilterPushdown(session, sql, expectedPushDown);

            values = IntStream.range(0, 4).mapToObj(i -> format("%s", i)).collect(Collectors.joining(","));
            sql = format("select count(*) from ndb.buck.schem.tab where x in (%s)", values);
            expectedPushDown = "pushed_down_predicates=[[x = 0, x = 1, x = 2, x = 3]]";
            testCompactFilterPushdown(session, sql, expectedPushDown);

            values = format("%s, %s, %s", Long.MAX_VALUE, -Long.MAX_VALUE, 0L);
            sql = format("select count(*) from ndb.buck.schem.tab where x in (%s)", values);
            expectedPushDown = "pushed_down_predicates=[[x = 9223372036854775807, x = -9223372036854775807, x = 0]]";
            testCompactFilterPushdown(session, sql, expectedPushDown);

            session.sql("create table ndb.buck.schem.tab_float (x float)").collect();
            values = IntStream.range(0, 4).mapToObj(i -> format("%s.5", i)).collect(Collectors.joining(","));
            sql = format("select count(*) from ndb.buck.schem.tab_float where x in (%s)", values);
            expectedPushDown = "pushed_down_predicates=[[x = 0.5, x = 1.5, x = 2.5, x = 3.5]]";
            testCompactFilterPushdown(session, sql, expectedPushDown);

            values = IntStream.range(0, 60).mapToObj(i -> format("%s.5", i)).collect(Collectors.joining(","));
            sql = format("select count(*) from ndb.buck.schem.tab_float where x in (%s)", values);
            expectedPushDown = "x = 14.5"; // predicates are not sorted
            testCompactFilterPushdown(session, sql, expectedPushDown);
        }
    }

    private static void testCompactFilterPushdown(SparkSession session, String sql, String expectedPushDown)
    {
        SparkPlan sparkPlan = session.sql(sql)
                .queryExecution().executedPlan();
        SparkPlan last = sparkPlan.collectLeaves().last();
        SparkPlan plan = ((AdaptiveSparkPlanExec) last).inputPlan().children().head().children().head();
        if (plan instanceof BatchScanExec) {
            Scan scan = ((BatchScanExec) plan).scan();
            String description = scan.description();
            assertTrue(description.contains(expectedPushDown), format("Actual: %s, Expected: %s", description, expectedPushDown));
        }
        else {
            fail();
        }
    }

    @Test
    public void testSparkStatisticsFallbackToTableLevelStats()
            throws VastException
    {
        long numRows = 70000L;
        long sizeInBytes = 280000L;
        Statistics tableStatistics = new Statistics(BigInt.apply(sizeInBytes), Option.apply(BigInt.apply(numRows)), AttributeMap$.MODULE$.empty(), false);
        VastStatistics vastStatistics = new VastStatistics(numRows, sizeInBytes);
        when(mockClient.s3GetObj(anyString(), anyString()))
                .thenReturn(Optional.empty());
        when(mockClient.getTableStats(any(), anyString(), anyString()))
                .thenReturn(vastStatistics);
        Supplier<VastClient> supplier = () -> mockClient;
        StructField charNField = new StructField("x", IntegerType$.MODULE$, true, Metadata.empty());
        StructType schema = new StructType(new StructField[] {charNField});
        VastTable table = new VastTable("buck/schem", "tab", "id", schema, supplier, false);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("show schemas from ndb").show();
            SparkPersistentStatistics sparkPersistentStatistics = new SparkPersistentStatistics(mockClient, NDB.getConfig());
            Optional<Statistics> newTableStatistics = sparkPersistentStatistics.getTableStatistics(table);
            assertEquals(Optional.of(tableStatistics), newTableStatistics);
        }
    }

    @Test
    public void testTxKeepAlive()
            throws IOException, InterruptedException
    {
        Map<VastTransaction, AtomicInteger> activeTxDuringInsert = new ConcurrentHashMap<>();
        String message = "bad request";
        Consumer<HttpExchange> insertSleeper = httpExchange -> {
            try {
                Thread.sleep(3 * 1000);
                activeTxDuringInsert.putAll(JobEventService.getInstance().orElseThrow(IllegalStateException::new).getActiveTransactions());
                httpExchange.sendResponseHeaders(400, message.length());
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(message.getBytes(StandardCharsets.UTF_8));
                }
            }
            catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        AtomicInteger getTxCtr = new AtomicInteger(0);
        Consumer<HttpExchange> getTxCtrAction = httpExchange -> {
            try {
                getTxCtr.incrementAndGet();
                httpExchange.sendResponseHeaders(200, message.length());
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(message.getBytes(StandardCharsets.UTF_8));
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        Consumer<HttpExchange> commitTxAction = httpExchange -> {
            try {
                httpExchange.sendResponseHeaders(200, "".length());
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(message.getBytes(StandardCharsets.UTF_8));
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            NDB.init();
            Optional<SparkListenerInterface> any = session.sparkContext().listenerBus().listeners().stream().filter(l -> l instanceof NDBJobsListener).findAny();
            assertTrue(any.isPresent());
            session.sql("create database ndb.buck.schem").collect();
            session.sql("create table ndb.buck.schem.tab (i integer)").show();
            handler.setHook("/buck/schem/tab", POST, insertSleeper);
            handler.setHook("/", GET, getTxCtrAction);
            handler.setHook("/", PUT, commitTxAction);
            try {
                session.sql("insert into ndb.buck.schem.tab values (1), (2), (3)").show();
            }
            catch (Exception se) {
                assertTrue(se.getMessage().contains("Failed inserting rows"), format("%s", se));
            }
            assertEquals(activeTxDuringInsert.size(), 1, format("activeTxDuringInsert: %s", activeTxDuringInsert));
            Thread.sleep(1100); // 1s is the keep alive interval
            Map<VastTransaction, AtomicInteger> activeTx = JobEventService.getInstance().orElseThrow(IllegalStateException::new).getActiveTransactions();
            assertTrue(activeTx.isEmpty(), format("activeTransactions: %s", activeTx));
            session.sql("select ndb.commit_tx()").show();
            activeTx = JobEventService.getInstance().orElseThrow(IllegalStateException::new).getActiveTransactions();
            assertTrue(activeTx.isEmpty(), format("activeTransactions: %s", activeTx));
            assertTrue(getTxCtr.get() > 0);
        }
    }
}
