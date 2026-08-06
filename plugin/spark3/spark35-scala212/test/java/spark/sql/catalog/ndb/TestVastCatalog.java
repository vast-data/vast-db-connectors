/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import com.amazonaws.http.HttpMethodName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sun.net.httpserver.HttpExchange;
import com.vastdata.client.RowColumnSecurityResponse;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.VastSchedulingInfo;
import com.vastdata.client.error.VastConflictException;
import com.vastdata.client.error.VastException;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.partition.PartitionConstants;
import com.vastdata.client.stats.VastStatistics;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.client.tx.VastTransactionFactory;
import com.vastdata.client.tx.ka.JobEventService;
import com.vastdata.mockserver.MockMapSchema;
import com.vastdata.mockserver.MockUtils;
import com.vastdata.mockserver.VastMockS3Server;
import com.vastdata.mockserver.VastRootHandler;
import com.vastdata.mockserver.handle.MockSchemaUtil;
import com.vastdata.spark.CommonSparkTestUtils;
import com.vastdata.spark.SparkTestUtils;
import com.vastdata.spark.VastArrowAllocator;
import com.vastdata.spark.VastScan;
import com.vastdata.spark.VastTable;
import com.vastdata.spark.VastView;
import com.vastdata.spark.statistics.FilterEstimator;
import com.vastdata.spark.statistics.SparkPersistentStatistics;
import com.vastdata.spark.statistics.SparkVastStatisticsManager;
import com.vastdata.spark.statistics.SparkVastStatisticsManagerTestUtil;
import com.vastdata.spark.statistics.TableLevelStatistics;
import com.vastdata.spark.tx.VastSparkTransactionsManager;
import ndb.NDB;
import ndb.NDBJobsListener;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.scheduler.SparkListenerInterface;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeMap;
import org.apache.spark.sql.catalyst.expressions.AttributeMap$;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.AttributeSet;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.FilterExec;
import org.apache.spark.sql.execution.ProjectExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.mutable.Builder;
import scala.math.BigInt;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.amazonaws.http.HttpMethodName.DELETE;
import static com.amazonaws.http.HttpMethodName.GET;
import static com.amazonaws.http.HttpMethodName.POST;
import static com.amazonaws.http.HttpMethodName.PUT;
import static com.vastdata.OptionalPrimitiveHelpers.map;
import static com.vastdata.client.VastClient.AUDIT_LOG_BUCKET_NAME;
import static com.vastdata.client.VastClient.BIG_CATALOG_BUCKET_NAME;
import static java.lang.String.format;
import static java.lang.String.join;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowableOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;
import static spark.sql.catalog.ndb.SparkConfValidator.FORMAT_UNSAFE_SPARK_CONFIGURATION;
import static spark.sql.catalog.ndb.SparkConfValidator.SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION;
import static spark.sql.catalog.ndb.TypeUtil.SPARK_INT64_ROW_ID_FIELD;

@Listeners(CommonSparkTestUtils.TestListener.class)
public class TestVastCatalog
{
    private static final VastRootHandler handler = new VastRootHandler();
    private static VastMockS3Server mockServer;
    @Mock VastTransaction mockTransactionHandle;
    @Mock private VastClient mockClient;
    @Mock private VastCatalogUtils vastCatalogUtils;
    @Mock private SparkViewsMetadataReaderFactory mockViewMetadataReaderFactory;
    @Mock private SparkViewsMetadataReader mockViewMetadataReader;
    private int testPort;
    private AutoCloseable autoCloseable;

    private static void assertTableSchemaAfterAddColumn(VastCatalog unit,
                                                        Identifier tableIdent,
                                                        String[] colName,
                                                        DataType colType,
                                                        StructType expectedSchema)
            throws NoSuchTableException
    {
        TableChange addColumn = TableChange.addColumn(colName, colType);
        Table tableAfterChange = unit.alterTable(tableIdent, addColumn);
        StructType schema = tableAfterChange.schema();
        assertEquals(schema, expectedSchema);
    }

    private static void assertTransactionCommandError(SparkSession session,
                                                      String sqlText)
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

    private static void testGracefulVastException(SparkSession session,
                                                  String sql,
                                                  String tablePath,
                                                  HttpMethodName method,
                                                  String message,
                                                  int rc,
                                                  Class<?> expectedException)
    {
        Consumer<HttpExchange> action = httpExchange ->
        {
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
            assertTrue(foundCause,
                    format("Expected exception with cause of type %s, but got %s",
                            expectedException.getSimpleName(), any));
        }
        finally {
            assertEquals(
                    VastArrowAllocator.writeAllocator().getAllocatedMemory(),
                    0);
        }
    }

    private static void testProjectOptimizationOutput(SparkPlan sparkPlan,
                                                      String expectedAttributeName,
                                                      boolean expectFilter,
                                                      Optional<Set<String>> filterColumnNames)
    {
        Seq<Attribute> finalOutput = sparkPlan.output();
        assertEquals(finalOutput.length(), 1);
        Attribute att = finalOutput.head();
        assertEquals(att.name(), expectedAttributeName);
        SparkPlan last = sparkPlan.collectLeaves().last();
        assertTrue(last instanceof AdaptiveSparkPlanExec,
                format("last was of class: %s", last.getClass()));
        AdaptiveSparkPlanExec a = (AdaptiveSparkPlanExec) last;
        SparkPlan project = a.inputPlan().children().head().children().head();
        assertTrue(project instanceof ProjectExec,
                format("Expected a project node, but node was of class: %s",
                        last.getClass()));
        assertEquals(project.output(), finalOutput);
        SparkPlan projectChild = ((ProjectExec) project).child();
        if (expectFilter) {
            assertTrue(projectChild instanceof FilterExec,
                    format("Expected a filter node, but node was of class: %s",
                            projectChild.getClass()));
            if (filterColumnNames.isPresent()) {
                FilterExec filter = (FilterExec) projectChild;
                AttributeSet references = filter.condition().references();
                ImmutableSet.Builder<String> builder = ImmutableSet.builder();
                references.foreach(filterCol ->
                {
                    builder.add(filterCol.name());
                    return null;
                });
                assertEquals(builder.build(), filterColumnNames.get(),
                        "Filtered columns do not match expectations");
            }
        }
        else {
            assertTrue(projectChild instanceof BatchScanExec,
                    format("Expected a scan node, but node was of class: %s",
                            projectChild.getClass()));
        }
        assertEquals(projectChild.output().length(), 1);
        assertEquals(projectChild.output().head().name(), "i");
    }

    private static AttributeMap<ColumnStat> getColumnStatsAttrMap(ImmutableSet<Tuple2<Attribute, ColumnStat>> list)
    {
        Builder<Tuple2<Attribute, ColumnStat>, List<Tuple2<Attribute, ColumnStat>>> objectSeqBuilder = List$.MODULE$.newBuilder();
        list.forEach(objectSeqBuilder::$plus$eq);
        Seq<Tuple2<Attribute, ColumnStat>> seq = objectSeqBuilder.result();
        return AttributeMap$.MODULE$.apply(seq);
    }

    private static void testCompactFilterPushdown(SparkSession session,
                                                  String sql,
                                                  String expectedPushDown)
    {
        SparkPlan sparkPlan = session.sql(sql).queryExecution().executedPlan();
        SparkPlan last = sparkPlan.collectLeaves().last();
        SparkPlan plan = ((AdaptiveSparkPlanExec) last)
                .inputPlan()
                .children()
                .head()
                .children()
                .head();
        if (plan instanceof BatchScanExec) {
            Scan scan = ((BatchScanExec) plan).scan();
            String description = scan.description();
            assertTrue(description.contains(expectedPushDown),
                    format("Actual: %s, Expected: %s", description,
                            expectedPushDown));
        }
        else {
            fail();
        }
    }

    private static Statistics overrideEstimationFactor(final SparkSession session,
                                                       final String query,
                                                       final double factor)
    {
        try (MockedStatic<FilterEstimator> mockedFilterEstimator = mockStatic(
                FilterEstimator.class)) {
            mockedFilterEstimator
                    .when(() -> FilterEstimator.estimateStatistics(anyList(),
                            ArgumentMatchers.any(), ArgumentMatchers.any()))
                    .thenAnswer(input ->
                    {
                        final TableLevelStatistics statistics = input.getArgument(
                                1);
                        final UnaryOperator<Long> applySelectivity = stat -> (long) (stat * factor);
                        return new TableLevelStatistics(
                                map(statistics.sizeInBytes(), applySelectivity),
                                map(statistics.numRows(), applySelectivity),
                                statistics.columnStats());
                    });
            return session.sql(query).queryExecution().optimizedPlan().stats();
        }
    }

    private static void assertClose(final long left,
                                    final long right,
                                    final long epsilon)
    {
        assertTrue(Math.abs(left - right) < epsilon);
    }

    private static void testPushedDownPredicates(SparkPlan sparkPlan,
                                                 String expectedPushedDownPredicates,
                                                 Optional<String> expectedPostScanFilter)
    {
        SparkPlan last = sparkPlan.collectLeaves().last();
        assertTrue(last instanceof AdaptiveSparkPlanExec,
                format("last was of class: %s", last.getClass()));
        AdaptiveSparkPlanExec a = (AdaptiveSparkPlanExec) last;
        final SparkPlan project = a
                .inputPlan()
                .children()
                .head()
                .children()
                .head();
        BatchScanExec vastBatchScanFilter;

        if (expectedPostScanFilter.isPresent()) { // not pushed down
            FilterExec postFilter = (FilterExec) project;
            assertTrue(postFilter
                    .condition()
                    .toString()
                    .matches(expectedPostScanFilter.get()));

            vastBatchScanFilter = (BatchScanExec) postFilter.children().head();
        }
        else { // pushed down
            vastBatchScanFilter = (BatchScanExec) project;
        }

        VastScan scan = (VastScan) vastBatchScanFilter.scan();
        String description = scan.description();
        assertTrue(description.contains(expectedPushedDownPredicates));
    }

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
        SparkSession.clearDefaultSession();
        SparkSession.clearActiveSession();
        Map<String, Set<MockMapSchema>> testMockServerSchema = new HashMap<>(1);
        testMockServerSchema.put(AUDIT_LOG_BUCKET_NAME, ImmutableSet.of());
        testMockServerSchema.put(BIG_CATALOG_BUCKET_NAME, ImmutableSet.of());
        handler.setSchema(testMockServerSchema);
        autoCloseable = openMocks(this);
        when(mockTransactionHandle.getId()).thenReturn(
                Long.parseUnsignedLong("514026084031791104"));
    }

    @Test
    public void testDMLWithColumnMask()
            throws IOException, VastUserException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck.schem.tab1 (col_int_x integer, col_int_y integer, col_s string, decimal_3838 decimal(38, 0))")
                    .show();
            assertThatThrownBy(() -> session
                    .sql("delete from ndb.buck.schem.tab1 where decimal_3838=0.1 AND col_int_x = 6")
                    .show())
                    .isInstanceOf(SparkException.class)
                    .hasMessageContaining("QueryData");
            assertThatThrownBy(() -> session
                    .sql("delete from ndb.buck.schem.tab1 where col_int_x = 6")
                    .show())
                    .isInstanceOf(SparkException.class)
                    .hasMessageContaining("QueryData");
            CatalogPlugin ndb = session
                    .sessionState()
                    .catalogManager()
                    .catalog("ndb");
            VastCatalog vastCatalog = (VastCatalog) ndb;
            Map<String, String> masked = ImmutableMap.of("col_s",
                    "regexp_replace(col_s, '[0-9]', '***')");
            RowColumnSecurityResponse rowColumnSecurityResponse = new RowColumnSecurityResponse(
                    ImmutableList.of(), ImmutableSet.of(), ImmutableSet.of(),
                    masked);
            VastConfig vastConfig = NDB.getConfig();
            VastClient vastClient = NDB.getVastClient(vastConfig);
            VastCatalogTestUtils vastCatalogTestUtils = new VastCatalogTestUtils(
                    vastConfig, vastClient,
                    VastSparkTransactionsManager.getInstance(vastClient,
                            new VastTransactionFactory()));
            vastCatalogTestUtils.setRowColumnsSecurityResponse("buck/schem",
                    "tab1", rowColumnSecurityResponse);
            vastCatalog.setVastCatalogUtils(vastCatalogTestUtils);
            InitializedVastCatalog.setVastCatalog(vastCatalog);
            assertThatThrownBy(() -> session
                    .sql("delete from ndb.buck.schem.tab1 where decimal_3838=0.1 AND col_int_x = 6")
                    .show())
                    .isInstanceOf(VastRuntimeException.class)
                    .hasMessageContaining(
                            "Delete from table is not allowed by current VAST security policy rules");
            assertThatThrownBy(() -> session
                    .sql("delete from ndb.buck.schem.tab1 where col_int_x = 6")
                    .show())
                    .isInstanceOf(VastRuntimeException.class)
                    .hasMessageContaining(
                            "Delete from table is not allowed by current VAST security policy rules");
            assertThatThrownBy(() -> session
                    .sql("update ndb.buck.schem.tab1 set col_int_x = col_int_x + 1")
                    .show())
                    .isInstanceOf(VastRuntimeException.class)
                    .hasMessageContaining(
                            "Update table is not allowed by current VAST security policy rules");
        }
    }

    @Test
    public void testDMLWithRowFilter()
            throws IOException, VastUserException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck.schem.tab1 (col_int_x integer, col_int_y integer, col_s string)")
                    .show();
            assertThatThrownBy(() -> session
                    .sql("update ndb.buck.schem.tab1 set col_int_x = col_int_x / 2 where cos(col_int_y) > 0.5")
                    .show())
                    .isInstanceOf(SparkException.class)
                    .hasMessageContaining("QueryData");
            CatalogPlugin ndb = session
                    .sessionState()
                    .catalogManager()
                    .catalog("ndb");
            VastCatalog vastCatalog = (VastCatalog) ndb;
            java.util.List<String> filters = ImmutableList.of(
                    "col_int_x > col_int_y", "length(col_s) > 10");
            RowColumnSecurityResponse rowColumnSecurityResponse = new RowColumnSecurityResponse(
                    filters, ImmutableSet.of(), ImmutableSet.of(),
                    ImmutableMap.of());
            VastConfig vastConfig = NDB.getConfig();
            VastClient vastClient = NDB.getVastClient(vastConfig);
            VastCatalogTestUtils vastCatalogTestUtils = new VastCatalogTestUtils(
                    vastConfig, vastClient,
                    VastSparkTransactionsManager.getInstance(vastClient,
                            new VastTransactionFactory()));
            vastCatalogTestUtils.setRowColumnsSecurityResponse("buck/schem",
                    "tab1", rowColumnSecurityResponse);
            vastCatalog.setVastCatalogUtils(vastCatalogTestUtils);
            InitializedVastCatalog.setVastCatalog(vastCatalog);
            assertThatThrownBy(() -> session
                    .sql("delete from ndb.buck.schem.tab1 where col_int_x = 0")
                    .show())
                    .isInstanceOf(SparkException.class)
                    .hasMessageContaining("QueryData");
            assertThatThrownBy(() -> session
                    .sql("update ndb.buck.schem.tab1 set set col_int_x = col_int_x / 2 where col_int_y = 7")
                    .show())
                    .isInstanceOf(VastRuntimeException.class)
                    .hasMessageContaining(
                            "Update table is not allowed by current VAST security policy rules");
        }
    }

    @Test
    public void testUpdateWithPushedDownRowFilter()
            throws IOException, VastUserException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck.schem.tab1 (col_int_x integer, col_int_y integer, col_s string)")
                    .show();
            CatalogPlugin ndb = session
                    .sessionState()
                    .catalogManager()
                    .catalog("ndb");
            VastCatalog vastCatalog = (VastCatalog) ndb;
            java.util.List<String> filters = ImmutableList.of("col_int_x = 2");
            RowColumnSecurityResponse rowColumnSecurityResponse = new RowColumnSecurityResponse(
                    filters, ImmutableSet.of(), ImmutableSet.of(),
                    ImmutableMap.of());
            VastConfig vastConfig = NDB.getConfig();
            VastClient vastClient = NDB.getVastClient(vastConfig);
            VastCatalogTestUtils vastCatalogTestUtils = new VastCatalogTestUtils(
                    vastConfig, vastClient,
                    VastSparkTransactionsManager.getInstance(vastClient,
                            new VastTransactionFactory()));
            vastCatalogTestUtils.setRowColumnsSecurityResponse("buck/schem",
                    "tab1", rowColumnSecurityResponse);
            vastCatalog.setVastCatalogUtils(vastCatalogTestUtils);
            InitializedVastCatalog.setVastCatalog(vastCatalog);
            assertThatThrownBy(() -> session
                    .sql("update ndb.buck.schem.tab1 set col_int_x = 2 where col_int_x = 1")
                    .show())
                    .isInstanceOf(VastRuntimeException.class)
                    .hasMessageContaining(
                            "Update table is not allowed by current VAST security policy rules");
        }
    }

    @Test
    public void testAFullResolutionFlowWithRCLS()
            throws IOException, VastUserException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck.schem").show();
            CatalogPlugin ndb = session
                    .sessionState()
                    .catalogManager()
                    .catalog("ndb");
            VastCatalog vastCatalog = (VastCatalog) ndb;
            java.util.List<String> filters = ImmutableList.of(
                    "col_int_x > col_int_y", "length(col_s) > 10",
                    "((col_int_y > 0) AND (col_s != 'bla'))");
            Set<String> allowed = ImmutableSet.of();
            Set<String> denied = ImmutableSet.of();
            Map<String, String> masked = ImmutableMap.of("col_s",
                    "regexp_replace(col_s, '[0-9]', '***')"); //TODO: add case for pushed-down predicate on a masked column
            RowColumnSecurityResponse rowColumnSecurityResponse = new RowColumnSecurityResponse(
                    filters, allowed, denied, masked);
            VastConfig vastConfig = NDB.getConfig();
            VastClient vastClient = NDB.getVastClient(vastConfig);
            VastCatalogTestUtils vastCatalogTestUtils = new VastCatalogTestUtils(
                    vastConfig, vastClient,
                    VastSparkTransactionsManager.getInstance(vastClient,
                            new VastTransactionFactory()));
            vastCatalogTestUtils.setRowColumnsSecurityResponse("buck/schem",
                    "tab1", rowColumnSecurityResponse);
            vastCatalog.setVastCatalogUtils(vastCatalogTestUtils);
            InitializedVastCatalog.setVastCatalog(vastCatalog);
            StructType schema = new StructType(
                    new StructField[] {new StructField("col_int_x",
                            DataTypes.IntegerType, true, Metadata.empty()),
                            new StructField("col_int_y", DataTypes.IntegerType,
                                    true, Metadata.empty()),
                            new StructField("col_s", DataTypes.StringType, true,
                                    Metadata.empty())});
            VastView testView = new VastView("view1",
                    "SELECT *, 'const_string' as col_vs FROM ndb.buck.schem.tab1",
                    "ndb", "", new String[] {"buck", "schem"}, schema,
                    new String[0], new String[0], new String[0]);
            when(mockViewMetadataReader.getVastView(
                    nullable(SimpleVastTransaction.class),
                    nullable(String.class), nullable(String.class),
                    nullable(String[].class), anyList(),
                    nullable(VastSchedulingInfo.class),
                    nullable(String.class))).thenReturn(testView);
            when(mockViewMetadataReaderFactory.instance()).thenReturn(
                    mockViewMetadataReader);
            vastCatalog.setSparkViewsMetadataReaderFactory(
                    mockViewMetadataReaderFactory);
            session
                    .sql("CREATE TABLE ndb.buck.schem.tab1 (col_int_x integer, col_int_y integer, col_s string)")
                    .show();
            session
                    .sql("CREATE VIEW ndb.buck.schem.view1 AS (SELECT *, 'const_string_view_column' as col_vs FROM ndb.buck.schem.tab1)")
                    .show();
            session
                    .sql("alter table ndb.buck.schem.tab1 add column (col_bigint_z bigint)")
                    .show();
            session
                    .sql("alter table ndb.buck.schem.tab1 add columns (col_bigint_z2 bigint, col_bigint_z3 bigint)")
                    .show();
            session
                    .sql("alter table ndb.buck.schem.tab1 drop column col_bigint_z")
                    .show();
            session
                    .sql("alter table ndb.buck.schem.tab1 drop columns col_bigint_z2, col_bigint_z3")
                    .show();
            session.sql("show columns from ndb.buck.schem.tab1").show();

            session.sql("describe table ndb.buck.schem.tab1").show();
            session.sql("select * from ndb.buck.schem.tab1").explain("cost");
            session
                    .sql("select col_int_y + 1 as y_incr, col_s, substring(col_s, 1, 2) as sub_col_s from ndb.buck.schem.view1 where cos(col_int_y) > 0.5 and col_int_y > 7 and length(col_s) > 3 order by y_incr desc")
                    .explain("cost");
            java.util.List<String> viewFilters = ImmutableList.of(
                    "length(col_vs) > length(col_s)");
            Map<String, String> viewMasked = ImmutableMap.of("col_vs",
                    "substring(col_vs, 0, 6)");
            RowColumnSecurityResponse viewRowColumnSecurityResponse = new RowColumnSecurityResponse(
                    viewFilters, allowed, denied, viewMasked);
            vastCatalogTestUtils.setRowColumnsSecurityResponse("buck/schem",
                    "view1", viewRowColumnSecurityResponse);
            session
                    .sql("select col_int_y + 1 as y_incr, col_s, substring(col_s, 1, 2) as sub_col_s from ndb.buck.schem.view1 where cos(col_int_y) > 0.5 and col_int_y > 7 and length(col_s) > 3 order by y_incr desc")
                    .explain("cost");
            session
                    .sql("select * from ndb.buck.schem.view1 where cos(col_int_y) > 0.5 and col_int_y > 7 and length(col_s) > 3 order by 1 desc")
                    .explain("cost");
            session
                    .sql("select col_vs, col_int_y + 1 as y_incr, col_s, substring(col_s, 1, 2) as sub_col_s from ndb.buck.schem.view1 where cos(col_int_y) > 0.5 and col_int_y > 7 and length(col_s) > 3 order by y_incr desc")
                    .explain("cost");
        }
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
            session.sql(
                    "create table ndb.buck.schem.tab (b boolean, i integer, r ARRAY<DOUBLE>)");
            VastCatalog unit = new VastCatalog();
            unit.initialize("", CaseInsensitiveStringMap.empty());
            Identifier tableIdent = Identifier.of(
                    new String[] {"buck", "schem"}, "tab");
            String[] colName = new String[] {"s"};
            DataType colType = DataTypes.StringType;
            TableChange addColumn = TableChange.addColumn(colName, colType);
            Table tableAfterChange = unit.alterTable(tableIdent, addColumn);
            StructType schema = tableAfterChange.schema();
            ArrayType arrayType = DataTypes.createArrayType(
                    DataTypes.DoubleType);
            StructType expectedSchema = new StructType(
                    new StructField[] {createStructField("b",
                            DataTypes.BooleanType, true),
                            createStructField("i", DataTypes.IntegerType, true),
                            createStructField("r", arrayType, true),
                            createStructField("s", DataTypes.StringType,
                                    true)});
            assertEquals(schema, expectedSchema);

            colName = new String[] {"m"};
            MapType mapType = DataTypes.createMapType(DataTypes.DateType,
                    DataTypes.TimestampType);
            colType = mapType;
            expectedSchema = new StructType(
                    new StructField[] {createStructField("b",
                            DataTypes.BooleanType, true),
                            createStructField("i", DataTypes.IntegerType, true),
                            createStructField("r", arrayType, true),
                            createStructField("s", DataTypes.StringType, true),
                            createStructField("m", mapType, true)});
            assertTableSchemaAfterAddColumn(unit, tableIdent, colName, colType,
                    expectedSchema);
            colName = new String[] {"st"};
            StructType structType = DataTypes.createStructType(
                    new StructField[] {new StructField("subfield",
                            DataTypes.BinaryType, true, Metadata.empty())});
            colType = structType;
            expectedSchema = new StructType(
                    new StructField[] {createStructField("b",
                            DataTypes.BooleanType, true),
                            createStructField("i", DataTypes.IntegerType, true),
                            createStructField("r", arrayType, true),
                            createStructField("s", DataTypes.StringType, true),
                            createStructField("m", mapType, true),
                            createStructField("st", structType, true)});
            assertTableSchemaAfterAddColumn(unit, tableIdent, colName, colType,
                    expectedSchema);
        }
    }

    @Test
    public void testCreateViewRelativeNames()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck.schem.tab1 (b BOOLEAN, i1 INTEGER)")
                    .show();
            session.sql("USE ndb.buck.schem").show();
            session
                    .sql("CREATE VIEW view1 as (select i1, INTERVAL '2021' YEAR from tab1 where i1 > 10)")
                    .show();
        }
    }

    @Test
    public void testCreateViewFullyQualifiedNames()
            throws IOException
    {
        // CREATE [ OR REPLACE ] [ [ GLOBAL ] TEMPORARY ] VIEW [ IF NOT EXISTS ] view_identifier
        //    create_view_clauses AS query
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck.schem.tab1 (b BOOLEAN, i1 INTEGER)")
                    .show();
            session
                    .sql("CREATE TABLE ndb.buck.schem.tab2 (b BOOLEAN, i2 INTEGER)")
                    .show();
            String createViewNoAliasSql = "CREATE VIEW ndb.buck.schem.view_no_alias " + "COMMENT 'View comment'" + "as select tab1.b, tab2.b from ndb.buck.schem.tab1 join ndb.buck.schem.tab2 on i1 = i2";
            session.sql(createViewNoAliasSql).show();
            String createView1Sql = "CREATE VIEW ndb.buck.schem.view1 " + "(t1b COMMENT 'b from tab2', t2b) " + "COMMENT 'View comment'" + "as select tab1.b, tab2.b from ndb.buck.schem.tab1 join ndb.buck.schem.tab2 on i1 = i2";
            session.sql(createView1Sql).show();
            Row[] collect = (Row[]) session
                    .sql("SHOW VIEWS FROM ndb.buck.schem")
                    .collect();
            assertEquals(collect.length, 2);
            Set<String> expectedViewNames = new HashSet<>(
                    Arrays.asList("view1", "view_no_alias"));
            Set<String> actualViewNames = Arrays
                    .stream(collect)
                    .map(row -> row.getAs("viewName").toString())
                    .collect(Collectors.toSet());
            assertEquals(actualViewNames, expectedViewNames);
            try {
                session.sql(createView1Sql).show();
            }
            catch (RuntimeException e) {
                assertEquals(e.getCause().getClass(),
                        ViewAlreadyExistsException.class);
            }

            String replaceView1Sql = "CREATE OR REPLACE VIEW ndb.buck.schem.view1 " + "(t1b COMMENT 'b from tab2', t2b) " + "COMMENT 'Replcaed View comment'" + "as select tab1.b, tab2.b from ndb.buck.schem.tab1 join ndb.buck.schem.tab2 on i1 = i2";
            session.sql(replaceView1Sql).show();

            String createViewBadSchema = "CREATE OR REPLACE VIEW ndb.buck.schem2.view1 " + "(t1b COMMENT 'b from tab2', t2b) " + "COMMENT 'Replcaed View comment'" + "as select tab1.b, tab2.b from ndb.buck.schem.tab1 join ndb.buck.schem.tab2 on i1 = i2";
            try {
                session.sql(createViewBadSchema).show();
            }
            catch (RuntimeException e) {
                assertEquals(e.getCause().getClass(),
                        NoSuchNamespaceException.class);
            }

            String createViewBadTable = "CREATE OR REPLACE VIEW ndb.buck.schem1.view1 " + "(t1b COMMENT 'b from tab2', t2b) " + "COMMENT 'Replcaed View comment'" + "as select tab1.b, tab2.b from ndb.buck.schem.tab3 join ndb.buck.schem.tab2 on i1 = i2";
            try {
                session.sql(createViewBadTable).show();
            }
            catch (Exception e) {
                assertTrue(e instanceof AnalysisException,
                        "Expected AnalysisException but got: " + e);
            }
        }
    }

    @Test(expectedExceptions = AnalysisException.class, expectedExceptionsMessageRegExp = ".*The table or view `nosuchtab` cannot be found.*")
    public void testNoSchemaPathQuery()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select * from nosuchtab").show();
        }
    }

    @Test
    public void testBuiltinProvidersResolutionORION327182()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            assertThatThrownBy(() -> session
                    .sql("select * from parquet.`hive-tpcds-sf10tb/store_sales/`")
                    .show())
                    .isInstanceOf(AnalysisException.class)
                    .hasMessageContaining("[PATH_NOT_FOUND]")
                    .hasMessageContaining("store_sales.");
            assertThatThrownBy(
                    () -> session.sql("select * from nosuchtab").show())
                    .isInstanceOf(AnalysisException.class)
                    .hasMessageContaining("[TABLE_OR_VIEW_NOT_FOUND]")
                    .hasMessageContaining(
                            "The table or view `nosuchtab` cannot be found.");
            assertThatThrownBy(() -> session
                    .sql("select * from nosuchcatalog.nosuchschema.nosuchtab")
                    .show())
                    .isInstanceOf(AnalysisException.class)
                    .hasMessageContaining("[TABLE_OR_VIEW_NOT_FOUND]")
                    .hasMessageContaining(
                            "The table or view `nosuchcatalog`.`nosuchschema`.`nosuchtab` cannot be found.");
            assertThatThrownBy(() -> session
                    .sql("select * from ndb.buck.schem.tab")
                    .show())
                    .isInstanceOf(AnalysisException.class)
                    .hasMessageContaining("[TABLE_OR_VIEW_NOT_FOUND]")
                    .hasMessageContaining(
                            "The table or view `ndb`.`buck`.`schem`.`tab` cannot be found.");
        }
    }

    @Test
    public void testNoAllowedColumns()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck.schem.tab (b BOOLEAN, i INTEGER)")
                    .show();
            assertThatThrownBy(() -> session
                    .sql("select count(*) from ndb.buck.schem.tab")
                    .show())
                    .isInstanceOf(SparkException.class)
                    .hasMessageContaining("QueryData");
            Consumer<HttpExchange> emptySchemaResponse = he ->
            {
                URI requestURI = he.getRequestURI();
                String query = requestURI.getQuery();
                try {
                    byte[] bytes;
                    int code;
                    if (query.equalsIgnoreCase("row-column-security")) {
                        code = 404;
                        bytes = "No security policy for table".getBytes(
                                StandardCharsets.UTF_8);
                    }
                    else {
                        bytes = MockSchemaUtil
                                .serializeFields(ImmutableList.of())
                                .get();
                        code = 200;
                    }
                    he
                            .getResponseHeaders()
                            .put("tabular-next-key", ImmutableList.of("0"));
                    he
                            .getResponseHeaders()
                            .put("tabular-is-truncated",
                                    ImmutableList.of("false"));
                    he.sendResponseHeaders(code, bytes.length);
                    try (OutputStream os = he.getResponseBody()) {
                        os.write(bytes);
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
            handler.setHook("/buck/schem/tab", GET, emptySchemaResponse);
            assertThatThrownBy(() -> session
                    .sql("select count(*) from ndb.buck.schem.tab")
                    .show())
                    .isInstanceOf(VastRuntimeException.class)
                    .hasMessageContaining("Schema is empty");
        }
    }

    @Test
    public void testDropView()
            throws IOException
    {
        // DROP VIEW [ IF EXISTS ] view_identifier
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck.schem.tab (b BOOLEAN, i INTEGER)")
                    .show();
            session.sql("DROP VIEW IF EXISTS ndb.buck.schem.view1").show();
            try {
                session.sql("DROP VIEW ndb.buck.schem.view1").show();
            }
            catch (RuntimeException e) {
                assertEquals(e.getCause().getClass(),
                        NoSuchViewException.class);
            }

            session
                    .sql("CREATE VIEW ndb.buck.schem.view1 AS (SELECT * FROM ndb.buck.schem.tab)")
                    .show();
            Row[] rows = (Row[]) session
                    .sql("SHOW VIEWS from ndb.buck.schem")
                    .collect();
            assertEquals(rows.length, 1);
            assertEquals(rows[0].getAs("viewName").toString(), "view1");

            session.sql("DROP VIEW IF EXISTS ndb.buck.schem.view1").show();
            assertEquals(((Row[]) session
                    .sql("SHOW VIEWS from ndb.buck.schem")
                    .collect()).length, 0);

            session
                    .sql("CREATE VIEW ndb.buck.schem.view1 AS (SELECT * FROM ndb.buck.schem.tab)")
                    .show();
            rows = (Row[]) session
                    .sql("SHOW VIEWS from ndb.buck.schem")
                    .collect();
            assertEquals(rows.length, 1);
            assertEquals(rows[0].getAs("viewName").toString(), "view1");

            session.sql("DROP VIEW ndb.buck.schem.view1").show();
            session.sql("SHOW VIEWS from ndb.buck.schem").show();
        }
    }

    @Test(expectedExceptions = AnalysisException.class, expectedExceptionsMessageRegExp = ".*TABLE_OR_VIEW_NOT_FOUND.*view1.*")
    public void testAlterViewAsOnNonexistingViewFailsGracefully()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck.schem").show();
            session
                    .sql("ALTER VIEW ndb.buck.schem.view1 AS (SELECT * FROM ndb.buck.schem.tab where i > 0)")
                    .show(20, false);
        }
    }

    @Test(expectedExceptions = VastRuntimeException.class, expectedExceptionsMessageRegExp = ".*Delete from a view is not supported.*")
    public void testDeleteFromView()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck.schem.tab (b BOOLEAN, i INTEGER)")
                    .show();
            session
                    .sql("CREATE VIEW ndb.buck.schem.view1 AS (SELECT * FROM ndb.buck.schem.tab)")
                    .show();
            StructType schema = new StructType(
                    new StructField[] {new StructField("b",
                            DataTypes.BooleanType, true, Metadata.empty()),
                            new StructField("i", DataTypes.IntegerType, true,
                                    Metadata.empty())});
            VastView testView = new VastView("view1",
                    "SELECT * FROM ndb.buck.schem.tab", "ndb", "",
                    new String[] {"buck", "schem"}, schema, new String[0],
                    new String[0], new String[0]);
            when(mockViewMetadataReader.getVastView(
                    nullable(SimpleVastTransaction.class),
                    nullable(String.class), nullable(String.class),
                    nullable(String[].class), anyList(),
                    nullable(VastSchedulingInfo.class),
                    nullable(String.class))).thenReturn(testView);
            when(mockViewMetadataReaderFactory.instance()).thenReturn(
                    mockViewMetadataReader);
            CatalogPlugin ndb = session
                    .sessionState()
                    .catalogManager()
                    .catalog("ndb");
            VastCatalog vastCatalog = (VastCatalog) ndb;
            vastCatalog.setSparkViewsMetadataReaderFactory(
                    mockViewMetadataReaderFactory);
            InitializedVastCatalog.setVastCatalog(vastCatalog);
            session
                    .sql("DELETE FROM ndb.buck.schem.view1 where i > 0")
                    .show(20, false);
        }
    }

    @Test
    public void testCreateViewWithCos()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck.schem.tab (b BOOLEAN, i INTEGER)")
                    .show();
            session
                    .sql("CREATE VIEW ndb.buck.schem.view1 AS SELECT i+1 FROM ndb.buck.schem.tab where cos(i) > 0.5")
                    .show();
        }
    }

    @Test
    public void testDataTypes()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab " + "(b boolean, i integer, m1 map<string, integer>, m2 map<char(5), timestamp>, " + "r STRUCT<a: INTEGER, b: STRING, c: char(7)>, v varchar(30), c char(40), d DATE, t timestamp)")
                    .show();
            session.sql("show columns from ndb.buck.schem.tab").show();
            session
                    .sql("insert into ndb.buck.schem.tab(b, i, m1, m2, r, v, c, d, t) values " + "(FALSE, 321, map('astr', 777), map('qwert', timestamp '2008-11-09 15:45:21.123'), " + "(3, 'structstr', 'char7'), 'varcharstr', 'charstr', date '2008-11-11', " + "timestamp '2008-11-09 15:45:21')")
                    .show();
            session
                    .sql("select r.c, r.b from ndb.buck.schem.tab")
                    .explain("cost");
        }
    }

    @Test(expectedExceptions = VastRuntimeException.class, expectedExceptionsMessageRegExp = ".*Failed committing transaction.*")
    public void testInsertCommitError()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (b boolean, i integer)")
                    .show();
            String message = "Some bad request";
            Consumer<HttpExchange> action = httpExchange ->
            {
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
            session
                    .sql("insert into ndb.buck.schem.tab(b, i) values (FALSE, 321)")
                    .show();
        }
    }

    @Test(enabled = false)
    public void testArrayPushdown()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (l array<string>)")
                    .show();
            session
                    .sql("select l from ndb.buck.schem.tab where l is not null")
                    .show();
        }
    }

    private Row[] getRows(SparkSession session, String sql)
    {
        Object o = session.sql(sql).collect();
        return (Row[]) o;
    }

    @Test
    public void testShowSchemas()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        String testBucket2 = "buck2";
        mockUtils.createBucket(this.testPort, testBucket);
        mockUtils.createBucket(this.testPort, testBucket2);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session.sql("show schemas").show(false);
            assertEquals(getRows(session, "show schemas from ndb")[0].get(0),
                    "buck.schem");
            assertEquals(
                    getRows(session, "show schemas from ndb.buck")[0].get(0),
                    "buck.schem");
        }
    }

    @DataProvider
    public Object[][] sparkConfigurationAllowWrite()
    {
        // {disable protection, max failures, speculation}
        return new Object[][] {{false, 1, false},
                {true, 1, false},
                {true, 2, false},
                {true, 1, true},
                {true, 2, true}};
    }

    @Test(dataProvider = "sparkConfigurationAllowWrite")
    public void testRequireSafeSparkConfigurationAllowWrite(final boolean disableSparkDuplicateWritesProtection,
                                                            final int maxFailures,
                                                            final boolean speculation)
            throws IOException
    {
        final MockUtils mockUtils = new MockUtils();
        final String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort,
                maxFailures, speculation,
                disableSparkDuplicateWritesProtection)) {
            final SparkConf sparkConfiguration = session
                    .sparkContext()
                    .getConf();
            assertTrue(sparkConfiguration.contains(
                    SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION));
            assertEquals(sparkConfiguration.getBoolean(
                            SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION, true),
                    disableSparkDuplicateWritesProtection);

            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab " + "(b boolean, i integer, r STRUCT<a: INTEGER, b: STRING>, v varchar(30), c char(40), d DATE, t timestamp)")
                    .show();
            session.sql("show columns from ndb.buck.schem.tab").show();
            session
                    .sql("insert into ndb.buck.schem.tab(b, i, r, v, c, d, t) values " + "(FALSE, 321, (3, 'structstr'), 'varcharstr', 'charstr', date '2008-11-11', timestamp '2008-11-09 15:45:21')")
                    .show();
        }
    }

    @DataProvider
    public Object[][] sparkConfigurationDenyWrite()
    {
        // {max failures, speculation}
        return new Object[][] {{1, true}, {2, false}, {2, true}};
    }

    @Test(dataProvider = "sparkConfigurationDenyWrite", expectedExceptions = RuntimeException.class)
    public void testRequireSafeSparkConfigurationDenyWrite(final int maxFailures,
                                                           final boolean speculation)
            throws Throwable
    {
        final MockUtils mockUtils = new MockUtils();
        final String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort,
                maxFailures, speculation, false)) {
            final SparkConf sparkConfiguration = session
                    .sparkContext()
                    .getConf();
            assertTrue(sparkConfiguration.contains(
                    SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION));
            assertFalse(sparkConfiguration.getBoolean(
                    SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION, true));

            assertFalse(session
                            .sparkContext()
                            .getConf()
                            .getBoolean(
                                    SETTING_DISABLE_SPARK_DUPLICATE_WRITES_PROTECTION,
                                    true),
                    "Default configuration expected to enable Spark duplicate writes protection");

            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab " + "(b boolean, i integer, r STRUCT<a: INTEGER, b: STRING>, v varchar(30), c char(40), d DATE, t timestamp)")
                    .show();
            session.sql("show columns from ndb.buck.schem.tab").show();
            session
                    .sql("insert into ndb.buck.schem.tab(b, i, r, v, c, d, t) values " + "(FALSE, 321, (3, 'structstr'), 'varcharstr', 'charstr', date '2008-11-11', timestamp '2008-11-09 15:45:21')")
                    .show();
        }
        catch (final RuntimeException error) {
            assertEquals(error.getMessage(),
                    String.format(FORMAT_UNSAFE_SPARK_CONFIGURATION,
                            maxFailures, speculation));
            assertTrue(Arrays
                    .stream(error.getStackTrace())
                    .anyMatch(element -> element
                            .getMethodName()
                            .equals("newWriteBuilder") && element
                            .getClassName()
                            .equals(VastTable.class.getCanonicalName())));
            assertEquals(error.getCause().getMessage(),
                    String.format(FORMAT_UNSAFE_SPARK_CONFIGURATION,
                            maxFailures, speculation));
            assertTrue(Arrays
                    .stream(error.getCause().getStackTrace())
                    .anyMatch(element -> element
                            .getMethodName()
                            .equals("<init>") && element
                            .getClassName()
                            .equals(SparkConfValidator.class.getCanonicalName())));
            throw error;
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
        Consumer<HttpExchange> action = httpExchange ->
        {
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
                session
                        .sql("create table ndb.buck.schem.`tab vast.import_data`(b boolean, i integer)")
                        .show();
                fail("expected failure - creating vast.import_data");
            }
            catch (Exception ignored) {
            }
            session
                    .sql("create table ndb.buck.schem.tab(b boolean, i integer)")
                    .show();
            session
                    .sql("insert into ndb.buck.schem.tab(b, i) values (FALSE, 321)")
                    .show();
            String valuesStr = "(TRUE, 123, 'file1/file')";
            String sql = format(
                    "insert into ndb.buck.schem.`tab vast.import_data(b, i)` " + "(b, i, `$parquet_file_path`) values %s",
                    valuesStr);
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
            session
                    .sql("create table ndb.buck.schem.tab (b boolean, i integer not null)")
                    .show();
        }
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = TypeUtil.NDB_CATALOG_DOES_NOT_SUPPORT_TYPES + " \\[i: Interval\\(YEAR_MONTH\\), d: Duration\\(MICROSECOND\\)]")
    public void testUnsupportedColumns()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (b boolean, i interval year, d interval second)")
                    .show();
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
            session
                    .sql("create table ndb.buck.schem.tab (a string, b integer, c double)")
                    .show();
            Dataset<Row> df = session
                    .sql("select * from ndb.buck.schem.tab")
                    .select("*");
            df.writeTo("ndb.buck.schem.tab2").create();
            session.sql("show tables from ndb.buck.schem").show();
            session.sql("select ndb.commit_tx()").show();
        }
        finally {
            assertEquals(
                    VastArrowAllocator.writeAllocator().getAllocatedMemory(),
                    0);
        }
    }

    //    enable for manual debugging
    @Test(enabled = false)
    public void testInsert()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        int noOfVals = 120000;

        String[] valuesArray = IntStream
                .range(0, noOfVals)
                .mapToObj(i -> format(
                        "(%s, %s, 'c%s', 's%s', cast(%s.001 as decimal(30,3)), %s.0f)",
                        i % 2 == 0, i, i, i, i, i))
                .toArray(String[]::new);
        String valuesStr = String.join(",", valuesArray);
        HashMap<String, Object> extraConf = new HashMap<>();
        extraConf.put("spark.ndb.max_row_count_per_insert", 2777);
        try (SparkSession session = SparkTestUtils.getSession(testPort,
                extraConf)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (b boolean, i integer, c char(20), s varchar(20), d decimal(30,3), f float)")
                    .show();
            String format = format("insert into ndb.buck.schem.tab values %s",
                    valuesStr);
            session.sql(format).show();
            String path = "buck/schem/tab";
            testGracefulVastException(session, format, path, POST,
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidBucketState</Code><Message>The request is not valid with the current state of the bucket.</Message><Resource>aresource</Resource><RequestId>a00100000006</RequestId></Error>",
                    409, VastConflictException.class);
            testGracefulVastException(session, format, path, POST, "Forbidden",
                    403, VastUserException.class);
        }
        assertEquals(VastArrowAllocator.writeAllocator().getAllocatedMemory(),
                0);
    }

    @Test(enabled = false)
    public void testInsertTSTypes()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab as select timestamp '2008-11-09 15:45:21' as ts, timestamp_ntz '2008-11-09 15:45:21' as tsntz")
                    .show();
            session.sql("describe table ndb.buck.schem.tab").show();
        }
        assertEquals(VastArrowAllocator.writeAllocator().getAllocatedMemory(),
                0);
    }

    @Test(enabled = true)
    public void testInsertDecimal()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        String valuesStr = "(3.001, cast(3.001 as decimal(20,3)))";
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (d10 decimal(10,3), d20 decimal(20,3))")
                    .show();
            String format = format("insert into ndb.buck.schem.tab values %s",
                    valuesStr);
            session.sql(format).show();
        }
    }

    @Test(expectedExceptions = AnalysisException.class)
    public void testInsertStringAsTS_ORION189489()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (id int, ts_string timestamp, ts_bool timestamp)")
                    .show();
            String format = "insert into ndb.buck.schem.tab values (5, 'a', true)";
            session.sql(format).show();
        }
    }

    //    Unitest base for https://vastdata.atlassian.net/browse/ORION-172812
    @Test(enabled = false)
    public void testInsertLargeStrings()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        char charToAppend = 'a';
        char[] charArray = new char[20];
        Arrays.fill(charArray, charToAppend);
        String newString = new String(charArray);
        String[] values = new String[100000];
        Arrays.fill(values, format("('%s', '%s')", newString, newString));
        String valuesStr = join(",", values);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (s string, vc varchar(40))")
                    .show();
            session
                    .sql(format("insert into ndb.buck.schem.tab values %s",
                            valuesStr))
                    .show();
        }
    }

    @Test
    public void testInsertPlan()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (i integer, b boolean)")
                    .show();
            session
                    .sql("insert into ndb.buck.schem.tab values (1, true), (2, false)")
                    .show();
        }
    }

    @Test(expectedExceptions = AnalysisException.class, expectedExceptionsMessageRegExp = ".*PARTITION_MANAGEMENT_IS_UNSUPPORTED.*")
    public void testDropPartition()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (b boolean, i integer)")
                    .show();
            session
                    .sql("alter table ndb.buck.schem.`tab vast.allow_non_acid` drop partition (i = 5)")
                    .show();
        }
    }

    @Test
    public void testAlterTable()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (b boolean, i integer, s string, c char(5))")
                    .show();
            session
                    .sql("alter table ndb.buck.schem.tab drop columns (s, c)")
                    .show();
            try {
                session
                        .sql("alter table ndb.buck.schem.tab drop column vastdb_spark_row_id")
                        .show();
            }
            catch (Exception e) {
                assertTrue(e
                        .getMessage()
                        .contains(
                                "Missing field vastdb_spark_row_id in table"));
                assertTrue(e instanceof AnalysisException,
                        format("Unexpected exception: %s", e));
            }
            try {
                session
                        .sql("alter table ndb.buck.schem.tab add column (vastdb_spark_row_id bigint)")
                        .show();
            }
            catch (RuntimeException e) {
                assertTrue(e
                        .getMessage()
                        .contains("Adding vastdb_spark_row_id is not allowed"));
            }
            try {
                session
                        .sql("alter table ndb.buck.schem.tab replace columns (bi bigint, vastdb_spark_row_id bigint)")
                        .show();
            }
            catch (Exception e) {
                assertTrue(e
                        .getMessage()
                        .contains("Adding vastdb_spark_row_id is not allowed"));
            }
            session
                    .sql("alter table ndb.buck.schem.tab replace columns (bi bigint)")
                    .show();
            StructType schema = session
                    .read()
                    .format("ndb")
                    .option("table", "ndb.buck.schem.tab")
                    .load()
                    .schema();
            StructField biField = StructField.apply("bi", DataTypes.LongType,
                    true, Metadata.empty());
            StructField[] expectedFields = new StructField[] {biField};
            assertEquals(schema, new StructType(expectedFields));
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Failed renaming table - changing bucket is not supported.*", dataProvider = "badRenameTableQueries")
    public void testRenameBucketIsNotSupported(String badRenameQuery)
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (b boolean, i integer, s string, c char(5))")
                    .show();

            session.sql(badRenameQuery).show();
        }
    }

    @DataProvider(name = "badRenameTableQueries")
    public Object[][] badRenameTableQueriesData()
    {
        return new Object[][] {{
                "alter table ndb.buck.schem.tab rename to buck2.schem.newtable"},
                {"alter table ndb.buck.schem.tab rename to buck2.schem.newtable"},
                {"alter table ndb.buck.schem.tab rename to buck2.schem1.tab"}};
    }

    @Test(expectedExceptions = SparkException.class, expectedExceptionsMessageRegExp = ".*QueryData.*")
    public void testDelete()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (b boolean, i integer, s string, d decimal(10,2))")
                    .show();
            session
                    .sql("delete from ndb.buck.schem.tab where cos(i) > 0.3")
                    .show();
        }
    }

    @Test(expectedExceptions = SparkException.class, expectedExceptionsMessageRegExp = ".*QueryData.*")
    public void testSQLUpdate()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (b boolean, i integer, s string, d decimal(10,2), c char(5))")
                    .show();
            session
                    .sql("update ndb.buck.schem.tab set b = false where i > 0")
                    .explain(true);
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
            session
                    .sql("create table ndb.buck.schem.tab (i integer, b boolean)")
                    .show();
            testProjectOptimizationOutput(session
                    .sql("SELECT DISTINCT i + 1 from ndb.buck.schem.tab where i > 126 and b is not null")
                    .queryExecution()
                    .executedPlan(), "(i + 1)", false, Optional.empty());

            testProjectOptimizationOutput(session
                    .sql("SELECT DISTINCT i as alias1 from ndb.buck.schem.tab where i > 126")
                    .queryExecution()
                    .executedPlan(), "alias1", false, Optional.empty());
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
            session
                    .sql("create table ndb.buck.schem.tab (i integer, b boolean)")
                    .show();
            Optional<Set<String>> filterColumnNames = Optional.of(
                    ImmutableSet.of("i"));
            testProjectOptimizationOutput(session
                    .sql("SELECT DISTINCT i + 1 from ndb.buck.schem.tab where cos(i) > 0.3 and b is not null")
                    .queryExecution()
                    .executedPlan(), "(i + 1)", true, filterColumnNames);

            testProjectOptimizationOutput(session
                    .sql("SELECT DISTINCT i as alias1 from ndb.buck.schem.tab where cos(i) > 0.3")
                    .queryExecution()
                    .executedPlan(), "alias1", true, filterColumnNames);
        }
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
            session
                    .sql("create table ndb.buck.schem.tab (b boolean, i integer, s string, d decimal(10,2))")
                    .show();
            session
                    .sql("select s from ndb.buck.schem.tab where substr(s, 1, 3) in ('aa', 'bb', 'cc')")
                    .explain("cost");
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
            session.sql(
                    "create table ndb.buck.schem.tab (b boolean, i integer, d double)");
            session
                    .sql("select * from ndb.buck.schem.tab where i > 0")
                    .explain("cost");
            session
                    .sql("select i as i_alias, b as b_alias from ndb.buck.schem.tab where i > 0")
                    .explain("cost");
            session
                    .sql("select count(*) from ndb.buck.schem.tab where i > 0")
                    .explain("cost");
            session
                    .sql("select i, i + 1, 0.0 as proj1, (i > 10 and b) as proj2 from ndb.buck.schem.tab where i > 0")
                    .explain("cost");
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
            session.sql(
                    "create table ndb.buck.schem.tab (b boolean, i integer, d double)");
            SparkPlan sparkPlan = session
                    .sql("select i, i + 1, 0.0 as proj1, (i > 10 and b) as proj2 from ndb.buck.schem.tab where i > 0")
                    .queryExecution()
                    .executedPlan();
            SparkPlan last = sparkPlan.collectLeaves().last();
            assertTrue(last instanceof BatchScanExec,
                    format("last was of class: %s", last.getClass()));
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
            session.sql(
                    "create table ndb.buck.schem.tab (b boolean, i integer, s string)");
            scala.collection.Seq<SparkPlan> sparkPlanSeq = session
                    .sql("select count(*) from ndb.buck.schem.tab where (i%2 > 0 or b) and s is not null")
                    .queryExecution()
                    .executedPlan()
                    .collectLeaves();
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
            assertTrue(child instanceof FilterExec,
                    format("Expected FilterExec node, but child was actually: %s",
                            child.getClass()));
            FilterExec filter = (FilterExec) child;
            child = filter.child();
            assertTrue(child instanceof BatchScanExec,
                    format("Expected BatchScanExec node, but child was actually: %s",
                            child.getClass()));
            scala.collection.Seq<Attribute> output = child.output();
            assertEquals(output.size(), 2);
            HashSet<String> expectedNames = new HashSet<>();
            expectedNames.add("i");
            expectedNames.add("b");
            assertTrue(expectedNames.remove(output.apply(0).name()));
            assertTrue(expectedNames.remove(output.apply(1).name()));
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
            session.sql(
                    "create table ndb.buck.schem.tab (b boolean, i integer)");
            session
                    .sql("select count(b) from ndb.buck.schem.tab where i = 0 or i > 1 and i <= 3 ")
                    .show();
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
            session
                    .sql("select * from ndb.buck.schem.tab where b = 'abcd'")
                    .explain("cost");
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
            session.sql(
                    "create table ndb.buck.schem.tab (a integer, b integer)");
            session
                    .sql("select * from ndb.buck.schem.tab where a in (4, 7, 9) and b in (1, 3, 5)")
                    .explain("cost");
        }
    }

    @Test
    public void testSort1()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem");
            session.sql(
                    "create table ndb.buck.schem.tab (col_b boolean, col_i integer)");
            SparkPlan sparkPlan = session
                    .sql("select * from ndb.buck.schem.tab order by 1")
                    .queryExecution()
                    .executedPlan();
            SparkPlan last = sparkPlan.collectLeaves().last();
            last.output().foreach(o ->
            {
                assertFalse(o.name().contains(SPARK_INT64_ROW_ID_FIELD.name()));
                return null;
            });
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
            session.sql(
                    "create table ndb.buck.schem.item (i_product_name string, i_manufact_id integer, i_manufact string, i_size string, i_units string, i_color string, i_category string)");
            session
                    .sql("SELECT DISTINCT i_product_name\n" + "FROM\n" + "  ndb.buck.schem.item i1\n" + "WHERE (i_manufact_id BETWEEN 738 AND (738 + 40))\n" + "   AND ((\n" + "      SELECT count(*) item_cnt\n" + "      FROM\n" + "        ndb.buck.schem.item\n" + "      WHERE ((i_manufact = i1.i_manufact)\n" + "            AND (((i_category = 'Women')\n" + "                  AND ((i_color = 'powder')\n" + "                     OR (i_color = 'khaki'))\n" + "                  AND ((i_units = 'Ounce')\n" + "                     OR (i_units = 'Oz'))\n" + "                  AND ((i_size = 'medium')\n" + "                     OR (i_size = 'extra large')))\n" + "               OR ((i_category = 'Women')\n" + "                  AND ((i_color = 'brown')\n" + "                     OR (i_color = 'honeydew'))\n" + "                  AND ((i_units = 'Bunch')\n" + "                     OR (i_units = 'Ton'))\n" + "                  AND ((i_size = 'N/A')\n" + "                     OR (i_size = 'small')))\n" + "               OR ((i_category = 'Men')\n" + "                  AND ((i_color = 'floral')\n" + "                     OR (i_color = 'deep'))\n" + "                  AND ((i_units = 'N/A')\n" + "                     OR (i_units = 'Dozen'))\n" + "                  AND ((i_size = 'petite')\n" + "                     OR (i_size = 'large')))\n" + "               OR ((i_category = 'Men')\n" + "                  AND ((i_color = 'light')\n" + "                     OR (i_color = 'cornflower'))\n" + "                  AND ((i_units = 'Box')\n" + "                     OR (i_units = 'Pound'))\n" + "                  AND ((i_size = 'medium')\n" + "                     OR (i_size = 'extra large')))))\n" + "         OR ((i_manufact = i1.i_manufact)\n" + "            AND (((i_category = 'Women')\n" + "                  AND ((i_color = 'midnight')\n" + "                     OR (i_color = 'snow'))\n" + "                  AND ((i_units = 'Pallet')\n" + "                     OR (i_units = 'Gross'))\n" + "                  AND ((i_size = 'medium')\n" + "                     OR (i_size = 'extra large')))\n" + "               OR ((i_category = 'Women')\n" + "                  AND ((i_color = 'cyan')\n" + "                     OR (i_color = 'papaya'))\n" + "                  AND ((i_units = 'Cup')\n" + "                     OR (i_units = 'Dram'))\n" + "                  AND ((i_size = 'N/A')\n" + "                     OR (i_size = 'small')))\n" + "               OR ((i_category = 'Men')\n" + "                  AND ((i_color = 'orange')\n" + "                     OR (i_color = 'frosted'))\n" + "                  AND ((i_units = 'Each')\n" + "                     OR (i_units = 'Tbl'))\n" + "                  AND ((i_size = 'petite')\n" + "                     OR (i_size = 'large')))\n" + "               OR ((i_category = 'Men')\n" + "                  AND ((i_color = 'forest')\n" + "                     OR (i_color = 'ghost'))\n" + "                  AND ((i_units = 'Lb')\n" + "                     OR (i_units = 'Bundle'))\n" + "                  AND ((i_size = 'medium')\n" + "                     OR (i_size = 'extra large')))))\n" + "   ) > 0)\n" + "ORDER BY i_product_name ASC\n" + "LIMIT 100")
                    .explain("cost");
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
            session
                    .sql("select * from ndb.buck.schem.tab where instr(s, 'bla') > 0")
                    .explain("cost");
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
            String nodePlan = session
                    .sql("select * from ndb.buck.schem.tab where x > 5")
                    .queryExecution()
                    .executedPlan()
                    .collectLeaves()
                    .head()
                    .toString();
            assertTrue(nodePlan.contains("x > 5"));
            nodePlan = session
                    .sql("select * from ndb.buck.schem.tab where x in (1, 2, 3)")
                    .queryExecution()
                    .executedPlan()
                    .collectLeaves()
                    .head()
                    .toString();
            assertTrue(nodePlan.contains("x = 1") && nodePlan.contains(
                    "x = 2") && nodePlan.contains("x = 3"));
        }
    }

    @Test
    public void testStatisticsInjection()
            throws IOException
    {
        StructField x1 = new StructField("x1", IntegerType$.MODULE$, true,
                Metadata.empty());
        StructField x2 = new StructField("x2", IntegerType$.MODULE$, true,
                Metadata.empty());
        StructType schema1 = new StructType(new StructField[] {x1, x2});
        VastTable t1 = new VastTable(vastCatalogUtils, "buck/schem", "t1", "id",
                schema1, new Transform[0], null, false, Optional.empty(),
                Collections.emptyMap());
        StructField y1 = new StructField("y1", IntegerType$.MODULE$, true,
                Metadata.empty());
        StructField y2 = new StructField("y2", IntegerType$.MODULE$, true,
                Metadata.empty());
        StructType schema2 = new StructType(new StructField[] {y1, y2});
        VastTable t2 = new VastTable(vastCatalogUtils, "buck/schem", "t2", "id",
                schema2, new Transform[0], null, false, Optional.empty(),
                Collections.emptyMap());
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.conf().set("spark.sql.cbo.enabled", true);
            session.conf().set("spark.sql.cbo.joinReorder.enabled", true);
            session.conf().set("spark.sql.cbo.planStats.enabled", true);
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.t1 (x1 integer, x2 integer)")
                    .show();
            session
                    .sql("create table ndb.buck.schem.t2 (y1 integer, y2 integer)")
                    .show();
            SparkVastStatisticsManager.getInstance().deleteTableStatistics(t1);
            SparkVastStatisticsManager.getInstance().deleteTableStatistics(t2);
            Tuple2<Attribute, ColumnStat> x1stat = getColumnStats(0, 9, 10, 0,
                    "x1", IntegerType$.MODULE$, 0);
            Tuple2<Attribute, ColumnStat> x2stat = getColumnStats(0, 9, 10, 0,
                    "x2", IntegerType$.MODULE$, 1);
            Tuple2<Attribute, ColumnStat> y1stat = getColumnStats(0, 99, 100, 0,
                    "y1", IntegerType$.MODULE$, 0);
            Tuple2<Attribute, ColumnStat> y2stat = getColumnStats(0, 99, 100, 0,
                    "y2", IntegerType$.MODULE$, 1);
            AttributeMap<ColumnStat> t1Stats = getColumnStatsAttrMap(
                    ImmutableSet.of(x1stat, x2stat));
            AttributeMap<ColumnStat> t2Stats = getColumnStatsAttrMap(
                    ImmutableSet.of(y1stat, y2stat));
            Statistics t1MockStats = new Statistics(BigInt.apply(40),
                    Option.empty(), t1Stats, false);
            Statistics t2MockStats = new Statistics(BigInt.apply(400),
                    Option.empty(), t2Stats, false);
            SparkVastStatisticsManager
                    .getInstance()
                    .setTableStatistics(t1, t1MockStats);
            SparkVastStatisticsManager
                    .getInstance()
                    .setTableStatistics(t2, t2MockStats);
            session
                    .sql("select t1.x1 from ndb.buck.schem.t1 join ndb.buck.schem.t2 on t1.x1 = t2.y1")
                    .explain("cost");
        }
    }

    private Tuple2<Attribute, ColumnStat> getColumnStats(Object min,
                                                         Object max,
                                                         Integer distinctCount,
                                                         Integer nullCount,
                                                         String name,
                                                         DataType type,
                                                         int fieldIndex)
    {
        StructField field = new StructField(name, type, true, Metadata.empty());
        return buildTup(fieldIndex, field, min, max, distinctCount, nullCount);
    }

    private Tuple2<Attribute, ColumnStat> buildTup(int fieldIndex,
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
        ColumnStat colStats = new ColumnStat(distinctCount,
                Option.apply(minValue), Option.apply(maxValue), nullCount,
                avgLen, maxLen, Option.empty(), 0);
        Attribute attribute = new AttributeReference(field.name(),
                field.dataType(), field.nullable(), field.metadata(),
                ExprId.apply(fieldIndex),
                (Seq<String>) Seq$.MODULE$.<String>empty());
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
            session
                    .sql("create table ndb.buck.schem.tab (k integer, d date)")
                    .collect();
            session
                    .sql("select * from ndb.buck.schem.tab t1 JOIN ndb.buck.schem.tab t2 ON t1.k = t2.k WHERE t2.d BETWEEN '2020-02-02' AND '2020-02-22'")
                    .explain("cost");
            session
                    .sql("select * from ndb.buck.schem.tab t1 JOIN ndb.buck.schem.tab t2 ON t1.k = t2.k WHERE t2.k in (1,2,3)")
                    .explain("cost");
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
            session
                    .sql("create table ndb.buck.schem.tabdate (x date)")
                    .collect();
            java.util.List<String> dates = IntStream
                    .range(10, 31)
                    .mapToObj(i -> format("CAST('1970-08-%s' AS DATE)", i))
                    .collect(Collectors.toList());
            String values = String.join(",", dates);
            String sql = format(
                    "select count(*) from ndb.buck.schem.tabdate where x in (%s)",
                    values);
            String expectedPushDown = "pushed_down_predicates=[[(x >= 221) AND (x <= 241)]]"; // 221 = 10-08-1970, 241 = 30-08-1970
            testCompactFilterPushdown(session, sql, expectedPushDown);

            session.sql("create table ndb.buck.schem.tab (x bigint)").collect();
            java.util.List<String> zeros = Collections.nCopies(60, "0");
            values = String.join(",", zeros) + ",7, 0, 62";
            sql = format(
                    "select count(*) from ndb.buck.schem.tab where x in (%s)",
                    values);
            expectedPushDown = "pushed_down_predicates=[[x = 0, x = 7, x = 62]]";
            testCompactFilterPushdown(session, sql, expectedPushDown);

            sql = "select count(*) from ndb.buck.schem.tab where x >= 0 and x <= 99";
            expectedPushDown = "pushed_down_predicates=[[x IS NOT NULL], [x >= 0], [x <= 99]]";
            testCompactFilterPushdown(session, sql, expectedPushDown);

            values = IntStream
                    .range(0, 60)
                    .mapToObj(i -> format("%s", i))
                    .collect(Collectors.joining(","));
            sql = format(
                    "select count(*) from ndb.buck.schem.tab where x in (%s)",
                    values);
            expectedPushDown = "pushed_down_predicates=[[(x >= 0) AND (x <= 59)]]";
            testCompactFilterPushdown(session, sql, expectedPushDown);

            values = IntStream
                    .range(0, 4)
                    .mapToObj(i -> format("%s", i))
                    .collect(Collectors.joining(","));
            sql = format(
                    "select count(*) from ndb.buck.schem.tab where x in (%s)",
                    values);
            expectedPushDown = "pushed_down_predicates=[[x = 0, x = 1, x = 2, x = 3]]";
            testCompactFilterPushdown(session, sql, expectedPushDown);

            values = format("%s, %s, %s", Long.MAX_VALUE, -Long.MAX_VALUE, 0L);
            sql = format(
                    "select count(*) from ndb.buck.schem.tab where x in (%s)",
                    values);
            expectedPushDown = "pushed_down_predicates=[[x = 9223372036854775807, x = -9223372036854775807, x = 0]]";
            testCompactFilterPushdown(session, sql, expectedPushDown);

            session
                    .sql("create table ndb.buck.schem.tab_float (x float)")
                    .collect();
            values = IntStream
                    .range(0, 4)
                    .mapToObj(i -> format("%s.5", i))
                    .collect(Collectors.joining(","));
            sql = format(
                    "select count(*) from ndb.buck.schem.tab_float where x in (%s)",
                    values);
            expectedPushDown = "pushed_down_predicates=[[x = 0.5, x = 1.5, x = 2.5, x = 3.5]]";
            testCompactFilterPushdown(session, sql, expectedPushDown);

            values = IntStream
                    .range(0, 60)
                    .mapToObj(i -> format("%s.5", i))
                    .collect(Collectors.joining(","));
            sql = format(
                    "select count(*) from ndb.buck.schem.tab_float where x in (%s)",
                    values);
            expectedPushDown = "x = 14.5"; // predicates are not sorted
            testCompactFilterPushdown(session, sql, expectedPushDown);
        }
    }

    @Test
    public void testSparkStatisticsFallbackToTableLevelStats()
            throws VastException
    {
        long numRows = 70000L;
        long sizeInBytes = 280000L;
        Statistics tableStatistics = new Statistics(BigInt.apply(sizeInBytes),
                Option.apply(BigInt.apply(numRows)),
                AttributeMap$.MODULE$.empty(), false);
        VastStatistics vastStatistics = new VastStatistics(numRows,
                sizeInBytes);
        when(mockClient.s3GetObj(anyString(), anyString())).thenReturn(
                Optional.empty());
        when(mockClient.getTableStats(any(), anyString(), anyString(),
                nullable(String.class))).thenReturn(vastStatistics);
        Supplier<VastClient> supplier = () -> mockClient;
        StructField charNField = new StructField("x", IntegerType$.MODULE$,
                true, Metadata.empty());
        StructType schema = new StructType(new StructField[] {charNField});
        VastTable table = new VastTable(vastCatalogUtils, "buck/schem", "tab",
                "id", schema, new Transform[0], supplier, false,
                Optional.empty(), Collections.emptyMap());
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("show schemas from ndb").show();
            SparkPersistentStatistics sparkPersistentStatistics = new SparkPersistentStatistics(
                    mockClient, NDB.getConfig());
            Optional<Statistics> newTableStatistics = sparkPersistentStatistics.getTableStatistics(
                    table);
            assertEquals(Optional.of(tableStatistics), newTableStatistics);
        }
    }

    @Test(dataProvider = "statisticsLowerSizeEstimationFactors")
    public void testStatisticsLowerSizeEstimation(final long factorReciprocal,
                                                  final long precisionReciprocal)
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem");
            session.sql("create table ndb.buck.schem.tab (b BINARY)");
            final String query = "select * from ndb.buck.schem.tab where b = 'abcd'";
            final Statistics baseStatistics = overrideEstimationFactor(session,
                    query, 1.0);
            final double precision = 1.0 / precisionReciprocal;
            final long epsilonRowCount = (long) (baseStatistics
                    .rowCount()
                    .get()
                    .toLong() * precision);
            final long epsilonSizeInBytes = (long) (baseStatistics
                    .sizeInBytes()
                    .toLong() * precision);
            final Statistics statistics = overrideEstimationFactor(session,
                    query, 1.0 / factorReciprocal);
            assertClose(baseStatistics.rowCount().get().toLong(),
                    statistics.rowCount().get().toLong() * factorReciprocal,
                    epsilonRowCount);
            assertClose(baseStatistics.sizeInBytes().toLong(),
                    statistics.sizeInBytes().toLong() * factorReciprocal,
                    epsilonSizeInBytes);
        }
    }

    @DataProvider
    private Long[][] statisticsLowerSizeEstimationFactors()
    {
        return new Long[][] {{2L, 10_000_000L},
                {3L, 10_000_000L},
                {1024L, 100_000L}};
    }

    @Test
    public void testTxKeepAlive()
            throws IOException, InterruptedException
    {
        Map<VastTransaction, AtomicInteger> activeTxDuringInsert = new ConcurrentHashMap<>();
        String message = "bad request";
        Consumer<HttpExchange> insertSleeper = httpExchange ->
        {
            try {
                Thread.sleep(3 * 1000);
                activeTxDuringInsert.putAll(JobEventService
                        .getInstance()
                        .orElseThrow(IllegalStateException::new)
                        .getActiveTransactions());
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
        Consumer<HttpExchange> getTxCtrAction = httpExchange ->
        {
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
        Consumer<HttpExchange> commitTxAction = httpExchange ->
        {
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
            Optional<SparkListenerInterface> any = session
                    .sparkContext()
                    .listenerBus()
                    .listeners()
                    .stream()
                    .filter(l -> l instanceof NDBJobsListener)
                    .findAny();
            assertTrue(any.isPresent());
            session.sql("create database ndb.buck.schem").collect();
            session.sql("create table ndb.buck.schem.tab (i integer)").show();
            handler.setHook("/buck/schem/tab", POST, insertSleeper);
            handler.setHook("/", GET, getTxCtrAction);
            handler.setHook("/", PUT, commitTxAction);
            try {
                session
                        .sql("insert into ndb.buck.schem.tab values (1), (2), (3)")
                        .show();
            }
            catch (Exception se) {
                assertTrue(se.getMessage().contains("Failed inserting rows"),
                        format("%s", se));
            }
            assertEquals(activeTxDuringInsert.size(), 1,
                    format("activeTxDuringInsert: %s", activeTxDuringInsert));
            Thread.sleep(1100); // 1s is the keep alive interval
            Map<VastTransaction, AtomicInteger> activeTx = JobEventService
                    .getInstance()
                    .orElseThrow(IllegalStateException::new)
                    .getActiveTransactions();
            assertTrue(activeTx.isEmpty(),
                    format("activeTransactions: %s", activeTx));
            session.sql("select ndb.commit_tx()").show();
            activeTx = JobEventService
                    .getInstance()
                    .orElseThrow(IllegalStateException::new)
                    .getActiveTransactions();
            assertTrue(activeTx.isEmpty(),
                    format("activeTransactions: %s", activeTx));
            assertTrue(getTxCtr.get() > 0);
        }
    }

    @Test
    public void testSparkImplicitTransactionLister_ORION292984()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        AtomicInteger putCounter = new AtomicInteger(0);
        Consumer<HttpExchange> putResponder = httpExchange ->
        {
            try {
                putCounter.incrementAndGet();
                httpExchange.sendResponseHeaders(200, 0);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(new byte[0]);
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem");
            session.sql("create table ndb.buck.schem.tab (s string)");
            handler.setHook("/", PUT, putResponder);
            int expectedCommits = 0;
            assertThatThrownBy(() -> session
                    .sql("select * from ndb.buck.schem.tab where s is not null")
                    .show())
                    .isInstanceOf(SparkException.class)
                    .hasMessageContaining("QueryData");
            session
                    .sql("select now()")
                    .show(); // to make sure no race on the commit counter after the exception
            expectedCommits += 4; // load table, row-col-security *2, batch
            assertEquals(putCounter.get(), expectedCommits);
            assertThatThrownBy(
                    () -> session.read().table("ndb.buck.schem.tab").show())
                    .isInstanceOf(SparkException.class)
                    .hasMessageContaining("QueryData");
            session.sql("select now()").collect();
            expectedCommits += 4; // load table, row-col-security *2, batch
            assertEquals(putCounter.get(), expectedCommits);
            Dataset<Row> ds = session.sql(
                    "select * from ndb.buck.schem.tab where s is not null");
            expectedCommits += 2; // load table + rcls
            assertEquals(putCounter.get(), expectedCommits);
            System.out.printf("%s: %s%n", ds.hashCode(),
                    Arrays.toString(ds.columns()));
            assertThatThrownBy(ds::count)
                    .isInstanceOf(SparkException.class)
                    .hasMessageContaining("QueryData");
            session.sql("select now()").collect();
            expectedCommits += 2; // batch + rcls
            assertEquals(putCounter.get(),
                    expectedCommits); // single batch + load table
            session.sql("select ndb.create_tx()").show();
            assertThatThrownBy(() -> session
                    .sql("select * from ndb.buck.schem.tab where s is not null")
                    .show())
                    .isInstanceOf(SparkException.class)
                    .hasMessageContaining("QueryData");
            assertThatThrownBy(
                    () -> session.read().table("ndb.buck.schem.tab").show())
                    .isInstanceOf(SparkException.class)
                    .hasMessageContaining("QueryData");
            ds = session.sql(
                    "select * from ndb.buck.schem.tab where s is not null");
            System.out.printf("%s: %s%n", ds.hashCode(),
                    Arrays.toString(ds.columns()));
            assertThatThrownBy(ds::count)
                    .isInstanceOf(SparkException.class)
                    .hasMessageContaining("QueryData");
            session.sql("select ndb.commit_tx()").show();
            assertEquals(putCounter.get(), ++expectedCommits); // no commits
        }
    }

    @DataProvider(name = "nan-not-pushdown-test-cases")
    public Object[][] nanNotPushdownTestCases()
    {
        return new Object[][] {{"f = 'nan'", "\\(f#\\d+ = NaN\\)"},
                {"f != 'nan'", "NOT \\(f#\\d+ = NaN\\)"},
                {"f <> 'nan'", "NOT \\(f#\\d+ = NaN\\)"},
                {"f < 'nan'", "\\(f#\\d+ < NaN\\)"},
                {"f > 'nan'", "\\(f#\\d+ > NaN\\)"},
                {"f <= 'nan'", "\\(f#\\d+ <= NaN\\)"},
                {"f >= 'nan'", "\\(f#\\d+ >= NaN\\)"}};
    }

    @Test(dataProvider = "nan-not-pushdown-test-cases")
    public void testNanNotPushedDown(String whereString, String postFilterRegex)
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            session.sql("create database ndb.buck.schem").show();
            session.sql("create table ndb.buck.schem.tab (f float)").show();

            testPushedDownPredicates(session
                            .sql(format(
                                    "select distinct f from ndb.buck.schem.tab where %s",
                                    whereString))
                            .queryExecution()
                            .executedPlan(), "f IS NOT NULL",
                    Optional.of(postFilterRegex));
        }
    }

    @Test
    public void testSplitMultiplierConf()
            throws IOException, VastUserException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        HashMap<String, Object> extraConf = new HashMap<>();
        extraConf.put("spark.ndb.split_size_multiplier", 7);
        try (SparkSession session = SparkTestUtils.getSession(testPort,
                extraConf)) {
            assertEquals(NDB.getConfig().getSplitSizeMultiplier(), 7);
        }
    }

    @Test
    public void testSortByCreateTable()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (f float) TBLPROPERTIES ( 'sorted_by' = 'f')")
                    .show();
            session.sql("drop table ndb.buck.schem.tab").show();
        }
    }

    @Test
    public void testPreventSortByAlterTableWhenTableAlreadySorted()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (f float, g float) TBLPROPERTIES ( 'sorted_by' = 'f')")
                    .show();
            catchThrowableOfType(() -> session
                    .sql("ALTER TABLE ndb.buck.schem.tab SET TBLPROPERTIES ('sorted_by' = 'g')")
                    .show(), VastRuntimeException.class);
        }
    }

    @Test
    public void testPreventSortByAlterTableForNonExistColumn()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("select ndb.create_tx()").show();
            session.sql("create database ndb.buck.schem").show();
            session
                    .sql("create table ndb.buck.schem.tab (f float, g float)")
                    .show();
            catchThrowableOfType(() -> session
                    .sql("ALTER TABLE ndb.buck.schem.tab SET TBLPROPERTIES ('sorted_by' = 'not_exist')")
                    .show(), VastRuntimeException.class);
        }
    }

    @Test(enabled = false)
    public void testShowCreateTable()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck_create";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck_create.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck_create.schem.tab1 (b BOOLEAN, i1 INTEGER)")
                    .show();
            Row[] result = (Row[]) session
                    .sql("SHOW CREATE TABLE ndb.buck_create.schem.tab1")
                    .collect();
            assertThat(result[0].get(0).toString()).doesNotContain(
                    "'sorted_by'");
        }
    }

    @Test(enabled = false)
    public void testShowCreateTableWithSortedColumns()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck_create";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck_create.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck_create.schem.tab1 (b BOOLEAN, i1 INTEGER) TBLPROPERTIES ( 'sorted_by' = 'i1')")
                    .show();
            Row[] result = (Row[]) session
                    .sql("SHOW CREATE TABLE ndb.buck_create.schem.tab1")
                    .collect();
            assertThat(result[0].get(0).toString()).contains("'sorted_by'");
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Loading table failed during fetching table info for identifier tab1___VAST_PARTITIONS.*")
    public void testPITScan()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck_create";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck_create.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck_create.schem.tab1 (b BOOLEAN, i1 INTEGER) TBLPROPERTIES ( 'sorted_by' = 'i1')")
                    .show();
            session
                    .sql("select * from ndb.buck_create.schem.tab1.partitions")
                    .show();
        }
    }

    @Test
    public void testCreateTableElysiumNegativeScenarios()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").collect();
            VastRuntimeException e1 = expectThrows(VastRuntimeException.class,
                    () ->
                    {
                        session
                                .sql("create table ndb.buck.schem.tab (i integer) TBLPROPERTIES ('sorted_by' = 'i,i')")
                                .collect();
                    });
            assertTrue(e1
                            .getMessage()
                            .contains("Each column can only appear once in sorted_by"),
                    format("Expected duplicate error, got: %s",
                            e1.getMessage()));
            VastRuntimeException e2 = expectThrows(VastRuntimeException.class,
                    () ->
                    {
                        session
                                .sql("create table ndb.buck.schem.tab (i integer) TBLPROPERTIES ('sorted_by' = 'i,I')")
                                .collect();
                    });
            assertTrue(e2.getMessage().contains("does not exist in the table"),
                    format("Expected existence error for 'I', got: %s",
                            e2.getMessage()));
            VastRuntimeException e3 = expectThrows(VastRuntimeException.class,
                    () ->
                    {
                        session
                                .sql("create table ndb.buck.schem.tab (i integer) TBLPROPERTIES ('sorted_by' = ' ')")
                                .collect();
                    });
            assertTrue(e3.getMessage().contains("does not exist in the table"),
                    format("Expected existence error for empty string, got: %s",
                            e3.getMessage()));
            VastRuntimeException e4 = expectThrows(VastRuntimeException.class,
                    () ->
                    {
                        session
                                .sql("create table ndb.buck.schem.tab (i integer) TBLPROPERTIES ('sorted_by' = 'I')")
                                .collect();
                    });
            assertTrue(e4.getMessage().contains("does not exist in the table"),
                    format("Expected existence error for 'I', got: %s",
                            e4.getMessage()));
        }
    }

    @Test
    public void testCreateTableElysiumPositiveScenarios()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").collect();
            session
                    .sql("create table ndb.buck.schem.tab_pos1 (i integer, j integer)")
                    .collect();
            session
                    .sql("create table ndb.buck.schem.tab_pos2 (i integer, j integer) TBLPROPERTIES ('sorted_by' = 'i')")
                    .collect();
            session
                    .sql("create table ndb.buck.schem.tab_pos3 (i integer, j integer) TBLPROPERTIES ('sorted_by' = 'j, i')")
                    .collect();
        }
    }

    @Test
    public void testAlterTableElysiumNegativeScenarios()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck_alter_neg";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck_alter_neg.schem").collect();
            session
                    .sql("create table ndb.buck_alter_neg.schem.tab (i integer, j integer)")
                    .collect();
            VastRuntimeException e1 = expectThrows(VastRuntimeException.class,
                    () ->
                    {
                        session
                                .sql("ALTER TABLE ndb.buck_alter_neg.schem.tab SET TBLPROPERTIES ('sorted_by' = 'i,i')")
                                .collect();
                    });
            assertTrue(e1
                            .getMessage()
                            .contains("Each column can only appear once in sorted_by"),
                    "Actual error message was: " + e1.getMessage());
            VastRuntimeException e2 = expectThrows(VastRuntimeException.class,
                    () ->
                    {
                        session
                                .sql("ALTER TABLE ndb.buck_alter_neg.schem.tab SET TBLPROPERTIES ('sorted_by' = 'non_existent')")
                                .collect();
                    });
            assertTrue(e2.getMessage().contains("does not exist in the table"),
                    "Actual error message was: " + e2.getMessage());
            VastRuntimeException e3 = expectThrows(VastRuntimeException.class,
                    () ->
                    {
                        session
                                .sql("ALTER TABLE ndb.buck_alter_neg.schem.tab SET TBLPROPERTIES ('sorted_by' = ' ')")
                                .collect();
                    });
            assertTrue(e3.getMessage().contains("does not exist in the table"),
                    "Actual error message was: " + e3.getMessage());
            VastRuntimeException e4 = expectThrows(VastRuntimeException.class,
                    () ->
                    {
                        session
                                .sql("ALTER TABLE ndb.buck_alter_neg.schem.tab SET TBLPROPERTIES ('sorted_by' = 'I')")
                                .collect();
                    });
            assertTrue(e4.getMessage().contains("does not exist in the table"),
                    "Actual error message was: " + e4.getMessage());
        }
    }

    @Test
    public void testAlterTableElysiumPositiveScenarios()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck_alter_pos";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck_alter_pos.schem").collect();
            session
                    .sql("create table ndb.buck_alter_pos.schem.tab (i integer, j integer)")
                    .collect();
            try {
                session
                        .sql("ALTER TABLE ndb.buck_alter_pos.schem.tab SET TBLPROPERTIES ('sorted_by' = 'i')")
                        .collect();
            }
            catch (VastRuntimeException e1) {
                assertTrue(e1.getMessage().contains("404"),
                        "Actual error message was: " + e1.getMessage());
            }
            session
                    .sql("create table ndb.buck_alter_pos.schem.tab2 (i integer, j integer)")
                    .collect();
            try {
                session
                        .sql("ALTER TABLE ndb.buck_alter_pos.schem.tab2 SET TBLPROPERTIES ('sorted_by' = 'i, j')")
                        .collect();
            }
            catch (VastRuntimeException e2) {
                assertTrue(e2.getMessage().contains("404"),
                        "Actual error message was: " + e2.getMessage());
            }
        }
    }

    //Unitest base for https://vastdata.atlassian.net/browse/ORION-325246
    @Test(enabled = false)
    public void testDropEndpointRetryPerAvailableEndPoint()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        AtomicInteger deleteAttemptCount = new AtomicInteger(0);
        AtomicInteger commitAttemptCount = new AtomicInteger(0);

        Set<String> rollbackHosts = Collections.newSetFromMap(
                new ConcurrentHashMap<>());
        Set<String> commitHosts = Collections.newSetFromMap(
                new ConcurrentHashMap<>());

        // drop Cnode during rollback attempt
        Consumer<HttpExchange> hardDisconnectHook = httpExchange ->
        {
            deleteAttemptCount.incrementAndGet();
            rollbackHosts.add(
                    httpExchange.getRequestHeaders().getFirst("Host"));
            httpExchange.close();
        };
        Consumer<HttpExchange> commitDisconnectHook = httpExchange ->
        {
            commitAttemptCount.incrementAndGet();
            commitHosts.add(httpExchange.getRequestHeaders().getFirst("Host"));
            httpExchange.close();
        };
        HashMap<String, Object> extraConf = new HashMap<>();
        extraConf.put("spark.ndb.retry_max_count", 3);
        extraConf.put("spark.ndb.retry_sleep_duration", 10);

        String mockUri = "http://localhost:" + testPort;
        String mockUri2 = "http://127.0.0.1:" + testPort;
        String endpointsString = mockUri + "," + mockUri2;

        extraConf.put("spark.ndb.data_endpoints", endpointsString);
        handler.setHook("/", DELETE, hardDisconnectHook);
        handler.setHook("/", PUT, commitDisconnectHook);
        try (SparkSession session = SparkTestUtils.getSession(testPort,
                extraConf)) {
            int expectedMinimumAttempts = 3;

            session.sql("select ndb.create_tx()").show();
            assertThatThrownBy(() -> session
                    .sql("select ndb.rollback_tx()")
                    .show()).hasRootCauseInstanceOf(EOFException.class);

            assertTrue(deleteAttemptCount.get() > expectedMinimumAttempts,
                    "Expected more connection attempts for ROLLBACK. Actual attempts: " + deleteAttemptCount.get());

            assertTrue(rollbackHosts.size() > 1,
                    "Expected client to rotate endpoints. Hosts contacted: " + rollbackHosts);

            session.sql("select ndb.create_tx()").show();
            assertThatThrownBy(() -> session
                    .sql("select ndb.commit_tx()")
                    .show()).hasRootCauseInstanceOf(EOFException.class);

            assertTrue(commitAttemptCount.get() > expectedMinimumAttempts,
                    "Expected more connection attempts for COMMIT. Actual attempts: " + commitAttemptCount.get());

            assertTrue(commitHosts.size() > 1,
                    "Expected client to rotate endpoints. Hosts contacted: " + commitHosts);
        }
    }

    @Test
    public void testCTE()
            throws IOException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);
        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("create database ndb.buck.schem").collect();
            session
                    .sql("create table ndb.buck.schem.t1 (i integer, j integer)")
                    .collect();
            session
                    .sql("create table ndb.buck.schem.t2 (i2 integer, j2 integer)")
                    .collect();

            session
                    .sql("with t1 as (select i, count(i) from ndb.buck.schem.t1 group by 1), t1cte as (select j, count(j) from ndb.buck.schem.t1 group by 1)" + "select * from ndb.buck.schem.t2, t1, t1cte where t2.i2 = t1.i and t2.i2 > 7 and t2.j2 = t1cte.j and t1cte.j > 5")
                    .explain("cost");
        }
    }

    @Test
    public void testPITScanWithRowFilterIsBlocked()
            throws IOException, VastUserException
    {
        MockUtils mockUtils = new MockUtils();
        String testBucket = "buck";
        mockUtils.createBucket(this.testPort, testBucket);

        try (SparkSession session = SparkTestUtils.getSession(testPort)) {
            session.sql("CREATE DATABASE ndb.buck.schem").show();
            session
                    .sql("CREATE TABLE ndb.buck.schem.tab1 (col_int_x integer, col_int_y integer) PARTITIONED BY (col_int_x)")
                    .show();

            // 1. Grab the Catalog
            CatalogPlugin ndb = session
                    .sessionState()
                    .catalogManager()
                    .catalog("ndb");
            VastCatalog vastCatalog = (VastCatalog) ndb;

            // 2. Create a mock Row-Level Security policy (Row filter on col_int_x)
            java.util.List<String> filters = ImmutableList.of("col_int_x > 0");
            RowColumnSecurityResponse rclsResponse = new RowColumnSecurityResponse(
                    filters, ImmutableSet.of(), ImmutableSet.of(), ImmutableMap
                            .of());

            VastConfig vastConfig = NDB.getConfig();
            VastClient vastClient = NDB.getVastClient(vastConfig);
            VastCatalogTestUtils vastCatalogTestUtils = new VastCatalogTestUtils(
                    vastConfig, vastClient, VastSparkTransactionsManager
                            .getInstance(vastClient,
                                    new VastTransactionFactory()));

            // 3. Apply the security policy to the PIT table
            // (Note: Your API fetches it using the table name + PIT_NAME_SUFFIX)
            String pitTableName = "tab1" + PartitionConstants.PIT_NAME_SUFFIX;
            vastCatalogTestUtils
                    .setRowColumnsSecurityResponse("buck/schem", pitTableName,
                            rclsResponse);

            vastCatalog.setVastCatalogUtils(vastCatalogTestUtils);
            InitializedVastCatalog.setVastCatalog(vastCatalog);

            // 4. Query the PIT table directly and assert that it throws your new Access Denied exception
            assertThatThrownBy(() -> session
                    .sql("select * from ndb.buck.schem.tab1.partitions")
                    .show())
                            .isInstanceOf(VastRuntimeException.class)
                            .hasMessageContaining("Access Denied");
        }
    }
}
