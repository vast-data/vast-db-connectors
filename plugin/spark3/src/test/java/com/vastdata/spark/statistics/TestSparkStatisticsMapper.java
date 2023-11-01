/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastClientForTests;
import com.vastdata.client.VastConfig;
import com.vastdata.spark.VastScan;
import com.vastdata.spark.VastScanBuilder;
import com.vastdata.spark.VastTable;
import com.vastdata.spark.VastTableMetaData;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeMap;
import org.apache.spark.sql.catalyst.expressions.AttributeMap$;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.ByteType$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ShortType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.mockito.Mock;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Seq;
import scala.collection.mutable.Builder;
import scala.math.BigInt;

import java.io.IOException;
import java.sql.Date;
import java.util.Optional;
import java.util.function.Supplier;

import static com.vastdata.spark.SparkTestUtils.getTestConfig;
import static com.vastdata.spark.statistics.StatsUtils.fieldToAttribute;
import static java.lang.String.format;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestSparkStatisticsMapper
{
    @Mock VastTable tableMock;
    @Mock VastScanBuilder vastScanBuilderMock;
    @Mock VastScan vastScanMock;

    @Mock
    private VastClient mockClient;

    private AutoCloseable autoCloseable;

    @BeforeTest
    public void setup()
    {
        autoCloseable = openMocks(this);
    }

    @AfterTest
    public void tearDown()
            throws Exception
    {
        autoCloseable.close();
    }

    private VastConfig getMockServerReadyVastConfig()
    {
        return getTestConfig()
                .setRetryMaxCount(1)
                .setRetrySleepDuration(1);
    }

    @Test(enabled = false)
    public void testFilterEstimation()
    {
        StructField intField = new StructField("intf", IntegerType$.MODULE$, true, Metadata.empty());
        StructField[] fields = new StructField[] {intField};

        StructType schema = new StructType(fields);

        Tuple2<Attribute, ColumnStat> tuInt = buildTup(schema, intField, 1, 100);
        Builder<Tuple2<Attribute, ColumnStat>, List<Tuple2<Attribute, ColumnStat>>> tuple2ListBuilder = List$.MODULE$.newBuilder();
        tuple2ListBuilder.$plus$eq(tuInt);
        Seq<Tuple2<Attribute, ColumnStat>> seq = tuple2ListBuilder.result();
        AttributeMap<ColumnStat> attrMap = AttributeMap$.MODULE$.apply(seq);
        BigInt testSize = BigInt.apply(400L);
        Option<BigInt> testRows = Option.apply(BigInt.apply(100L));
        BigInt expectedSize = BigInt.apply(200L);
        Option<BigInt> expectedRows = Option.apply(BigInt.apply(50L));
        Statistics testStats = new Statistics(testSize, testRows, attrMap, false);
        Statistics expected = new Statistics(expectedSize, expectedRows, attrMap, false);

        VastTableMetaData md = new VastTableMetaData("b/s", "tname", "id", schema, false);
        when(tableMock.getTableMD()).thenReturn(md);
        when(tableMock.newScanBuilder(any(CaseInsensitiveStringMap.class))).thenReturn(vastScanBuilderMock);
        when(vastScanBuilderMock.build()).thenReturn(vastScanMock);
        when(vastScanMock.estimateStatistics()).thenReturn(TableLevelStatistics.of(400L, 100L));
        Builder<AttributeReference, List<AttributeReference>> objectSeqBuilder = List$.MODULE$.newBuilder();
        ExprId exprId = NamedExpression.newExprId();
        AttributeReference attr = new AttributeReference(intField.name(), intField.dataType(), intField.nullable(), intField.metadata(), exprId, List$.MODULE$.<String>newBuilder().result());
        objectSeqBuilder.$plus$eq(attr);
        Seq<AttributeReference> output = objectSeqBuilder.result();
        CaseInsensitiveStringMap empty = CaseInsensitiveStringMap.empty();
        DataSourceV2Relation v2rel = new DataSourceV2Relation(tableMock, output, Option.empty(), Option.empty(), empty);
        DataSourceV2ScanRelation v2ScanRelation = new DataSourceV2ScanRelation(v2rel, tableMock.newScanBuilder(empty).build(), v2rel.output(), Option.empty(), Option.empty());
        Expression condition = new GreaterThan(attr, Literal.create(50, DataTypes.IntegerType));
        Filter filter = new Filter(condition, v2ScanRelation);
        new SparkPlanStatisticsSetter().accept(v2ScanRelation, testStats);
        Option<Statistics> filterStats = FilterEstimation.apply(filter).estimate();
        assertTrue(filterStats.nonEmpty());
        System.out.printf("BEFORE: %s = %s%n", filter, filter.statsCache());
        new SparkPlanStatisticsSetter().accept(filter, filterStats.get());
        System.out.printf("AFTER: %s = %s%n", filter, filter.statsCache());
        assertEquals(filterStats.get(), expected);
    }

    @Test
    public void testSerDe()
            throws JsonProcessingException
    {
        StructField intField = new StructField("intf", IntegerType$.MODULE$, true, Metadata.empty());
        StructField decField = new StructField("decf", new DecimalType(10, 5), true, Metadata.empty());
        StructField longField = new StructField("longf", LongType$.MODULE$, true, Metadata.empty());
        StructField shortField = new StructField("shortf", ShortType$.MODULE$, true, Metadata.empty());
        StructField doubleField = new StructField("doublef", DoubleType$.MODULE$, true, Metadata.empty());
        StructField floatField = new StructField("floatf", FloatType$.MODULE$, true, Metadata.empty());
        StructField byteField = new StructField("bytef", ByteType$.MODULE$, true, Metadata.empty());
        StructField boolField = new StructField("boolf", BooleanType$.MODULE$, true, Metadata.empty());
        StructField dateField = new StructField("datef", DateType$.MODULE$, true, Metadata.empty());
        StructField strField = new StructField("strf", StringType$.MODULE$, true, Metadata.empty());
        //TODO add coverage

        StructField[] fields = new StructField[] {intField, decField, longField, shortField, doubleField, floatField,
                byteField, boolField, dateField, strField};

        StructType schema = new StructType(fields);
        VastTableMetaData md = new VastTableMetaData("b/s", "tname", "id", schema, false);
        when(tableMock.getTableMD()).thenReturn(md);
        when(tableMock.schema()).thenReturn(schema);
        ObjectMapper instance = SparkStatisticsMapper.instance(tableMock);
        BigInt size = BigInt.apply("26364397224300470284329554475476558257587049");
        Option<BigInt> rows = Option.apply(BigInt.apply(123));

        Tuple2<Attribute, ColumnStat> tuInt = buildTup(schema, intField, 1, 100);
        Tuple2<Attribute, ColumnStat> tuDec = buildTup(schema, decField, new Decimal().set(1111111111L, 10, 5), new Decimal().set(2222222222L, 10, 5));
        Tuple2<Attribute, ColumnStat> tuLong = buildTup(schema, longField, 1111111111, 1999999999);
        Tuple2<Attribute, ColumnStat> tuShort = buildTup(schema, shortField, 11, 99);
        Tuple2<Attribute, ColumnStat> tuDouble = buildTup(schema, doubleField, 11.1111111111, 999.999151546546);
        Tuple2<Attribute, ColumnStat> tuFloat = buildTup(schema, floatField, 11.111, 999.999);
        Tuple2<Attribute, ColumnStat> tuByte = buildTup(schema, byteField, '1', '2');
        Tuple2<Attribute, ColumnStat> tuBool = buildTup(schema, boolField, false, true);
        Tuple2<Attribute, ColumnStat> tuDate = buildTup(schema, dateField, (int) Date.valueOf("2017-08-18").toLocalDate().toEpochDay(), (int) Date.valueOf("2019-08-18").toLocalDate().toEpochDay());
        Tuple2<Attribute, ColumnStat> tuStr = buildTup(schema, strField, "abc", "abcd");

        Builder<Tuple2<Attribute, ColumnStat>, List<Tuple2<Attribute, ColumnStat>>> objectSeqBuilder = List$.MODULE$.newBuilder();
        objectSeqBuilder.$plus$eq(tuInt);
        objectSeqBuilder.$plus$eq(tuDec);
        objectSeqBuilder.$plus$eq(tuLong);
        objectSeqBuilder.$plus$eq(tuShort);
        objectSeqBuilder.$plus$eq(tuDouble);
        objectSeqBuilder.$plus$eq(tuFloat);
        objectSeqBuilder.$plus$eq(tuByte);
        objectSeqBuilder.$plus$eq(tuBool);
        objectSeqBuilder.$plus$eq(tuDate);
        objectSeqBuilder.$plus$eq(tuStr);
        Seq<Tuple2<Attribute, ColumnStat>> seq = objectSeqBuilder.result();
        AttributeMap<ColumnStat> attrMap = AttributeMap$.MODULE$.apply(seq);

        Statistics expected = new Statistics(size, rows, attrMap, false);
        String serialized = instance.writeValueAsString(expected);

        Statistics result = instance.readValue(serialized, Statistics.class);
        assertEquals(result.sizeInBytes(), result.sizeInBytes());
        assertEquals(result.rowCount(), result.rowCount());
        AttributeMap<ColumnStat> actualMap = result.attributeStats();
        AttributeMap<ColumnStat> expectedMap = expected.attributeStats();
        assertEquals(actualMap.size(), expectedMap.size(), format("actualMap: %s, expectedMap: %s", actualMap, expectedMap));
        assertEquals(actualMap.toString(), expectedMap.toString());
        assertEquals(actualMap.get(tuDate._1).get().max().get(), expectedMap.get(tuDate._1).get().max().get());
    }

    private AttributeMap<ColumnStat> getColumnStats(Object min, Object max)
    {
        StructField intField = new StructField("x", IntegerType$.MODULE$, true, Metadata.empty());
        StructField[] fields = new StructField[] {intField};
        StructType schema = new StructType(fields);
        Tuple2<Attribute, ColumnStat> tuInt = buildTup(schema, intField, min, max);
        Builder<Tuple2<Attribute, ColumnStat>, List<Tuple2<Attribute, ColumnStat>>> objectSeqBuilder = List$.MODULE$.newBuilder();
        objectSeqBuilder.$plus$eq(tuInt);
        Seq<Tuple2<Attribute, ColumnStat>> seq = objectSeqBuilder.result();
        return AttributeMap$.MODULE$.apply(seq);
    }

    private Tuple2<Attribute, ColumnStat> buildTup(StructType schema, StructField field, Object minValue, Object maxValue)
    {
        Option<BigInt> distinctCount = Option.apply(BigInt.apply(100));
        Option<BigInt> nullCount = Option.apply(BigInt.apply(0));
        Option<Object> avgLen = Option.apply(4L);
        Option<Object> maxLen = Option.apply(4L);
        ColumnStat colStats = new ColumnStat(distinctCount, Option.apply(minValue), Option.apply(maxValue), nullCount, avgLen, maxLen, Option.empty(), 0);
        Attribute attribute = fieldToAttribute(field, schema);
        return Tuple2.apply(attribute, colStats);
    }

    @Test
    public void testSparkStatisticsDeserialize()
            throws IOException
    {
        Supplier<VastClient> supplier = () -> VastClientForTests.getVastClient(new JettyHttpClient(), 456);
        StructField charNField = new StructField("x", IntegerType$.MODULE$, true, Metadata.empty());
        StructType schema = new StructType(new StructField[] {charNField});
        VastTable table = new VastTable("buck/schem", "tab", "id", schema, supplier, false);
        ObjectMapper mapper = SparkStatisticsMapper.instance(table);
        AttributeMap<ColumnStat> tableStats = getColumnStats(1, 100);
        Statistics tableStatistics = new Statistics(BigInt.apply(70000), Option.apply(BigInt.apply(280000)), tableStats, false);
        String tsBuffer = mapper.writeValueAsString(tableStatistics);
        when(mockClient.s3GetObj(anyString(), anyString()))
                .thenReturn(Optional.of(tsBuffer));
        SparkPersistentStatistics sparkPersistentStatistics = new SparkPersistentStatistics(mockClient, getMockServerReadyVastConfig());
        sparkPersistentStatistics.setTableStatistics(table, tableStatistics);
        sparkPersistentStatistics.deleteTableStatistics(table);
        Optional<Statistics> newTableStatistics = sparkPersistentStatistics.getTableStatistics(table);
        assertEquals(Optional.of(tableStatistics), newTableStatistics);
    }
}
