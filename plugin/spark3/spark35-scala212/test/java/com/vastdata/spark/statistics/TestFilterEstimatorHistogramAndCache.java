/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import com.vastdata.spark.CommonSparkTestUtils;
import com.vastdata.spark.VastTable;
import com.vastdata.spark.predicate.VastPredicate;
import org.apache.spark.sql.catalyst.expressions.AttributeMap$;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.connector.read.colstats.Histogram;
import org.apache.spark.sql.connector.read.colstats.HistogramBin;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.mockito.Mockito;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import scala.Option;
import scala.math.BigInt;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Listeners(CommonSparkTestUtils.TestListener.class)
public class TestFilterEstimatorHistogramAndCache
{
    @Test
    public void testHistogramAffectsEqualitySelectivity()
    {
        NamedReference ref = FieldReference.column("x");
        StructField field = new StructField("x", DataTypes.IntegerType, true,
                Metadata.empty());
        Predicate equalsPredicate = new Predicate("=", new Expression[] {ref,
                LiteralValue.apply(15, DataTypes.IntegerType)});
        VastPredicate vastPredicate = new VastPredicate(equalsPredicate, ref,
                field);

        HistogramBin lowNdvBin = Mockito.mock(HistogramBin.class);
        Mockito.when(lowNdvBin.lo()).thenReturn(0.0);
        Mockito.when(lowNdvBin.hi()).thenReturn(9.0);
        Mockito.when(lowNdvBin.ndv()).thenReturn(10L);
        HistogramBin highSelectivityBin = Mockito.mock(HistogramBin.class);
        Mockito.when(highSelectivityBin.lo()).thenReturn(10.0);
        Mockito.when(highSelectivityBin.hi()).thenReturn(19.0);
        Mockito.when(highSelectivityBin.ndv()).thenReturn(2L);
        Histogram histogram = Mockito.mock(Histogram.class);
        Mockito.when(histogram.bins()).thenReturn(
                new HistogramBin[] {lowNdvBin, highSelectivityBin});

        ColumnStatistics withHistogram = new ColumnLevelStatistics(
                OptionalLong.of(100L), Optional.of(0), Optional.of(99),
                OptionalLong.of(0), OptionalLong.of(4), OptionalLong.of(4),
                Optional.of(histogram));
        ColumnStatistics withoutHistogram = new ColumnLevelStatistics(
                OptionalLong.of(100L), Optional.of(0), Optional.of(99),
                OptionalLong.of(0), OptionalLong.of(4), OptionalLong.of(4),
                Optional.empty());

        TableLevelStatistics statsWithHistogram = new TableLevelStatistics(
                OptionalLong.of(1000), OptionalLong.of(100),
                Collections.singletonMap(ref, withHistogram));
        TableLevelStatistics statsWithoutHistogram = new TableLevelStatistics(
                OptionalLong.of(1000), OptionalLong.of(100),
                Collections.singletonMap(ref, withoutHistogram));

        double withHistogramSelectivity = FilterEstimator.estimateSelectivity(Collections.singletonList(Collections.singletonList(vastPredicate)), statsWithHistogram, "test-token");
        double withoutHistogramSelectivity = FilterEstimator.estimateSelectivity(Collections.singletonList(Collections.singletonList(vastPredicate)), statsWithoutHistogram, "test-token");

        assertEquals(withHistogramSelectivity, 0.25, 1e-9,
                "Expected histogram-based selectivity to use per-bin NDV");
        assertEquals(withoutHistogramSelectivity, 0.01, 1e-9,
                "Expected fallback selectivity to use distinct count");
        assertTrue(withHistogramSelectivity > withoutHistogramSelectivity,
                "Histogram path should differ from fallback in this setup");
    }

    @Test
    public void testHistogramAffectsRangeSelectivity()
    {
        NamedReference ref = FieldReference.column("x");
        StructField field = new StructField("x", DataTypes.IntegerType, true,
                Metadata.empty());
        Predicate rangePredicate = new Predicate("<", new Expression[] {ref,
                LiteralValue.apply(15, DataTypes.IntegerType)});
        VastPredicate vastPredicate = new VastPredicate(rangePredicate, ref,
                field);

        HistogramBin firstBin = Mockito.mock(HistogramBin.class);
        Mockito.when(firstBin.lo()).thenReturn(0.0);
        Mockito.when(firstBin.hi()).thenReturn(9.0);
        Mockito.when(firstBin.ndv()).thenReturn(10L);
        HistogramBin secondBin = Mockito.mock(HistogramBin.class);
        Mockito.when(secondBin.lo()).thenReturn(10.0);
        Mockito.when(secondBin.hi()).thenReturn(19.0);
        Mockito.when(secondBin.ndv()).thenReturn(10L);
        Histogram histogram = Mockito.mock(Histogram.class);
        Mockito.when(histogram.bins()).thenReturn(
                new HistogramBin[] {firstBin, secondBin});

        ColumnStatistics withHistogram = new ColumnLevelStatistics(
                OptionalLong.of(100L), Optional.of(0), Optional.of(99),
                OptionalLong.of(0), OptionalLong.of(4), OptionalLong.of(4),
                Optional.of(histogram));
        ColumnStatistics withoutHistogram = new ColumnLevelStatistics(
                OptionalLong.of(100L), Optional.of(0), Optional.of(99),
                OptionalLong.of(0), OptionalLong.of(4), OptionalLong.of(4),
                Optional.empty());

        TableLevelStatistics statsWithHistogram = new TableLevelStatistics(
                OptionalLong.of(1000), OptionalLong.of(100),
                Collections.singletonMap(ref, withHistogram));
        TableLevelStatistics statsWithoutHistogram = new TableLevelStatistics(
                OptionalLong.of(1000), OptionalLong.of(100),
                Collections.singletonMap(ref, withoutHistogram));

        double withHistogramSelectivity = FilterEstimator.estimateSelectivity(Collections.singletonList(Collections.singletonList(vastPredicate)), statsWithHistogram, "test-token");
        double withoutHistogramSelectivity = FilterEstimator.estimateSelectivity(Collections.singletonList(Collections.singletonList(vastPredicate)), statsWithoutHistogram, "test-token");

        assertEquals(withoutHistogramSelectivity, 15.0 / 99.0, 1e-9,
                "Expected fallback range selectivity from min/max interpolation");
        assertTrue(withHistogramSelectivity > withoutHistogramSelectivity,
                "Histogram path should change range selectivity versus fallback");
    }

    @Test
    public void testStatisticsCacheLifecycle()
    {
        SparkVastStatisticsManagerTestUtil.initInMemoryStatsInstance();
        VastTable table = Mockito.mock(VastTable.class);

        SparkVastStatisticsManager manager = SparkVastStatisticsManager.getInstance();
        manager.deleteTableStatistics(table);
        assertFalse(manager.getTableStatistics(table).isPresent(),
                "Cache should start empty");

        org.apache.spark.sql.catalyst.plans.logical.Statistics v1 = new org.apache.spark.sql.catalyst.plans.logical.Statistics(
                BigInt.apply(1000L), Option.apply(BigInt.apply(100L)),
                AttributeMap$.MODULE$.empty(), false);
        org.apache.spark.sql.catalyst.plans.logical.Statistics v2 = new org.apache.spark.sql.catalyst.plans.logical.Statistics(
                BigInt.apply(2000L), Option.apply(BigInt.apply(200L)),
                AttributeMap$.MODULE$.empty(), false);

        manager.setTableStatistics(table, v1);
        assertTrue(manager.getTableStatistics(table).isPresent());
        assertEquals(manager.getTableStatistics(table).get().sizeInBytes().longValue(),
                1000L);

        manager.setTableStatistics(table, v2);
        assertTrue(manager.getTableStatistics(table).isPresent());
        assertEquals(manager.getTableStatistics(table).get().sizeInBytes().longValue(),
                2000L, "Expected overwritten value from cache");

        manager.deleteTableStatistics(table);
        assertFalse(manager.getTableStatistics(table).isPresent(),
                "Cache delete should remove statistics");
    }
}
