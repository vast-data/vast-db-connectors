/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.spark.statistics;

import com.vastdata.spark.predicate.VastPredicate;
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils;
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.ValueInterval;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.connector.read.colstats.Histogram;
import org.apache.spark.sql.connector.read.colstats.HistogramBin;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.jdk.javaapi.OptionConverters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.UnaryOperator;

import static com.vastdata.OptionalPrimitiveHelpers.map;
import static java.lang.Math.ceil;

/* A simple statistics-based filter selectivity estimator, based on Spark's FilterEstimation.scala
*/
public final class FilterEstimator
{
    private static final Logger LOG = LoggerFactory.getLogger(
            FilterEstimator.class);

    private FilterEstimator()
    {
    }

    private static int findFirstBinForValue(double value, HistogramBin[] bins)
    {
        int i = 0;

        while (i < bins.length && value > bins[i].hi()) {
            i++;
        }

        return i;
    }

    private static int findLastBinForValue(double value, HistogramBin[] bins)
    {
        int i = bins.length - 1;

        while (i >= 0 && value < bins[i].lo()) {
            i--;
        }

        return i;
    }

    private static double binHoldingRangePossibility(double upperBound, double lowerBound, HistogramBin bin)
    {
        if (bin.hi() == bin.lo()) {
            return 1.0;
        }
        else if (upperBound == lowerBound) {
            return bin.ndv() > 0 ? 1.0 / (double) bin.ndv() : 0.0;
        }
        else {
            return Math.min((upperBound - lowerBound) / (bin.hi() - bin.lo()), 1.0);
        }
    }

    private static double numBinsHoldingRange(
            double upperBound, boolean upperBoundInclusive,
            double lowerBound, boolean lowerBoundInclusive,
            HistogramBin[] bins)
    {
        if (bins.length == 0) {
            return 0.0;
        }

        lowerBound = Math.max(lowerBound, bins[0].lo());
        upperBound = Math.min(upperBound, bins[bins.length - 1].hi());

        if (lowerBound > upperBound) {
            return 0.0;
        }

        int upperBinIndex = upperBoundInclusive
                ? findLastBinForValue(upperBound, bins)
                : findFirstBinForValue(upperBound, bins);
        int lowerBinIndex = lowerBoundInclusive
                ? findFirstBinForValue(lowerBound, bins)
                : findLastBinForValue(lowerBound, bins);

        if (lowerBinIndex < 0 || upperBinIndex >= bins.length || lowerBinIndex > upperBinIndex) {
            return 0.0;
        }

        if (lowerBinIndex == upperBinIndex) {
            return binHoldingRangePossibility(upperBound, lowerBound, bins[lowerBinIndex]);
        }
        else {
            HistogramBin lowerBin = bins[lowerBinIndex];
            double lowerPart = binHoldingRangePossibility(lowerBin.hi(), lowerBound, lowerBin);
            HistogramBin higherBin = bins[upperBinIndex];
            double higherPart = binHoldingRangePossibility(upperBound, higherBin.lo(), higherBin);
            return lowerPart + higherPart + (upperBinIndex - lowerBinIndex - 1);
        }
    }

    private static double estimateEqualitySelectivityWithHistogram(
            Histogram histogram, double numericLiteral, double min, double max)
    {
        HistogramBin[] bins = histogram.bins();
        double numBinsTotal = numBinsHoldingRange(max, true, min, true, bins);

        if (numBinsTotal <= 0) {
            return 0.0;
        }

        double numBinsDatum = numBinsHoldingRange(numericLiteral, true, numericLiteral, true, bins);
        return numBinsDatum / numBinsTotal;
    }

    private static double estimateRangeSelectivityWithHistogram(
            Histogram histogram, double numericLiteral, String op, double min, double max)
    {
        HistogramBin[] bins = histogram.bins();
        double numBinsTotal = numBinsHoldingRange(max, true, min, true, bins);

        if (numBinsTotal <= 0) {
            return 1.0;
        }

        double numBinsRange;

        switch (op) {
            case "<":
                numBinsRange = numBinsHoldingRange(numericLiteral, false, min, true, bins);
                break;
            case "<=":
                numBinsRange = numBinsHoldingRange(numericLiteral, true, min, true, bins);
                break;
            case ">":
                numBinsRange = numBinsHoldingRange(max, true, numericLiteral, false, bins);
                break;
            case ">=":
                numBinsRange = numBinsHoldingRange(max, true, numericLiteral, true, bins);
                break;
            default:
                throw new IllegalArgumentException("Invalid range predicate: " + op);
        }

        return Math.min(1.0, numBinsRange / numBinsTotal);
    }

    private static boolean isStringOrBinaryOrWithinRange(
            ColumnStatistics colStats, StructField field, Predicate predicate)
    {
        final ValueInterval statsInterval = ValueInterval.apply(
                OptionConverters.toScala(colStats.min()),
                OptionConverters.toScala(colStats.max()), field.dataType());
        final org.apache.spark.sql.connector.expressions.Literal jLiteral = (org.apache.spark.sql.connector.expressions.Literal) predicate.children()[1];
        final org.apache.spark.sql.catalyst.expressions.Literal sLiteral = new org.apache.spark.sql.catalyst.expressions.Literal(
                jLiteral.value(), jLiteral.dataType());
        return statsInterval.contains(sLiteral);
    }

    private static double estimateOpSelectivity(Predicate predicate,
            StructField field, NamedReference reference, Statistics statistics,
            Map<NamedReference, ColumnStatistics> statsMap,
            boolean updateStatistics, String traceToken)
    {
        final ColumnStatistics colStats = statsMap.get(reference);
        if (colStats == null) {
            return 1.0;
        }
        final DataType dt = field.dataType();
        switch (predicate.name()) {
            case "<>":
            case "!=":
            case "=": {
                double percent;
                if (isStringOrBinaryOrWithinRange(colStats, field, predicate)) {
                    if (updateStatistics && "=".equals(predicate.name())) {
                        ColumnStatistics newStat;
                        if ((dt instanceof StringType) || (dt instanceof BinaryType)) {
                            newStat = new ColumnLevelStatistics(
                                    OptionalLong.of(1), colStats.min(),
                                    colStats.max(), OptionalLong.of(0),
                                    colStats.avgLen(), colStats.maxLen(), colStats.histogram());
                        }
                        else {
                            final org.apache.spark.sql.connector.expressions.Literal jLiteral = (org.apache.spark.sql.connector.expressions.Literal) predicate.children()[1];
                            newStat = new ColumnLevelStatistics(
                                    OptionalLong.of(1),
                                    Optional.of(jLiteral.value()),
                                    Optional.of(jLiteral.value()),
                                    OptionalLong.of(0), colStats.avgLen(),
                                    colStats.maxLen(), colStats.histogram());
                        }
                        statsMap.replace(reference, newStat);
                    }
                    if (colStats.histogram().isPresent()
                            && !((dt instanceof StringType) || (dt instanceof BinaryType))
                            && colStats.min().isPresent() && colStats.max().isPresent()) {
                        final org.apache.spark.sql.connector.expressions.Literal jLit =
                                (org.apache.spark.sql.connector.expressions.Literal) predicate.children()[1];
                        final double numericLiteral = EstimationUtils.toDouble(
                                jLit.value(), jLit.dataType());
                        final double colMin = EstimationUtils.toDouble(
                                colStats.min().get(), jLit.dataType());
                        final double colMax = EstimationUtils.toDouble(
                                colStats.max().get(), jLit.dataType());
                        percent = estimateEqualitySelectivityWithHistogram(
                                colStats.histogram().get(), numericLiteral, colMin, colMax);
                    }
                    else if (colStats.distinctCount().isPresent()) {
                        percent = 1.0 / colStats.distinctCount().getAsLong();
                    }
                    else {
                        return 1.0;
                    }
                }
                else {
                    percent = 0.0;
                }
                return "=".equals(predicate.name()) ? percent : (1.0 - percent);
            }
            case ">":
            case "<":
            case "<=":
            case ">=": {
                // Non-numeric types???
                if (!((dt instanceof NumericType) || (dt instanceof DateType) || (dt instanceof TimestampType) || (dt instanceof TimestampNTZType) || (dt instanceof BooleanType)) || !colStats
                        .min()
                        .isPresent() || !colStats.max().isPresent() || !colStats
                        .distinctCount()
                        .isPresent()) {
                    return 1.0;
                }
                final org.apache.spark.sql.connector.expressions.Literal literal = (org.apache.spark.sql.connector.expressions.Literal) predicate.children()[1];
                final double max = EstimationUtils.toDouble(
                        colStats.max().get(), literal.dataType());
                final double min = EstimationUtils.toDouble(
                        colStats.min().get(), literal.dataType());
                final long ndv = colStats.distinctCount().getAsLong();
                final double numericLiteral = EstimationUtils.toDouble(
                        literal.value(), literal.dataType());
                boolean noOverlap = false;
                boolean completeOverlap = false;
                switch (predicate.name()) {
                    case "<":
                        noOverlap = numericLiteral <= min;
                        completeOverlap = numericLiteral > max;
                        break;
                    case "<=":
                        noOverlap = numericLiteral < min;
                        completeOverlap = numericLiteral >= max;
                        break;
                    case ">":
                        noOverlap = numericLiteral >= max;
                        completeOverlap = numericLiteral < min;
                        break;
                    case ">=":
                        noOverlap = numericLiteral > max;
                        completeOverlap = numericLiteral <= min;
                        break;
                }
                double percent = 1.0;
                if (noOverlap) {
                    percent = 0.0;
                }
                else if (completeOverlap) {
                    percent = 1.0;
                }
                else if (colStats.histogram().isPresent()) {
                    percent = estimateRangeSelectivityWithHistogram(
                            colStats.histogram().get(), numericLiteral, predicate.name(), min, max);
                }
                else {
                    switch (predicate.name()) {
                        case "<":
                            if (numericLiteral == max) {
                                percent = 1.0 - 1.0 / ndv;
                            }
                            else {
                                percent = (numericLiteral - min) / (max - min);
                            }
                            break;
                        case "<=":
                            if (numericLiteral == min) {
                                percent = 1.0 / ndv;
                            }
                            else {
                                percent = (numericLiteral - min) / (max - min);
                            }
                            break;
                        case ">":
                            if (numericLiteral == min) {
                                percent = 1.0 - 1.0 / ndv;
                            }
                            else {
                                percent = (max - numericLiteral) / (max - min);
                            }
                            break;
                        case ">=":
                            if (numericLiteral == max) {
                                percent = 1.0 / ndv;
                            }
                            else {
                                percent = (max - numericLiteral) / (max - min);
                            }
                            break;
                    }
                }
                LOG.info(
                        "Estimating predicate({}): {}: name: {} min: {} max: {} selectivity: {}",
                        traceToken, predicate, predicate.name(), min, max,
                        percent);
                if (updateStatistics) {
                    Optional<Object> newMin = colStats.min();
                    Optional<Object> newMax = colStats.max();
                    switch (predicate.name()) {
                        case "<":
                        case "<=":
                            newMax = Optional.of(literal.value());
                            break;
                        case ">":
                        case ">=":
                            newMin = Optional.of(literal.value());
                    }
                    ColumnStatistics newStat = new ColumnLevelStatistics(
                            OptionalLong.of((long) ceil(ndv * percent)), newMin,
                            newMax, OptionalLong.of(0), colStats.avgLen(),
                            colStats.maxLen(), colStats.histogram());
                    statsMap.replace(reference, newStat);
                }
                return percent;
            }
            case "IS_NULL":
            case "IS_NOT_NULL": {
                if (!colStats.nullCount().isPresent()) {
                    return 1.0;
                }
                final long rowCount = statistics.numRows().getAsLong();
                final long nullCount = colStats.nullCount().getAsLong();
                final double nullPercent = rowCount == 0 ?
                        0.0 :
                        (nullCount >= rowCount ?
                                1.0 :
                                (double) nullCount / (double) rowCount);
                if (updateStatistics) {
                    ColumnStatistics newStat;
                    if ("IS_NULL".equals(predicate.name())) {
                        newStat = new ColumnLevelStatistics(OptionalLong.of(0),
                                Optional.empty(), Optional.empty(),
                                colStats.nullCount(), colStats.avgLen(),
                                colStats.maxLen(), Optional.empty());
                    }
                    else {
                        newStat = new ColumnLevelStatistics(
                                colStats.distinctCount(), colStats.min(),
                                colStats.max(), OptionalLong.of(0),
                                colStats.avgLen(), colStats.maxLen(), colStats.histogram());
                    }
                    statsMap.replace(reference, newStat);
                }
                return "IS_NULL".equals(predicate.name()) ?
                        nullPercent :
                        (1.0 - nullPercent);
            }
            case "AND": {
                final Expression[] children = predicate.children();
                if (children.length == 2 && (children[0] instanceof Predicate) && (children[1] instanceof Predicate)) {
                    return estimateOpSelectivity((Predicate) children[0], field,
                            reference, statistics, statsMap,
                            updateStatistics, traceToken) * estimateOpSelectivity(
                            (Predicate) children[1], field, reference,
                            statistics, statsMap, updateStatistics, traceToken);
                }
                return 1.0;
            }
            default:
                return 1.0;
        }
    }

    private static double estimateOrSelectivity(List<VastPredicate> predicates,
            Statistics statistics,
            Map<NamedReference, ColumnStatistics> statsMap, String traceToken)
    {
        return predicates
                .stream()
                .mapToDouble(p -> estimateOpSelectivity(p.getPredicate(),
                        p.getField(), p.getReference(), statistics, statsMap,
                        predicates.size() == 1, traceToken))
                .reduce((l, r) -> l + r - l * r)
                .orElse(1.0);
    }

    public static double estimateSelectivity(
            List<List<VastPredicate>> predicates, Statistics statistics, String traceToken)
    {
        if (!statistics.numRows().isPresent()) {
            return 1.0;
        }
        if (!(statistics instanceof TableLevelStatistics)) {
            LOG.warn("estimateSelectivity({}): No statistics", traceToken);
            return 1.0;
        }
        Map<NamedReference, ColumnStatistics> statsMap = new HashMap<>(
                statistics.columnStats()); // shallow copy, because we might change it
        return predicates.stream().mapToDouble(
                l -> estimateOrSelectivity(l, statistics, statsMap, traceToken)).reduce(1.0,
                (l, r) -> l * r);
    }

    private static Map<NamedReference, ColumnStatistics> updateColumnStatistics(
            final Map<NamedReference, ColumnStatistics> oldStats,
            Map<NamedReference, ColumnStatistics> newStats, double selectivity)
    {
        if (selectivity >= 1.0) {
            return newStats;
        }
        for (Map.Entry<NamedReference, ColumnStatistics> pair : oldStats.entrySet()) {
            ColumnStatistics updatedStats = newStats.get(pair.getKey());
            OptionalLong dc = updatedStats.distinctCount();
            if (dc.isPresent() && dc.getAsLong() > 1) {
                dc = OptionalLong.of((long) ceil(dc.getAsLong() * selectivity));
            }
            OptionalLong nc = updatedStats.nullCount();
            if (nc.isPresent() && nc.getAsLong() > 1) {
                nc = OptionalLong.of((long) ceil(nc.getAsLong() * selectivity));
            }
            newStats.replace(pair.getKey(),
                    new ColumnLevelStatistics(dc, updatedStats.min(),
                            updatedStats.max(), nc, updatedStats.avgLen(),
                            updatedStats.maxLen(), updatedStats.histogram()));
        }
        return newStats;
    }

    public static TableLevelStatistics estimateStatistics(
            List<List<VastPredicate>> predicates,
            TableLevelStatistics statistics,
            String traceToken)
    {
        if (!statistics.numRows().isPresent()) {
            return statistics;
        }
        if (!(statistics instanceof TableLevelStatistics)) {
            LOG.warn("estimateStatistics({}): No statistics", traceToken);
            return statistics;
        }
        Map<NamedReference, ColumnStatistics> colStats = new HashMap<NamedReference, ColumnStatistics>(
                (statistics).columnStats()); // shallow copy, because we might change it
        final double selectivity = predicates.stream().mapToDouble(
                l -> estimateOrSelectivity(l, statistics, colStats, traceToken)).reduce(1.0,
                (l, r) -> l * r);
        final UnaryOperator<Long> applySelectivity = stat -> (long) ceil(
                stat * selectivity);
        Map<NamedReference, ColumnStatistics> updatedColStats = updateColumnStatistics(
                statistics.columnStats(), colStats, selectivity);
        return new TableLevelStatistics(
                map(statistics.sizeInBytes(), applySelectivity),
                map(statistics.numRows(), applySelectivity), updatedColStats);
    }
}
