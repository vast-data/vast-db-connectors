package com.vastdata.spark.statistics;

import com.vastdata.spark.predicate.VastPredicate;

import org.apache.spark.sql.catalyst.plans.logical.ColumnStat;
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils;
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.ValueInterval;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/* A simple statistics-based filter selectivity estimator, based on Spark's FilterEstimation.scala
 * TODO: Add hystogram-based estimation, once we have hystograms
 */
public final class FilterEstimator
{
    private static final Logger LOG = LoggerFactory.getLogger(FilterEstimator.class);

    private FilterEstimator() {}

    private static boolean isStringOrBinaryOrWithinRange(ColumnStat colStats, StructField field, Predicate predicate)
    {
        final ValueInterval statsInterval =
            ValueInterval.apply(colStats.min(), colStats.max(), field.dataType());
        final org.apache.spark.sql.connector.expressions.Literal jLiteral =
            (org.apache.spark.sql.connector.expressions.Literal) predicate.children()[1];
        final org.apache.spark.sql.catalyst.expressions.Literal sLiteral =
            new org.apache.spark.sql.catalyst.expressions.Literal(jLiteral.value(), jLiteral.dataType());
        return statsInterval.contains(sLiteral);
    }

    private static double estimateOpSelectivity(Predicate predicate, StructField field, NamedReference reference, Statistics statistics)
    {
        LOG.info("Estimating predicate: {}: name: {}", predicate.toString(), predicate.name());
        if (!(statistics instanceof TableLevelStatistics)) {
            LOG.warn("No statistics");
            return 1.0;
        }
        final TableLevelStatistics stats = ((TableLevelStatistics) statistics);
        final ColumnStat colStats = stats.columnStats().get(reference);
        if (colStats == null){
            LOG.warn("No column statistics for {}", field.name());
            return 1.0;
        }
        switch (predicate.name()) {
        case "<>":
        case "!=":
        case "=":
        {
            double percent = 1.0;
            if (isStringOrBinaryOrWithinRange(colStats, field, predicate)) {
                if (colStats.distinctCount().isDefined()) {
                    percent = 1.0/colStats.distinctCount().get().toLong();
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
        case ">=":
        {
            final DataType dt = field.dataType();
            // Non-numeric types???
            if (!((dt instanceof NumericType) || (dt instanceof DateType) || (dt instanceof TimestampType) || (dt instanceof BooleanType))  ||
                colStats.min().isEmpty() || colStats.max().isEmpty() || colStats.distinctCount().isEmpty()) {
                return 1.0;
            }
            final org.apache.spark.sql.connector.expressions.Literal literal =
                (org.apache.spark.sql.connector.expressions.Literal) predicate.children()[1];
            final double max = EstimationUtils.toDouble(colStats.max().get(), literal.dataType());
            final double min = EstimationUtils.toDouble(colStats.min().get(), literal.dataType());
            final long ndv = colStats.distinctCount().get().toLong();
            final double numericLiteral = EstimationUtils.toDouble(literal.value(), literal.dataType());
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
            else {
                switch (predicate.name()) {
                case "<":
                    if (numericLiteral == max) {
                        percent = 1.0 - 1.0/ndv;
                    }
                    else {
                        percent = (numericLiteral - min)/(max - min);
                    }
                    break;
                case "<=":
                    if (numericLiteral == min) {
                        percent = 1.0/ndv;
                    }
                    else {
                        percent = (numericLiteral - min)/(max - min);
                    }
                    break;
                case ">":
                    if (numericLiteral == min) {
                        percent = 1.0 - 1.0/ndv;
                    }
                    else {
                        percent = (max - numericLiteral)/(max - min);
                    }
                    break;
                case ">=":
                    if (numericLiteral == max) {
                        percent = 1.0/ndv;
                    }
                    else {
                        percent = (max - numericLiteral)/(max - min);
                    }
                    break;
                }
            }
            LOG.info("Estimating predicate: {}: name: {} min: {} max: {} selectivity: {}", predicate.toString(), predicate.name(), min, max, percent);
            return percent;
        }
        case "IS_NULL":
        case "IS_NOT_NULL":
        {
            if (colStats.nullCount().isEmpty()) {
                return 1.0;
            }
            final long rowCount = statistics.numRows().getAsLong();
            final long nullCount = colStats.nullCount().get().toLong();
            final double nullPercent = rowCount == 0? 0.0 : (nullCount >= rowCount? 1.0 : (double) nullCount/(double) rowCount);
            return "IS_NULL".equals(predicate.name()) ? nullPercent : (1.0 - nullPercent);
        }
        case "AND":
        {
            final Expression[] children = predicate.children();
            if (children.length == 2 && (children[0] instanceof Predicate) && (children[1] instanceof Predicate)) {
                return estimateOpSelectivity((Predicate) children[0], field, reference, statistics)
                        * estimateOpSelectivity((Predicate) children[1], field, reference, statistics);
            }
            return 1.0;
        }
        default:
            return 1.0;
        }
    }

    private static double estimateOrSelectivity(List<VastPredicate> predicates, Statistics statistics)
    {
        return predicates.stream()
            .mapToDouble(p -> estimateOpSelectivity(p.getPredicate(), p.getField(), p.getReference(), statistics))
            .reduce((l, r) -> l + r - l * r)
            .orElse(1.0);
    }

    public static double estimateSelectivity(List<List<VastPredicate>> predicates, Statistics statistics)
    {
        if (!statistics.numRows().isPresent())
            return 1.0;
        return predicates.stream()
            .mapToDouble(l -> estimateOrSelectivity(l, statistics))
            .reduce(1.0, (l, r) -> l * r);
    }


    private static long fieldSize(StructField field, Map<NamedReference, ColumnStat> statsMap)
    {
        final NamedReference colRef = Expressions.column(field.name());
        final ColumnStat colStats = statsMap.get(colRef);
        if (colStats == null || colStats.avgLen().isEmpty()) {
            LOG.debug("No statistics available for {}", field.name());
            return field.dataType().defaultSize();
        }
        else {
            return ((long) colStats.avgLen().get()) + ((field.dataType() instanceof StringType)? (8 + 4) : 0);
        }
    }

    /*
     * Reimplementation of Sparks's EstimationUtils.getSizePerRow()
     */
    public static long getSizePerRow(StructType schema, TableLevelStatistics statistics)
    {
        return  8 +
            Arrays.stream(schema.fields())
            .mapToLong(f -> fieldSize(f, statistics.columnStats()))
            .reduce(0, Long::sum);
    }
}
