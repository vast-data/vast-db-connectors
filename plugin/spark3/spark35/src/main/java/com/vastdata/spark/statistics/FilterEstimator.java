package com.vastdata.spark.statistics;

import com.vastdata.spark.predicate.VastPredicate;
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils;
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.ValueInterval;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
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
import scala.jdk.javaapi.OptionConverters;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.UnaryOperator;

import static com.vastdata.OptionalPrimitiveHelpers.map;
import static java.lang.Math.ceil;

/* A simple statistics-based filter selectivity estimator, based on Spark's FilterEstimation.scala
 * TODO: Add hystogram-based estimation, once we have hystograms
 */
public final class FilterEstimator
{
    private static final Logger LOG = LoggerFactory.getLogger(FilterEstimator.class);

    private FilterEstimator() {}

    private static boolean isStringOrBinaryOrWithinRange(ColumnStatistics colStats, StructField field, Predicate predicate)
    {
        final ValueInterval statsInterval =
            ValueInterval.apply(OptionConverters.toScala(colStats.min()), OptionConverters.toScala(colStats.max()), field.dataType());
        final org.apache.spark.sql.connector.expressions.Literal jLiteral =
            (org.apache.spark.sql.connector.expressions.Literal) predicate.children()[1];
        final org.apache.spark.sql.catalyst.expressions.Literal sLiteral =
            new org.apache.spark.sql.catalyst.expressions.Literal(jLiteral.value(), jLiteral.dataType());
        return statsInterval.contains(sLiteral);
    }

    private static double estimateOpSelectivity(Predicate predicate, StructField field, NamedReference reference,
                                                Statistics statistics, Map<NamedReference, ColumnStatistics> statsMap,
                                                boolean updateStatistics)
    {
        final ColumnStatistics colStats =  statsMap.get(reference);
        if (colStats == null){
            return 1.0;
        }
        final DataType dt = field.dataType();
        switch (predicate.name()) {
        case "<>":
        case "!=":
        case "=":
        {
            double percent = 1.0;
            if (isStringOrBinaryOrWithinRange(colStats, field, predicate)) {
                if (updateStatistics && "=".equals(predicate.name())) {
                    ColumnStatistics newStat = null;
                    if ((dt instanceof StringType) || (dt instanceof BinaryType)) {
                        newStat =
                            new ColumnLevelStatistics(OptionalLong.of(1), colStats.min(), colStats.max(),
                                                      OptionalLong.of(0), colStats.avgLen(), colStats.maxLen());
                    }
                    else {
                        final org.apache.spark.sql.connector.expressions.Literal jLiteral =
                            (org.apache.spark.sql.connector.expressions.Literal) predicate.children()[1];
                        newStat =
                            new ColumnLevelStatistics(OptionalLong.of(1), Optional.of(jLiteral.value()),
                                                      Optional.of(jLiteral.value()), OptionalLong.of(0),
                                                      colStats.avgLen(), colStats.maxLen());
                    }
                    statsMap.replace(reference, newStat);
                }
                if (colStats.distinctCount().isPresent()) {
                    percent = 1.0/colStats.distinctCount().getAsLong();
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
            // Non-numeric types???
            if (!((dt instanceof NumericType) || (dt instanceof DateType) || (dt instanceof TimestampType) || (dt instanceof BooleanType))  ||
                !colStats.min().isPresent() || !colStats.max().isPresent() || !colStats.distinctCount().isPresent()) {
                return 1.0;
            }
            org.apache.spark.sql.connector.expressions.Literal literal =
                (org.apache.spark.sql.connector.expressions.Literal) predicate.children()[1];
            double max = EstimationUtils.toDouble(colStats.max().get(), literal.dataType());
            double min = EstimationUtils.toDouble(colStats.min().get(), literal.dataType());
            long ndv = colStats.distinctCount().getAsLong();
            double numericLiteral = EstimationUtils.toDouble(literal.value(), literal.dataType());
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
                ColumnStatistics newStat =
                    new ColumnLevelStatistics(OptionalLong.of((long)ceil(ndv * percent)), newMin, newMax,
                                              OptionalLong.of(0), colStats.avgLen(), colStats.maxLen());
                statsMap.replace(reference, newStat);
            }
            return percent;
        }
        case "IS_NULL":
        case "IS_NOT_NULL":
        {
            if (!colStats.nullCount().isPresent()) {
                return 1.0;
            }
            final long rowCount = statistics.numRows().getAsLong();
            final long nullCount = colStats.nullCount().getAsLong();
            final double nullPercent = rowCount == 0? 0.0 : (nullCount >= rowCount? 1.0 : (double) nullCount/(double) rowCount);
            if (updateStatistics) {
                ColumnStatistics newStat = null;
                if ("IS_NULL".equals(predicate.name())) {
                    newStat =
                        new ColumnLevelStatistics(OptionalLong.of(0), Optional.empty(), Optional.empty(),
                                                  colStats.nullCount(), colStats.avgLen(), colStats.maxLen());
                }
                else {
                    newStat =
                        new ColumnLevelStatistics(colStats.distinctCount(), colStats.min(), colStats.max(),
                                                  OptionalLong.of(0), colStats.avgLen(), colStats.maxLen());
                }
                statsMap.replace(reference, newStat);
            }
            return "IS_NULL".equals(predicate.name()) ? nullPercent : (1.0 - nullPercent);
        }
        case "AND":
        {
            Expression[] children = predicate.children();
            if (children.length == 2 && (children[0] instanceof Predicate) && (children[1] instanceof Predicate)) {
                return estimateOpSelectivity((Predicate) children[0], field, reference, statistics, statsMap, updateStatistics)
                    * estimateOpSelectivity((Predicate) children[1], field, reference, statistics, statsMap, updateStatistics);
            }
            return 1.0;
        }
        default:
            return 1.0;
        }
    }

    private static double estimateOrSelectivity(List<VastPredicate> predicates, Statistics statistics,
                                                Map<NamedReference, ColumnStatistics> statsMap)
    {
        return predicates.stream()
            .mapToDouble(p -> estimateOpSelectivity(p.getPredicate(), p.getField(), p.getReference(), statistics,
                                                    statsMap, predicates.size() == 1))
            .reduce((l, r) -> l + r - l * r)
            .orElse(1.0);
    }

    public static double estimateSelectivity(List<List<VastPredicate>> predicates, Statistics statistics)
    {
        if (!statistics.numRows().isPresent())
            return 1.0;
        if (!(statistics instanceof TableLevelStatistics)) {
            LOG.warn("No statistics");
            return 1.0;
        }
        Map<NamedReference, ColumnStatistics> statsMap =
            new HashMap<NamedReference, ColumnStatistics>(((TableLevelStatistics)statistics).columnStats()); // shallow copy, because we might change it
        return predicates.stream()
            .mapToDouble(l -> estimateOrSelectivity(l, statistics, statsMap))
            .reduce(1.0, (l, r) -> l * r);
    }

    private static Map<NamedReference, ColumnStatistics> updateColumnStatistics(final Map<NamedReference, ColumnStatistics> oldStats,
                                                                                Map<NamedReference, ColumnStatistics> newStats,
                                                                                double selectivity)
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
                             new ColumnLevelStatistics(dc, updatedStats.min(), updatedStats.max(), nc,
                                                       updatedStats.avgLen(), updatedStats.maxLen()));
        }
        return newStats;
    }

    public static TableLevelStatistics estimateStatistics(List<List<VastPredicate>> predicates,
                                                          TableLevelStatistics statistics)
    {
        if (!statistics.numRows().isPresent())
            return statistics;
        if (!(statistics instanceof TableLevelStatistics)) {
            LOG.warn("No statistics");
            return statistics;
        }
        Map<NamedReference, ColumnStatistics> colStats =
            new HashMap<NamedReference, ColumnStatistics>((statistics).columnStats()); // shallow copy, because we might change it
        final double selectivity = predicates.stream()
            .mapToDouble(l -> estimateOrSelectivity(l, statistics, colStats))
            .reduce(1.0, (l, r) -> l * r);
        final UnaryOperator<Long> applySelectivity = stat -> (long) ceil(stat * selectivity);
        Map<NamedReference, ColumnStatistics> updatedColStats = updateColumnStatistics(statistics.columnStats(), colStats, selectivity);
        return new TableLevelStatistics(map(statistics.sizeInBytes(), applySelectivity),
                                        map(statistics.numRows(), applySelectivity),
                                        updatedColStats);
    }

    private static long fieldSize(StructField field, Map<NamedReference, ColumnStatistics> statsMap)
    {
        NamedReference colRef = Expressions.column(field.name());
        ColumnStatistics colStats = statsMap.get(colRef);
        if (colStats == null || !colStats.avgLen().isPresent()) {
            LOG.debug("No statistics available for {}", field.name());
            return field.dataType().defaultSize();
        }
        else {
            return colStats.avgLen().getAsLong() + ((field.dataType() instanceof StringType)? (8 + 4) : 0);
        }
    }
    /*
     * Reimplementation of Sparks's EstimationUtils.getSizePerRow()
     */
    public static long getSizePerRow(StructType schema, Statistics statistics)
    {
        return  8 +
            Arrays.stream(schema.fields())
            .mapToLong(f -> fieldSize(f, statistics.columnStats()))
            .reduce(0, Long::sum);
    }
}
