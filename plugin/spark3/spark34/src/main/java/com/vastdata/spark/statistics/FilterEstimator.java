package com.vastdata.spark.statistics;

import com.vastdata.spark.predicate.VastPredicate;

import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils;
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.ValueInterval;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.connector.read.Statistics;
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

import scala.jdk.javaapi.OptionConverters;

/* A simple statistics-based filter selectivity estimator, based on Spark's FilterEstimation.scala
 * TODO: Add hystogram-based estimation, once we have hystograms
 */
public final class FilterEstimator
{
    private static final Logger LOG = LoggerFactory.getLogger(FilterEstimator.class);
    private static double estimateOpSelectivity(Predicate predicate, StructField field, NamedReference reference, Statistics statistics)
    {
        ColumnStatistics colStats = statistics.columnStats().get(reference);
        if (colStats == null){
            return 1.0;
        }
        switch (predicate.name()) {
        case "<>":
        case "!=":
        case "=":
        {
            if (!colStats.min().isPresent() || !colStats.max().isPresent()) {
                return 1.0;
            }
            ValueInterval statsInterval =
                ValueInterval.apply(OptionConverters.toScala(colStats.min()), OptionConverters.toScala(colStats.max()), field.dataType());
            org.apache.spark.sql.connector.expressions.Literal jLiteral =
                (org.apache.spark.sql.connector.expressions.Literal) predicate.children()[1];
            org.apache.spark.sql.catalyst.expressions.Literal sLiteral = 
                new org.apache.spark.sql.catalyst.expressions.Literal(jLiteral.value(), jLiteral.dataType());
            double percent = 1.0;
            if (statsInterval.contains(sLiteral)) {
                if (colStats.distinctCount().isPresent()) {
                    percent = 1.0/colStats.distinctCount().getAsLong();
                }
                percent = 1.0;
            }
            else {
                percent = 0.0;
            }
            return predicate.name() == "="? percent : (1.0 - percent);
        }
        case ">":
        case "<":
        case "<=":
        case ">=":
        {
            DataType dt = field.dataType();
            // Non-numeric types???
            if (!((dt instanceof NumericType) || (dt instanceof DateType) || (dt instanceof TimestampType) || (dt instanceof BooleanType))  ||
                !colStats.min().isPresent() || !colStats.max().isPresent() || !colStats.distinctCount().isPresent()) {
                return 1.0;
            }
            org.apache.spark.sql.connector.expressions.Literal literal =
                (org.apache.spark.sql.connector.expressions.Literal) predicate.children()[1];
            ValueInterval statsInterval =
                ValueInterval.apply(OptionConverters.toScala(colStats.min()), OptionConverters.toScala(colStats.max()), dt);
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
            return percent;
        }
        case "IS_NULL":
        case "IS_NOT_NULL":
        {
            if (!colStats.nullCount().isPresent()) {
                return 1.0;
            }
            long rowCount = statistics.numRows().getAsLong();
            long nullCount = colStats.nullCount().getAsLong();
            double nullPercent = rowCount == 0? 0.0 : (nullCount >= rowCount? 1.0 : (double) nullCount/(double) rowCount);
            return predicate.name() == "IS_NULL"? nullPercent : (1.0 - nullPercent);
        }
        case "AND":
        {
            Expression[] children = predicate.children();
            if (children.length == 2 && (children[0] instanceof Predicate) && (children[1] instanceof Predicate)) {
                return estimateOpSelectivity((Predicate) children[0], field, reference, statistics) * estimateOpSelectivity((Predicate) children[1], field, reference, statistics);
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
            .reduce(0, (l, r) -> l + r);
    }
};
