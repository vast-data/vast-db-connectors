/*
 *  Copyright (C) Vast Data Ltd.
 */
package spark.sql.catalog.ndb;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

import java.util.Optional;
import java.util.function.LongFunction;

import static org.apache.iceberg.util.DateTimeUtil.daysToYears;
import static org.apache.iceberg.util.DateTimeUtil.microsToYears;
import static spark.sql.catalog.ndb.PartitionFunctionUtils.applyRecurse;

public class YearsFunction
        extends org.apache.iceberg.spark.functions.YearsFunction
{

    private static Optional<Predicate> internalApply(Predicate predicate,
            NamedReference newRef)
    {
        final Literal literal = (Literal) predicate.children()[1];
        DataType type = literal.dataType();
        LongFunction transform;
        if (type instanceof DateType) {
            transform = i -> daysToYears((int) i);
        }
        else if ((type instanceof TimestampType) || (type instanceof TimestampNTZType)) {
            transform = l -> microsToYears(l);
        }
        else {
            throw new UnsupportedOperationException(
                    "Expected value to be date or timestamp: " + type.catalogString());
        }

        long val = literal.value() instanceof Long ?
                (Long) literal.value() :
                (Integer) literal.value();
        switch (predicate.name()) {
            case "<>":
            case "!=":
                return Optional.empty();
            case "=":
            case "<=":
            case ">=": {
                Expression[] tl = new Expression[] {newRef, Expressions.literal(
                        transform.apply(val))};
                return Optional.of(new Predicate(predicate.name(), tl));
            }
            case ">":
            case "<": {
                Expression[] tl = new Expression[] {newRef, Expressions.literal(
                        transform.apply(
                                val + (predicate.name() == "<" ? -1 : 1)))};
                return Optional.of(new Predicate(predicate.name() + "=", tl));
            }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported predicate: " + predicate.name());
        }
    }

    public static Optional<Predicate> apply(Predicate predicate,
            NamedReference newRef)
    {
        return applyRecurse(predicate, newRef, YearsFunction::internalApply);
    }

    @Override
    public String description()
    {
        return name() + "(col) - Call Vast's year transform\n" + "  col :: source column (must be date or timestamp)";
    }

}
