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
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

import java.util.Optional;

import static org.apache.iceberg.util.DateTimeUtil.microsToHours;
import static spark.sql.catalog.ndb.PartitionFunctionUtils.applyRecurse;

public class HoursFunction
        extends org.apache.iceberg.spark.functions.HoursFunction
{
    private static Optional<Predicate> internalApply(Predicate predicate,
            NamedReference newRef)
    {
        final Literal<?> literal = (Literal<?>) predicate.children()[1];
        DataType type = literal.dataType();
        if (!(type instanceof TimestampType) && !(type instanceof TimestampNTZType)) {
            throw new UnsupportedOperationException(
                    "Expected value to be timestamp: " + type.catalogString());
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
                        microsToHours(val))};
                return Optional.of(new Predicate(predicate.name(), tl));
            }
            case ">":
            case "<": {
                Expression[] tl = new Expression[] {newRef, Expressions.literal(
                        microsToHours(val + ("<".equals(predicate.name()) ?
                                -1 :
                                1)))};
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
        return applyRecurse(predicate, newRef, HoursFunction::internalApply);
    }

    @Override
    public String description()
    {
        return name() + "(col) - Call Vast's hour transform\n" + "  col :: source column (must be timestamp)";
    }
}
