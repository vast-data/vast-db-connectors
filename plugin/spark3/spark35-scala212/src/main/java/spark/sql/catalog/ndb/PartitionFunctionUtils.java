/*
 *  Copyright (C) Vast Data Ltd.
 */
package spark.sql.catalog.ndb;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

import java.util.Optional;
import java.util.function.BiFunction;

public final class PartitionFunctionUtils
{
    public static Optional<Predicate> applyRecurse(Predicate predicate,
            NamedReference newRef,
            BiFunction<Predicate, NamedReference, Optional<Predicate>> leaf)
    {
        if (predicate.children().length == 1) {
            return Optional.of(
                    new Predicate(predicate.name(), new Expression[] {newRef}));
        }
        if (predicate.name().equals("AND")) {
            Optional<Predicate> lhs = applyRecurse(
                    (Predicate) predicate.children()[0], newRef, leaf);
            Optional<Predicate> rhs = applyRecurse(
                    (Predicate) predicate.children()[1], newRef, leaf);
            if (!lhs.isPresent()) {
                return rhs;
            }
            return rhs
                    .map(value -> Optional.of(new Predicate("AND",
                            new Expression[] {lhs.get(), value})))
                    .orElse(lhs);
        }
        return leaf.apply(predicate, newRef);
    }
}
