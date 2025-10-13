/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.predicate;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.StructField;

import java.io.Serializable;

import static java.util.Objects.hash;

public class VastPredicate
        implements Serializable
{
    private final Predicate predicate;
    private final NamedReference reference;
    private final StructField field;

    public VastPredicate(Predicate predicate, NamedReference reference, StructField field)
    {
        this.predicate = predicate;
        this.reference = reference;
        this.field = field;
    }

    public Predicate getPredicate()
    {
        return predicate;
    }

    public NamedReference getReference()
    {
        return reference;
    }

    public StructField getField()
    {
        return field;
    }

    public String toString() {
        return this.getPredicate().toString();
    }

    @Override
    public int hashCode()
    {
        Expression[] children = predicate.children();
        if (children.length == 1) {
            return hash(predicate.name(), field);
        }
        if (children.length != 2) {
            return predicate.hashCode();
        }
        if (predicate.name().equals("AND")) {
            if ((children[0] instanceof Predicate) && (children[1] instanceof Predicate) &&
                (((Predicate)children[0]).children()[1] instanceof Literal) &&
                (((Predicate)children[1]).children()[1] instanceof Literal)) {
                Predicate lp = (Predicate)children[0];
                Predicate rp = (Predicate)children[1];
                Literal ll = (Literal)lp.children()[1];
                Literal rl = (Literal)rp.children()[1];
                return hash("AND", field, lp.name(), rp.name(), ll.value(), rl.value());
            }
            return predicate.hashCode();
        }
        else if (children[1] instanceof Literal) {
            return hash(predicate.name(), field, ((Literal) children[1]).value());
        }
        return predicate.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof VastPredicate)) {
            return false;
        }
        VastPredicate other = (VastPredicate)o;
        Expression[] children = predicate.children();
        Expression[] ochildren = other.predicate.children();
        if (!field.equals(other.field) || predicate.name() != other.predicate.name() || children.length != ochildren.length) {
            return false;
        }
        if (children.length == 1) {
            return predicate.name().equals(other.predicate.name()) && field.equals(other.field);
        }
        if (children.length != 2) {
            return predicate.equals(other.predicate);
        }
        if (predicate.name().equals("AND")) {
            if ((children[0] instanceof Predicate) && (children[1] instanceof Predicate) &&
                (((Predicate)children[0]).children()[1] instanceof Literal) &&
                (((Predicate)children[1]).children()[1] instanceof Literal) &&
                (ochildren[0] instanceof Predicate) && (ochildren[1] instanceof Predicate) &&
                (((Predicate)ochildren[0]).children()[1] instanceof Literal) &&
                (((Predicate)ochildren[1]).children()[1] instanceof Literal)) {
                Predicate lp = (Predicate)children[0];
                Predicate rp = (Predicate)children[1];
                Predicate olp = (Predicate)ochildren[0];
                Predicate orp = (Predicate)ochildren[1];
                Literal ll = (Literal)lp.children()[1];
                Literal rl = (Literal)rp.children()[1];
                Literal oll = (Literal)olp.children()[1];
                Literal orl = (Literal)orp.children()[1];
                return lp.name().equals(olp.name()) && rp.name().equals(orp.name()) &&
                    ll.value().equals(oll.value()) && rl.value().equals(orl.value());
            }
            return predicate.equals(other.predicate);
        }
        else if ((children[1] instanceof Literal) && (ochildren[1] instanceof Literal)) {
            return ((Literal) children[1]).value().equals(((Literal)ochildren[1]).value());
        }
        return predicate.equals(other.predicate);
    }
}
