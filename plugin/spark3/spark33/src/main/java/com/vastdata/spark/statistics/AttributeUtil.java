/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.statistics;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public final class AttributeUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(AttributeUtil.class);

    private AttributeUtil() {}

    public static Attribute replaceAttributeExpId(Attribute oldAttr, Function<Attribute, ExprId> newExpressionIdProvider)
    {
        LOG.debug("Building new attribute from column: {}, id: {}", oldAttr.name(), oldAttr.exprId());
        AttributeReference newAttr = new AttributeReference(
                oldAttr.name(),
                oldAttr.dataType(),
                oldAttr.nullable(),
                oldAttr.metadata(),
                newExpressionIdProvider.apply(oldAttr),
                oldAttr.qualifier());
        LOG.debug("Built new attribute of column {}, new id {}", newAttr.name(), newAttr.exprId());
        return newAttr;
    }
}
