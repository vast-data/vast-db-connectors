package com.vastdata.vdb.sdk;

import com.vastdata.client.VastExpressionSerializer;
import org.apache.arrow.computeir.flatbuf.Expression;
import org.apache.arrow.computeir.flatbuf.ExpressionImpl;
import org.apache.arrow.computeir.flatbuf.Int64Literal;
import org.apache.arrow.computeir.flatbuf.Literal;
import org.apache.arrow.computeir.flatbuf.LiteralImpl;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowIDPredicateSerializer
        extends VastExpressionSerializer
{
    private static final Logger LOG = LoggerFactory.getLogger(RowIDPredicateSerializer.class);

    private final Field field = Field.notNullable(null, new ArrowType.Int(64, true));

    private final long rowid;

    RowIDPredicateSerializer(long rowid)
    {
        this.rowid = rowid;
    }

    @Override
    public int serialize()
    {
        final int literalValueOffset = Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                builder,
                LiteralImpl.Int64Literal,
                Int64Literal.createInt64Literal(builder, this.rowid),
                field.getField(builder)));
        final int columnOffset = buildColumn(0);
        final int equalRowidOffset = buildEqual(columnOffset, literalValueOffset);

        return buildAnd(buildOr(equalRowidOffset));
    }
}
