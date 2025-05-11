package com.vastdata.vdb.sdk;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.vastdata.client.VastExpressionSerializer;
import com.vastdata.client.schema.EnumeratedSchema;

import static com.vastdata.client.schema.ArrowSchemaUtils.VASTDB_EXTERNAL_ROW_ID_COLUMN_NAME;

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
    private final EnumeratedSchema schema;

    RowIDPredicateSerializer(long rowid, EnumeratedSchema enumeratedSchema)
    {
        this.rowid = rowid;
        this.schema = enumeratedSchema;
    }
    
    private static int getColumnPosition(String colName, EnumeratedSchema enumeratedSchema)
    {
        // TODO: only predicates over scalar columns are supported
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        enumeratedSchema.collectProjectionIndices(colName, ImmutableList.of(), builder::add);
        return Iterables.getOnlyElement(builder.build());
    }


    @Override
    public int serialize()
    {
        final int literalValueOffset = Expression.createExpression(builder, ExpressionImpl.Literal, Literal.createLiteral(
                                                                                                                          builder,
                                                                                                                          LiteralImpl.Int64Literal,
                                                                                                                          Int64Literal.createInt64Literal(builder, this.rowid),
                                                                                                                          field.getField(builder)));

        final int columnPosition = RowIDPredicateSerializer.getColumnPosition(VASTDB_EXTERNAL_ROW_ID_COLUMN_NAME, this.schema);
        final int columnOffset = buildColumn(columnPosition);
        final int equalRowidOffset = buildEqual(columnOffset, literalValueOffset);

        return buildAnd(buildOr(equalRowidOffset));
    }
}
