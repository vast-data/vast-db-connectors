/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.flatbuffers.FlatBufferBuilder;
import com.vastdata.client.schema.EnumeratedSchema;
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.computeir.flatbuf.Call;
import org.apache.arrow.computeir.flatbuf.Expression;
import org.apache.arrow.computeir.flatbuf.FieldIndex;
import org.apache.arrow.computeir.flatbuf.FieldRef;
import org.apache.arrow.computeir.flatbuf.Int8Literal;
import org.apache.arrow.computeir.flatbuf.Literal;
import org.apache.arrow.computeir.flatbuf.Relation;
import org.apache.arrow.computeir.flatbuf.Source;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestVastPredicate
{
    @Test
    public void testAll()
    {
        EnumeratedSchema enumeratedSchema = new EnumeratedSchema(List.of());
        TrinoPredicateSerializer builder = new TrinoPredicateSerializer(TupleDomain.all(), Optional.empty(), List.of(), enumeratedSchema);
        FlatBufferBuilder flatbuffer = new FlatBufferBuilder();
        flatbuffer.finish(builder.serialize(flatbuffer));

        int pos = flatbuffer.dataBuffer().position();
        assertThat(flatbuffer.sizedByteArray())
                .asHexString()
                .isEqualTo("0C00000008000C000700080008000000000000030C00000008000C00080004000800000008000000080000000000000003000000616E6400");

        flatbuffer.dataBuffer().position(pos);
        Expression e = Expression.getRootAsExpression(flatbuffer.dataBuffer());
        Call c = (Call) e.impl(new Call());
        assertThat(c.name()).isEqualTo("and");
        assertThat(c.argumentsLength()).isEqualTo(0);
    }

    @Test
    public void testRelation()
    {
        // CREATE TABLE t (b INTEGER, a TINYINT);
        // SELECT b FROM "/bucket-for-filter/schema-for-filter".table1 WHERE a = 2
        byte[] bytes = HexFormat.of().parseHex("04000000d0feffff000000091400000000000e0014000000100004000c0008000e000000200000000c00000038010000300200000100000008000000040004000400000024ffffff000000030400000040feffff080000000c000000010000000c00000003000000616e64004cffffff000000030400000068feffff080000000c000000010000000c000000020000006f72000074ffffff000000030400000090feffff080000001000000002000000780000001000000005000000657175616c00000090ffffff000000011000000000000a001200070008000c000a000000000000021000000020000000000006000800070006000000000000020c000c0000000600070008000c0000000000010204000000d0feffff000000010800000008000c000700080008000000000000020c00000008000e000700080008000000000000050c000000000006000a000400060000000100000000000a000c000000080004000a000000080000000800000000000000020000007800000004000000a2ffffff1400000040000000400000000000020144000000010000000400000090ffffff080000000c00000001000000300000000e000000564153543a636f6c756d6e5f696400000000000088ffffff0000000108000000010000006100120018001400130012000c00000008000400120000001400000048000000500000000000020154000000010000000c00000008000c000800040008000000080000000c00000001000000310000000e000000564153543a636f6c756d6e5f696400000000000008000c000800070008000000000000011000000001000000620000002b0000002f6275636b65742d666f722d66696c7465722f736368656d612d666f722d66696c7465722f7461626c653100");
        Relation relation = Relation.getRootAsRelation(ByteBuffer.wrap(bytes));
        Source source = (Source) relation.impl(new Source());
        assertThat(source.name()).isEqualTo("/bucket-for-filter/schema-for-filter/table1");

        FieldIndex.Vector projections = source.projectionVector();
        assertThat(projections.length()).isEqualTo(1);
        assertThat(projections.get(0).position()).isEqualTo(0); // project field #0 in schema

        Expression filter = source.filter();
        Call call = (Call) filter.impl(new Call());
        assertThat(call.name()).isEqualTo("and");
        Expression.Vector args = call.argumentsVector();
        assertThat(args.length()).isEqualTo(1);

        Expression or = args.get(0);
        call = (Call) or.impl(new Call());
        assertThat(call.name()).isEqualTo("or");
        args = call.argumentsVector();
        assertThat(args.length()).isEqualTo(1);

        Expression range = args.get(0);
        call = (Call) range.impl(new Call());
        assertThat(call.name()).isEqualTo("equal"); // filter predicate type
        args = call.argumentsVector();
        assertThat(args.length()).isEqualTo(2);

        Expression columnExpr = args.get(0);
        Expression literalExpr = args.get(1);

        FieldRef ref = (FieldRef) columnExpr.impl(new FieldRef());
        FieldIndex index = (FieldIndex) ref.ref(new FieldIndex());
        assertThat(index.position()).isEqualTo(1); // filter field #1 in schema

        Literal literal = (Literal) literalExpr.impl(new Literal());

        Field field = Field.convertField(literal.type());
        Field expected = Field.nullablePrimitive(null, new ArrowType.Int(8, true));
        assertThat(field).isEqualTo(expected);

        Int8Literal int8 = (Int8Literal) literal.impl(new Int8Literal());
        assertThat(int8.value()).isEqualTo((byte) 2); // filter predicate value
    }
}
