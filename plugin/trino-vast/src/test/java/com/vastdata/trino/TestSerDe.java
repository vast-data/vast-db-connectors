/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.Streams;
import com.vastdata.client.QueryDataPagination;
import com.vastdata.client.VastDebugConfig;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.client.tx.VastTraceToken;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSerDe
{
    @Test
    public void testQueryDataStream()
    {
        byte[] response = HexFormat.of().parseHex(
                // first column
                "000000000300000000000000ffffffffb80000001000000000000a000c000600050008000a000000000104000c0000000800080000000400080000000400000001000000180000000000120018000800060007000c00000010001400120000000000010214000000580000000800000010000000000000000000000000000000010000000c00000008000c00040008000800000008000000180000000e000000564153543a636f6c756d6e5f69640000010000003000000008000c0008000700080000000000000108000000" +
                "ffffffff" + // keepalive
                "000000000300000000000000ffffffff8800000014000000000000000c0016000600050008000c000c0000000003040018000000080000000000000000000a0018000c00040008000a0000003c000000100000000300000000000000000000000200000000000000000000000000000000000000000000000000000003000000000000000000000001000000030000000000000000000000000000004455660000000000" +
                "000000000300000000000000ffffffff8800000014000000000000000c0016000600050008000c000c0000000003040018000000080000000000000000000a0018000c00040008000a0000003c000000100000000300000000000000000000000200000000000000000000000000000000000000000000000000000003000000000000000000000001000000030000000000000000000000000000001122330000000000" +
                "000000000300000000000000ffffffff00000000" + // EOS
                "ffffffff" + // keepalive
                // second column
                "000000000300000000000000ffffffffb80000001000000000000a000c000600050008000a000000000104000c0000000800080000000400080000000400000001000000180000000000120018000800060007000c00000010001400120000000000010214000000580000000800000010000000000000000000000000000000010000000c00000008000c00040008000800000008000000180000000e000000564153543a636f6c756d6e5f69640000010000003100000008000c0008000700080000000000000140000000" +
                "000000000300000000000000ffffffff8800000014000000000000000c0016000600050008000c000c0000000003040018000000180000000000000000000a0018000c00040008000a0000003c00000010000000030000000000000000000000020000000000000000000000000000000000000000000000000000001800000000000000000000000100000003000000000000000000000000000000111111111111111122222222222222223333333333333333" +
                "ffffffff" + // keepalive
                "000000000300000000000000ffffffff8800000014000000000000000c0016000600050008000c000c0000000003040018000000180000000000000000000a0018000c00040008000a0000003c00000010000000030000000000000000000000020000000000000000000000000000000000000000000000000000001800000000000000000000000100000003000000000000000000000000000000444444444444444455555555555555556666666666666666" +
                "ffffffff" + // keepalive
                "000000000300000000000000ffffffff00000000" + // EOS
                "feffffff"); // complete

        List<Field> fields = List.of(
                Field.nullable("a", new ArrowType.Int(8, true)),
                Field.nullable("x", new ArrowType.Int(32, true)),
                Field.nullable("b", new ArrowType.Int(64, true)));

        VastTraceToken traceToken = new VastTraceToken(Optional.empty(), 123L, 1);
        LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> baseFieldWithProjections =
                new QueryDataBaseFieldsWithProjectionsMappingBuilder()
                        .put(fields.get(0), List.of())
                        .put(fields.get(2), List.of())
                        .build();
        List<Integer> projections = List.of(0, 2);
        QueryDataPagination pagination = new QueryDataPagination(1);
        EnumeratedSchema enumeratedSchema = new EnumeratedSchema(fields);
        QueryDataResponseSchemaConstructor querySchema = QueryDataResponseSchemaConstructor.deconstruct(traceToken.toString(), enumeratedSchema.getSchema(), projections, baseFieldWithProjections);
        QueryDataResponseParser parser = new QueryDataResponseParser(traceToken, querySchema, VastDebugConfig.DEFAULT, pagination, Optional.empty());

        parser.parse(new ByteArrayInputStream(response));

        List<Page> pages = Streams.stream(parser).toList();
        assertThat(pages.size()).isEqualTo(1);
        assertThat(pagination.isFinished()).isEqualTo(false);

        Page page = pages.get(0);
        assertThat(page.getPositionCount()).isEqualTo(6);
        assertThat(page.getChannelCount()).isEqualTo(2);
        {
            byte[] expected = {0x44, 0x55, 0x66, 0x11, 0x22, 0x33};
            IntStream
                    .range(0, page.getPositionCount())
                    .forEach(i -> assertThat(((ByteArrayBlock) page.getBlock(0)).getByte(i)).isEqualTo(expected[i]));
        }
        {
            long[] expected = {0x1111111111111111L, 0x2222222222222222L, 0x3333333333333333L, 0x4444444444444444L, 0x5555555555555555L, 0x6666666666666666L};
            IntStream
                    .range(0, page.getPositionCount())
                    .forEach(i -> assertThat(((LongArrayBlock) page.getBlock(1)).getLong(i)).isEqualTo(expected[i]));
        }
        LongCount rowsCount = (LongCount) parser.getMetrics().getMetrics().get("totalPositions");
        assertThat(rowsCount.getTotal()).isEqualTo(6L);
    }
}
