/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestVastRecordBatchBuilder
{
    private final String traceStr = "TestToken";

    @Test
    public void testNestedMap()
    {
        // unlike other types, Map can't be tests in a round-trip way, because trino->vast encoding is different than vast->trino. See TestSerDe
        MapType mapType = new MapType(INTEGER, new ArrayType(BooleanType.BOOLEAN), new TypeOperators());
        Type type = RowType.rowType(
                RowType.field("mapcol", mapType));
        Field field = TypeUtils.convertTrinoTypeToArrowField(type, "rowcol", true);
        VastRecordBatchBuilder builder = new VastRecordBatchBuilder(new Schema(List.of(field)));
        ByteArrayBlock booleanBlock = new ByteArrayBlock(1, Optional.of(new boolean[] {true}), new byte[] {0});
        IntArrayBlock intBlock = new IntArrayBlock(1, Optional.of(new boolean[] {true}), new int[] {0});
        Block arrayBlock = ArrayBlock.fromElementBlock(1, Optional.of(new boolean[] {true}), new int[] {0, 0}, booleanBlock);
        MapBlock mapBlock = MapBlock.fromKeyValueBlock(Optional.of(new boolean[] {true}), new int[] {0, 0}, 1, intBlock, arrayBlock, mapType);
        RowBlock.fromFieldBlocks(1, new Block[] {mapBlock});
        Block rowBlock = RowBlock.fromNotNullSuppressedFieldBlocks(1, Optional.of(new boolean[] {true}), new Block[] {mapBlock});

        try (VectorSchemaRoot root = builder.build(new Page(rowBlock))) {
            FieldVector vector = root.getVector(0);
            assertEquals(vector.getValueCount(), 1);
            List<FieldVector> children = vector.getChildrenFromFields();
            assertEquals(children.getFirst().toString(), "[null]");
        }
        Field keys = Field.nullable("keys", new ArrowType.Int(32, true));
        Field vals = Field.nullable("vals", ArrowType.Bool.INSTANCE);
        Field valsArray = new Field("values", FieldType.nullable(ArrowType.List.INSTANCE), List.of(vals));
        List<Field> keyStructField = List.of(new Field("mapcolkeysstruct", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(keys)));
        List<Field> valStructField = List.of(new Field("mapcolvalsstruct", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(valsArray)));
        Field mapcol1 = new Field("mapcol", FieldType.nullable(ArrowType.List.INSTANCE), keyStructField);
        Field mapcol2 = new Field("mapcol", FieldType.nullable(ArrowType.List.INSTANCE), valStructField);
        Field rowcol1 = new Field("rowcol", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(mapcol1));
        Field rowcol2 = new Field("rowcol", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(mapcol2));

        // simulation of vast map representation for query data
        Block structOfKeys = RowBlock.fromNotNullSuppressedFieldBlocks(1, Optional.of(new boolean[] {true}), new Block[] {intBlock});
        Block structOfValues = RowBlock.fromNotNullSuppressedFieldBlocks(1, Optional.of(new boolean[] {true}), new Block[] {arrayBlock});
        Block block1 = ArrayBlock.fromElementBlock(1, Optional.of(new boolean[] {true}), new int[] {0, 0}, structOfKeys);
        Block block2 = ArrayBlock.fromElementBlock(1, Optional.of(new boolean[] {true}), new int[] {0, 0}, structOfValues);

        Block rowBlock1 = RowBlock.fromNotNullSuppressedFieldBlocks(1, Optional.of(new boolean[] {true}), new Block[] {block1});
        Block rowBlock2 = RowBlock.fromNotNullSuppressedFieldBlocks(1, Optional.of(new boolean[] {true}), new Block[] {block2});

        Schema schema1 = new Schema(List.of(rowcol1));
        VastRecordBatchBuilder builder1 = new VastRecordBatchBuilder(schema1);
        Schema schema2 = new Schema(List.of(rowcol2));
        VastRecordBatchBuilder builder2 = new VastRecordBatchBuilder(schema2);
        try (VectorSchemaRoot root1 = builder1.build(new Page(rowBlock1));
                VectorSchemaRoot root2 = builder2.build(new Page(rowBlock2))) {
            assertEquals(root1.getVector(0).toString(), "[null]");
            assertEquals(root2.getVector(0).toString(), "[null]");
            Block actual1 = new VastPageBuilder("trcestr", schema1).add(root1).build().getBlock(0);
            Block actual2 = new VastPageBuilder("trcestr", schema2).add(root2).build().getBlock(0);
            assertBlockEquals(type, actual1, rowBlock1);
            assertBlockEquals(type, actual2, rowBlock2);
        }
    }

    @Test
    public void testRow()
    {
        Type type = RowType.rowType(
                RowType.field("bigint", BigintType.BIGINT),
                RowType.field("bool", BooleanType.BOOLEAN),
                RowType.field("vchar", VarcharType.VARCHAR));
        Field field = TypeUtils.convertTrinoTypeToArrowField(type, "row", true);
        VastRecordBatchBuilder builder = new VastRecordBatchBuilder(new Schema(List.of(field)));
        LongArrayBlock bigintBlock = new LongArrayBlock(5, Optional.of(new boolean[] {true, false, true, false, true}), new long[] {5, 6, 7, 8, 9});
        ByteArrayBlock booleanBlock = new ByteArrayBlock(5, Optional.of(new boolean[] {true, false, false, true, true}), new byte[] {0, 1, 0, 1, 0});
        Slice slice = Slices.utf8Slice("somestring");
        int offset = slice.length();
        VariableWidthBlock varcharBlock = new VariableWidthBlock(5, slice, new int[] {0, 0, 0, 0, offset, offset}, Optional.of(new boolean[] {true, true, true, false, true}));
        Block rowBlock = RowBlock.fromNotNullSuppressedFieldBlocks(5, Optional.of(new boolean[] {true, false, false, false, true}), new Block[] {bigintBlock, booleanBlock, varcharBlock});
        try (VectorSchemaRoot root = builder.build(new Page(rowBlock))) {
            FieldVector vector = root.getVector(0);
            assertEquals(vector.getValueCount(), 5);
            List<FieldVector> children = vector.getChildrenFromFields();
            assertEquals(children.get(0).toString(), "[null, 6, null, 8, null]");
            assertEquals(children.get(1).toString(), "[null, true, false, null, null]");
            assertEquals(children.get(2).toString(), "[null, null, null, somestring, null]");
            assertEquals(vector.toString(), "[null, {\"bigint\":6,\"bool\":true}, {\"bool\":false}, {\"bigint\":8,\"vchar\":\"somestring\"}, null]");
            Block actual = new VastPageBuilder(traceStr, new Schema(List.of(field))).add(root).build().getBlock(0);
            assertBlockEquals(type, actual, rowBlock);
        }
    }

    @Test
    public void testRowRowRowBigint()
    {
        RowType.Field bigintField = RowType.field("bigint", BIGINT);
        RowType row3Type = RowType.rowType(bigintField);
        RowType.Field row3Field = RowType.field("row3", row3Type);
        RowType row2Type = RowType.rowType(row3Field);
        RowType.Field row2Field = RowType.field("row2", row2Type);
        RowType row1Type = RowType.rowType(row2Field);
        Field row1ArrowField = TypeUtils.convertTrinoTypeToArrowField(row1Type, "row1", true);
        VastRecordBatchBuilder builder = new VastRecordBatchBuilder(new Schema(List.of(row1ArrowField)));
        LongArrayBlock bigintBlock = new LongArrayBlock(2, Optional.of(new boolean[] {true, true}), new long[] {Long.MAX_VALUE, Long.MAX_VALUE});

        Block row3BlockNullFirst = RowBlock.fromNotNullSuppressedFieldBlocks(2, Optional.of(new boolean[] {true, false}), new Block[] {bigintBlock});
        Block row2BlockNullFirst = RowBlock.fromNotNullSuppressedFieldBlocks(2, Optional.of(new boolean[] {true, false}), new Block[] {row3BlockNullFirst});
        Block row1BlockNullFirst = RowBlock.fromNotNullSuppressedFieldBlocks(2, Optional.of(new boolean[] {true, false}), new Block[] {row2BlockNullFirst});
        verifyBuildRowRowRowBigintPage(row1Type, row1ArrowField, builder, row1BlockNullFirst,
                "[null, {\"row2\":{\"row3\":{}}}]", new Page(2, row1BlockNullFirst));

        Block row3BlockNullSecond = RowBlock.fromNotNullSuppressedFieldBlocks(2, Optional.of(new boolean[] {false, true}), new Block[] {bigintBlock});
        Block row2BlockNullSecond = RowBlock.fromNotNullSuppressedFieldBlocks(2, Optional.of(new boolean[] {false, true}), new Block[] {row3BlockNullSecond});
        Block row1BlockNullSecond = RowBlock.fromNotNullSuppressedFieldBlocks(2, Optional.of(new boolean[] {false, true}), new Block[] {row2BlockNullSecond});
        verifyBuildRowRowRowBigintPage(row1Type, row1ArrowField, builder, row1BlockNullSecond,
                "[{\"row2\":{\"row3\":{}}}, null]", new Page(2, row1BlockNullSecond));
    }

    private void verifyBuildRowRowRowBigintPage(RowType row1Type, Field row1ArrowField, VastRecordBatchBuilder builder, Block row1Block,
            String expectedVectorAsString, Page expectedPage)
    {
        try (VectorSchemaRoot root = builder.build(new Page(row1Block))) {
            FieldVector row1Vector = root.getVector(0);
            assertEquals(row1Vector.toString(), expectedVectorAsString);
            assertEquals(row1Vector.getClass(), StructVector.class);
            assertEquals(row1Vector.getValueCount(), 2);
            assertEquals(row1Vector.getChildrenFromFields().size(), 1);
            FieldVector row2Vector = row1Vector.getChildrenFromFields().get(0);
            assertEquals(row2Vector.getClass(), StructVector.class);
            assertEquals(row2Vector.getValueCount(), 2);
            assertEquals(row2Vector.getChildrenFromFields().size(), 1);
            FieldVector row3Vector = row2Vector.getChildrenFromFields().get(0);
            assertEquals(row3Vector.getClass(), StructVector.class);
            assertEquals(row3Vector.getValueCount(), 2);
            assertEquals(row3Vector.getChildrenFromFields().size(), 1);
            FieldVector valueVectors = row3Vector.getChildrenFromFields().get(0);
            assertEquals(valueVectors.getClass(), BigIntVector.class);
            assertEquals(valueVectors.getValueCount(), 2);
            assertEquals(valueVectors.getChildrenFromFields().size(), 0);
            Block actual = new VastPageBuilder(traceStr, new Schema(List.of(row1ArrowField))).add(root).build().getBlock(0);
            assertBlockEquals(row1Type, actual, row1Block);
            LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> baseFieldWithProjections =
                    new QueryDataBaseFieldsWithProjectionsMappingBuilder()
                            .put(row1ArrowField, List.of())
                            .build();
            QueryDataResponseSchemaConstructor deconstruct = QueryDataResponseSchemaConstructor.deconstruct(traceStr, new Schema(List.of(row1ArrowField)), List.of(3), baseFieldWithProjections);
            Page construct = deconstruct.construct(new Block[] {actual}, 2);
            assertEquals(construct.getChannelCount(), expectedPage.getChannelCount());
            assertEquals(construct.getPositionCount(), expectedPage.getPositionCount());
            assertEquals(construct.getBlock(0).getPositionCount(), expectedPage.getBlock(0).getPositionCount());
        }
    }

    @Test
    public void testScalar()
    {
        Field field = TypeUtils.convertTrinoTypeToArrowField(BIGINT, "l", true);
        VastRecordBatchBuilder builder = new VastRecordBatchBuilder(new Schema(List.of(field)));

        LongArrayBlock longBlock = new LongArrayBlock(6, Optional.empty(), new long[] {1, 2, 3, 4, 5, 6});
        try (VectorSchemaRoot root = builder.build(new Page(longBlock))) {
            assertThat(root.getVector(0).toString()).isEqualTo("[1, 2, 3, 4, 5, 6]");
            Block actual = new VastPageBuilder(traceStr, new Schema(List.of(field))).add(root).build().getBlock(0);
            assertBlockEquals(BIGINT, actual, longBlock);
        }
        builder.checkLeaks();
    }

    @Test
    public void testNestedList()
    {
        Type type = new ArrayType(INTEGER);
        Field field = TypeUtils.convertTrinoTypeToArrowField(type, "l", true);
        VastRecordBatchBuilder builder = new VastRecordBatchBuilder(new Schema(List.of(field)));

        IntArrayBlock longBlock = new IntArrayBlock(6, Optional.empty(), new int[] {1, 2, 3, 4, 5, 6});
        Block arrayBlock = ArrayBlock.fromElementBlock(3, Optional.empty(), new int[] {0, 1, 4, 6}, longBlock);
        try (VectorSchemaRoot root = builder.build(new Page(arrayBlock))) {
            assertThat(root.getVector(0).toString()).isEqualTo("[[1], [2,3,4], [5,6]]");

            Block actual = new VastPageBuilder(traceStr, new Schema(List.of(field))).add(root).build().getBlock(0);
            assertBlockEquals(type, actual, arrayBlock);
        }
        builder.checkLeaks();
    }

    @Test
    public void testEmptyList()
    {
        Type type = new ArrayType(BIGINT);
        Field field = TypeUtils.convertTrinoTypeToArrowField(type, "l", true);
        VastRecordBatchBuilder builder = new VastRecordBatchBuilder(new Schema(List.of(field)));

        LongArrayBlock longBlock = new LongArrayBlock(0, Optional.empty(), new long[] {});
        Block arrayBlock = ArrayBlock.fromElementBlock(1, Optional.empty(), new int[] {0, 0}, longBlock);
        try (VectorSchemaRoot root = builder.build(new Page(arrayBlock))) {
            assertThat(root.getVector(0).toString()).isEqualTo("[[]]");

            Block actual = new VastPageBuilder(traceStr, new Schema(List.of(field))).add(root).build().getBlock(0);
            assertBlockEquals(type, actual, arrayBlock);
        }
        builder.checkLeaks();
    }

    @Test
    public void testNestedListMultiple()
    {
        Type type = new ArrayType(BIGINT);
        Field field = TypeUtils.convertTrinoTypeToArrowField(type, "l", true);
        VastRecordBatchBuilder builder = new VastRecordBatchBuilder(new Schema(List.of(field)));

        VastPageBuilder pageBuilder = new VastPageBuilder(traceStr, new Schema(List.of(field)));

        Block arrayBlock1 = ArrayBlock.fromElementBlock(
                3,
                Optional.empty(),
                new int[] {0, 1, 4, 6},
                new LongArrayBlock(6, Optional.empty(), new long[] {1, 2, 3, 4, 5, 6}));

        Block arrayBlock2 = ArrayBlock.fromElementBlock(
                2,
                Optional.empty(),
                new int[] {0, 2, 3},
                new LongArrayBlock(3, Optional.empty(), new long[] {7, 8, 9}));

        try (
                VectorSchemaRoot root1 = builder.build(new Page(arrayBlock1));
                VectorSchemaRoot root2 = builder.build(new Page(arrayBlock2))) {
            assertThat(root1.getVector(0).toString()).isEqualTo("[[1], [2,3,4], [5,6]]");
            assertThat(root2.getVector(0).toString()).isEqualTo("[[7,8], [9]]");

            pageBuilder.add(root1);
            pageBuilder.add(root2);

            Block expected = ArrayBlock.fromElementBlock(
                    5,
                    Optional.empty(),
                    new int[] {0, 1, 4, 6, 8, 9},
                    new LongArrayBlock(9, Optional.empty(), new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9}));
            assertBlockEquals(type, pageBuilder.build().getBlock(0), expected);
        }

        builder.checkLeaks();
    }

    @Test
    public void testNestedListWithNulls()
    {
        Type type = new ArrayType(BIGINT);
        Field field = TypeUtils.convertTrinoTypeToArrowField(type, "l", true);
        VastRecordBatchBuilder builder = new VastRecordBatchBuilder(new Schema(List.of(field)));

        LongArrayBlock longBlock = new LongArrayBlock(
                7,
                Optional.of(new boolean[] {false, false, false, false, false, true, false}),
                new long[] {1, 2, 3, 4, 5, 6, 7});
        Block arrayBlock = ArrayBlock.fromElementBlock(
                6,
                Optional.of(new boolean[] {false, true, false, false, false, false}),
                new int[] {0, 1, 1, 4, 6, 6, 7}, longBlock);
        try (VectorSchemaRoot root = builder.build(new Page(arrayBlock))) {
            assertThat(root.getVector(0).toString()).isEqualTo("[[1], null, [2,3,4], [5,null], [], [7]]");
            Block actual = new VastPageBuilder(traceStr, new Schema(List.of(field))).add(root).build().getBlock(0);
            assertBlockEquals(type, actual, arrayBlock);
        }
        builder.checkLeaks();
    }

    @Test
    public void testDoubleNestedList()
    {
        Type type = new ArrayType(new ArrayType(BIGINT));
        Field field = TypeUtils.convertTrinoTypeToArrowField(type, "l", true);
        VastRecordBatchBuilder builder = new VastRecordBatchBuilder(new Schema(List.of(field)));

        LongArrayBlock longBlock = new LongArrayBlock(6, Optional.empty(), new long[] {1, 2, 3, 4, 5, 6});
        Block arrayBlock1 = ArrayBlock.fromElementBlock(3, Optional.empty(), new int[] {0, 1, 4, 6}, longBlock);
        Block arrayBlock2 = ArrayBlock.fromElementBlock(3, Optional.empty(), new int[] {0, 0, 2, 3}, arrayBlock1);
        try (VectorSchemaRoot root = builder.build(new Page(arrayBlock2))) {
            assertThat(root.getVector(0).toString()).isEqualTo("[[], [[1],[2,3,4]], [[5,6]]]");
            Block actual = new VastPageBuilder(traceStr, new Schema(List.of(field))).add(root).build().getBlock(0);
            assertBlockEquals(type, actual, arrayBlock2);
        }
        builder.checkLeaks();
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        assertEquals(actual.getPositionCount(), expected.getPositionCount());
        List<Object> collectedActualObjects = IntStream.range(0, actual.getPositionCount()).mapToObj(position -> type.getObjectValue(null, actual, position)).collect(Collectors.toList());
        List<Object> collectedExpectedObjects = IntStream.range(0, expected.getPositionCount()).mapToObj(position -> type.getObjectValue(null, expected, position)).collect(Collectors.toList());
        assertEquals(collectedActualObjects, collectedExpectedObjects);
    }
}
