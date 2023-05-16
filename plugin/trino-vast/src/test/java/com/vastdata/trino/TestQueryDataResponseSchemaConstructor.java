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
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.ArrayType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.BooleanType.wrapByteArrayAsBooleanBlockWithoutNulls;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestQueryDataResponseSchemaConstructor
{
    private static final String TRACE_TOKEN = "TestToken";

    @Test
    public void testDeconstruct1()
    {
        // flat list: [a, b(x, y(n,m), c] = [a, b, b.x, b.y, b.y.n, b.y.m, c] = [0, 1, 2 ,3, 4, 5, 6]
        // reverse index mapping: {0:0, 1:1, 2:1, 3:1, 4:3, 5:3, 6:6]
        List<Integer> testProjections = List.of(0, 4, 5, 6);
        Field utf = new Field("a", new FieldType(true, ArrowType.Utf8.INSTANCE, null), List.of());
        Field binary = new Field("x", new FieldType(true, ArrowType.Binary.INSTANCE, null), List.of());
        Field int1 = new Field("n", new FieldType(true, new ArrowType.Int(8, true), null), List.of());
        Field int2 = new Field("m", new FieldType(true, new ArrowType.Int(16, true), null), List.of());
        Field nestedStruct = new Field("y", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(int1, int2));
        Field parentStruct = new Field("b", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(binary, nestedStruct));
        Field bool = new Field("c", new FieldType(true, ArrowType.Bool.INSTANCE, null), List.of());
        Schema testSchema = new Schema(List.of(utf, parentStruct, bool));
        LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> baseFieldWithProjections =
                new QueryDataBaseFieldsWithProjectionsMappingBuilder()
                .put(utf, List.of())
                .put(parentStruct, List.of())
                .put(bool, List.of())
                .build();

        QueryDataResponseSchemaConstructor unit = QueryDataResponseSchemaConstructor.deconstruct(TRACE_TOKEN, testSchema, testProjections, baseFieldWithProjections);
        List<Field> projectedFlatFields = unit.getFields();
        List<Field> expectedProjections = List.of(utf,
                new Field("b", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(
                        new Field("y", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(int1)))),
                new Field("b", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(
                        new Field("y", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(int2)))),
                bool);
        assertEquals(projectedFlatFields, expectedProjections);
        HashMap<Object, Object> expectedReverseMapping = new HashMap<>();
        expectedReverseMapping.put(0, 0);
        expectedReverseMapping.put(1, 1);
        expectedReverseMapping.put(2, 1);
        expectedReverseMapping.put(3, 1);
        expectedReverseMapping.put(4, 3);
        expectedReverseMapping.put(5, 3);
        expectedReverseMapping.put(6, 6);
        assertEquals(unit.childToParent, expectedReverseMapping);
        Block booleanBlock = wrapByteArrayAsBooleanBlockWithoutNulls(new byte[1]);
        ShortArrayBlock smallIntBlock = new ShortArrayBlock(1, Optional.of(new boolean[] {false}), new short[] {4});
        Block smallIntRowBlock = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {smallIntBlock})});
        ByteArrayBlock byteIntBlock = new ByteArrayBlock(1, Optional.of(new boolean[] {false}), new byte[] {3});
        Block tinyIntRowBlock = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {byteIntBlock})});
        Slice slice = Slices.utf8Slice("somestring");
        int offset = slice.length();
        VariableWidthBlock varcharBlock = new VariableWidthBlock(1, slice, new int[] {0, offset}, Optional.of(new boolean[] {false}));
        Block[] mockBlocks = new Block[] {varcharBlock, tinyIntRowBlock, smallIntRowBlock, booleanBlock};
        Page constructedPage = unit.construct(mockBlocks, 0);
        assertEquals(constructedPage.getChannelCount(), 3);
    }

    @Test
    public void testDeconstruct2()
    {
        // flat list: [rowcol(x, a, b) = [rowcol, rowcol.x, rowcol.a, rowcol.b] = [0, 1, 2 ,3]
        // reverse index mapping: {0:0, 1:0, 2:0, 3:0]
        List<Integer> testProjections = List.of(1, 2, 3);
        Field int1 = new Field("x", new FieldType(true, new ArrowType.Int(32, true), null), List.of());
        Field bool = new Field("a", new FieldType(true, ArrowType.Bool.INSTANCE, null), List.of());
        Field utf = new Field("b", new FieldType(true, ArrowType.Utf8.INSTANCE, null), List.of());
        Field row = new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(
                int1, bool, utf));
        Schema testSchema = new Schema(List.of(row));
        LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> baseFieldWithProjections =
                new QueryDataBaseFieldsWithProjectionsMappingBuilder()
                        .put(row, List.of())
                        .build();
        QueryDataResponseSchemaConstructor unit = QueryDataResponseSchemaConstructor.deconstruct(TRACE_TOKEN, testSchema, testProjections, baseFieldWithProjections);
        List<Field> projectedFlatFields = unit.getFields();
        List<Field> expectedProjections = List.of(
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(int1)),
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(bool)),
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(utf)));
        assertEquals(projectedFlatFields, expectedProjections);
        HashMap<Object, Object> expectedReverseMapping = new HashMap<>();
        expectedReverseMapping.put(0, 0);
        expectedReverseMapping.put(1, 0);
        expectedReverseMapping.put(2, 0);
        expectedReverseMapping.put(3, 0);
        assertEquals(unit.childToParent, expectedReverseMapping);
        IntArrayBlock intBlock = new IntArrayBlock(1, Optional.of(new boolean[] {false}), new int[] {777});
        Block booleanBlock = wrapByteArrayAsBooleanBlockWithoutNulls(new byte[1]);
        Slice slice = Slices.utf8Slice("somestring");
        int offset = slice.length();
        VariableWidthBlock varcharBlock = new VariableWidthBlock(1, slice, new int[] {0, offset}, Optional.of(new boolean[] {false}));
        Block row1 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[]{false}), new Block[] {intBlock});
        Block row2 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[]{false}), new Block[] {booleanBlock});
        Block row3 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[]{false}), new Block[] {varcharBlock});
        Block[] mockBlocks = new Block[] {row1, row2, row3};
        Page constructedPage = unit.construct(mockBlocks, 0);
        assertEquals(constructedPage.getChannelCount(), 1);
        assertTrue(constructedPage.getBlock(0) instanceof RowBlock);
    }

    @Test
    public void testDeconstruct3()
    {
        // flat list: [x, a, b, l, rowcol(m)] = [x, a, b, l, l.l, rowcol, rowcol.m, rowcol.m.m] = [0, 1, 2, 3, 4, 5, 6, 7]
        // reverse index mapping: {0:0, 1:1, 2:2, 3:3, 4:3, 5:5, 5:6, 7:6]
        List<Integer> testProjections = List.of(0, 1, 2, 4, 7);
        Field int1 = new Field("x", new FieldType(true, new ArrowType.Int(32, true), null), List.of());
        Field bool = new Field("a", new FieldType(true, ArrowType.Bool.INSTANCE, null), List.of());
        Field utf = new Field("b", new FieldType(true, ArrowType.Utf8.INSTANCE, null), List.of());
        Field bigintList = TypeUtils.convertTrinoTypeToArrowField(new ArrayType(BIGINT), "l", true);
        Field tinyintList = TypeUtils.convertTrinoTypeToArrowField(new ArrayType(TINYINT), "m", true);
        Field row = new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(tinyintList));
        Schema testSchema = new Schema(List.of(int1, bool, utf, bigintList, row));
        LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> baseFieldWithProjections =
                new QueryDataBaseFieldsWithProjectionsMappingBuilder()
                        .put(int1, List.of())
                        .put(bool, List.of())
                        .put(utf, List.of())
                        .put(bigintList, List.of())
                        .put(row, List.of())
                        .build();
        QueryDataResponseSchemaConstructor unit = QueryDataResponseSchemaConstructor.deconstruct(TRACE_TOKEN, testSchema, testProjections, baseFieldWithProjections);
        List<Field> expectedProjections = List.of(
                new Field("x", new FieldType(true, new ArrowType.Int(32, true), null), List.of()),
                new Field("a", new FieldType(true, ArrowType.Bool.INSTANCE, null), List.of()),
                new Field("b", new FieldType(true, ArrowType.Utf8.INSTANCE, null), List.of()),
                bigintList,
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(tinyintList)));
        assertEquals(unit.getFields(), expectedProjections);
        HashMap<Object, Object> expectedReverseMapping = new HashMap<>();
        expectedReverseMapping.put(0, 0);
        expectedReverseMapping.put(1, 1);
        expectedReverseMapping.put(2, 2);
        expectedReverseMapping.put(3, 3);
        expectedReverseMapping.put(4, 3);
        expectedReverseMapping.put(5, 5);
        expectedReverseMapping.put(6, 5);
        expectedReverseMapping.put(7, 6);
        assertEquals(unit.childToParent, expectedReverseMapping);
        IntArrayBlock intBlock = new IntArrayBlock(1, Optional.of(new boolean[] {false}), new int[] {777});
        Block booleanBlock = wrapByteArrayAsBooleanBlockWithoutNulls(new byte[1]);
        Slice slice = Slices.utf8Slice("somestring");
        int offset = slice.length();
        VariableWidthBlock varcharBlock = new VariableWidthBlock(1, slice, new int[] {0, offset}, Optional.of(new boolean[] {false}));
        LongArrayBlock longBlock = new LongArrayBlock(1, Optional.of(new boolean[] {false}), new long[] {1});
        Block arrayBlock = ArrayBlock.fromElementBlock(1, Optional.of(new boolean[] {false}), new int[] {0, 1}, longBlock);
        Block[] mockBlocks = new Block[] {intBlock, booleanBlock, varcharBlock, arrayBlock,
                RowBlock.fromFieldBlocks(1, Optional.of(new boolean[]{false}), new Block[] {arrayBlock})};
        Page constructedPage = unit.construct(mockBlocks, 0);
        assertEquals(constructedPage.getChannelCount(), 5);
        assertTrue(constructedPage.getBlock(3) instanceof ArrayBlock);
        assertTrue(constructedPage.getBlock(4) instanceof RowBlock);
    }

    @Test
    public void testDeconstructMultiLevelNesting()
    {
        // flat list: [rowcol(l1, a, subrow(b, l2)] = [rowcol, rowcol.l1, rowcol.l1.l1, rowcol.a, rowcol.subrow, rowcol.subrow.b, rowcol.subrow.l2, rowcol.subrow.l2.l2]
        // = [0, 1, 2, 3, 4, 5, 6, 7]
        // reverse index mapping: {0:0, 1:0, 2:1, 3:0, 4:0, 5:4, 6:4, 7:6]
        List<Integer> testProjections = List.of(2, 3, 5, 7);
        Field l1 = TypeUtils.convertTrinoTypeToArrowField(new ArrayType(BIGINT), "l1", true);
        Field l2 = TypeUtils.convertTrinoTypeToArrowField(new ArrayType(TINYINT), "l2", true);
        Field a = new Field("a", new FieldType(true, ArrowType.Bool.INSTANCE, null), List.of());
        Field b = new Field("b", new FieldType(true, ArrowType.Bool.INSTANCE, null), List.of());
        Field subrow = new Field("subrow", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(b, l2));
        Field row = new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(l1, a, subrow));
        Schema testSchema = new Schema(List.of(row));
        LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> baseFieldWithProjections =
                new QueryDataBaseFieldsWithProjectionsMappingBuilder()
                        .put(row, List.of())
                        .build();
        QueryDataResponseSchemaConstructor unit = QueryDataResponseSchemaConstructor.deconstruct(TRACE_TOKEN, testSchema, testProjections, baseFieldWithProjections);
        List<Field> expectedProjections = List.of(
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(l1)),
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(a)),
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(
                        new Field("subrow", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(b)))),
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(
                        new Field("subrow", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(l2)))));
        assertEquals(unit.getFields(), expectedProjections);
        HashMap<Object, Object> expectedReverseMapping = new HashMap<>();
        expectedReverseMapping.put(0, 0);
        expectedReverseMapping.put(1, 0);
        expectedReverseMapping.put(2, 1);
        expectedReverseMapping.put(3, 0);
        expectedReverseMapping.put(4, 0);
        expectedReverseMapping.put(5, 4);
        expectedReverseMapping.put(6, 4);
        expectedReverseMapping.put(7, 6);
        assertEquals(unit.childToParent, expectedReverseMapping);
        LongArrayBlock longBlock = new LongArrayBlock(1, Optional.of(new boolean[] {false}), new long[] {1});
        Block bigintArrayBlock = ArrayBlock.fromElementBlock(1, Optional.of(new boolean[] {false}), new int[] {0, 1}, longBlock);
        Block projection2 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {bigintArrayBlock});
        Block boolBlock = new ByteArrayBlock(1, Optional.of(new boolean[] {false}), new byte[] {1});
        Block projection3 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {boolBlock});
        Block tinyintArrayBlock = ArrayBlock.fromElementBlock(1, Optional.of(new boolean[] {false}), new int[] {0, 1}, boolBlock);
        Block subRowArrayBlock = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[]{false}), new Block[] {tinyintArrayBlock});
        Block subRowBoolBlock = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[]{false}), new Block[] {boolBlock});
        Block projection5 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {subRowBoolBlock});
        Block projection7 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {subRowArrayBlock});
        Block[] testBlocks = new Block[] {projection2, projection3, projection5, projection7};
        Page constructedPage = unit.construct(testBlocks, 1);
        assertEquals(constructedPage.getChannelCount(), 1);
        assertTrue(constructedPage.getBlock(0) instanceof RowBlock);
    }

    @Test
    public void testDeconstructMultiLevelNesting2()
    {
        // flat list: [arraycol, rowcol(x(a, b), y, z)] = [arraycol, arraycol.arraycol, rowcol, rowcol.x, rowcol.x.a, rowcol.x.b, rowcol.x.b.b, rowcol.y, rowcol.z, rowcol.z.z]
        // = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        // reverse index mapping: {0:0, 1:0, 2:2, 2:3, 4:3, 5:3, 6:5, 7:2, 8:2, 9:8]
        List<Integer> testProjections = List.of(1, 4, 6, 7, 9);
        Field arrayCol = TypeUtils.convertTrinoTypeToArrowField(new ArrayType(VARCHAR), "arraycol", true);
        Field b = TypeUtils.convertTrinoTypeToArrowField(new ArrayType(VARCHAR), "b", true);
        Field a = TypeUtils.convertTrinoTypeToArrowField(BOOLEAN, "a", true);
        Field x = new Field("x", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(a, b));
        Field y = TypeUtils.convertTrinoTypeToArrowField(BIGINT, "y", true);
        Field z = TypeUtils.convertTrinoTypeToArrowField(new ArrayType(TINYINT), "z", true);
        Field rowCol = new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(x, y, z));
        Schema testSchema = new Schema(List.of(arrayCol, rowCol));
        LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> baseFieldWithProjections =
                new QueryDataBaseFieldsWithProjectionsMappingBuilder()
                        .put(arrayCol, List.of())
                        .put(rowCol, List.of())
                        .build();
        QueryDataResponseSchemaConstructor unit = QueryDataResponseSchemaConstructor.deconstruct(TRACE_TOKEN, testSchema, testProjections, baseFieldWithProjections);
        List<Field> expectedProjections = List.of(
                arrayCol,
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(
                        new Field("x", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(a)))),
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(
                        new Field("x", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(b)))),
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(y)),
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(z)));
        assertEquals(unit.getFields(), expectedProjections);
        HashMap<Object, Object> expectedReverseMapping = new HashMap<>();
        expectedReverseMapping.put(0, 0);
        expectedReverseMapping.put(1, 0);
        expectedReverseMapping.put(2, 2);
        expectedReverseMapping.put(3, 2);
        expectedReverseMapping.put(4, 3);
        expectedReverseMapping.put(5, 3);
        expectedReverseMapping.put(6, 5);
        expectedReverseMapping.put(7, 2);
        expectedReverseMapping.put(8, 2);
        expectedReverseMapping.put(9, 8);
        assertEquals(unit.childToParent, expectedReverseMapping);
        Slice slice = Slices.utf8Slice("somestring");
        int offset = slice.length();
        VariableWidthBlock varcharBlock = new VariableWidthBlock(1, slice, new int[] {0, offset}, Optional.of(new boolean[] {false}));
        Block arraycolBlock = ArrayBlock.fromElementBlock(1, Optional.of(new boolean[] {false}), new int[] {0, 1}, varcharBlock);
        ShortArrayBlock shortBlock = new ShortArrayBlock(1, Optional.of(new boolean[] {false}), new short[] {1});
        Block shortArrayBlock = ArrayBlock.fromElementBlock(1, Optional.of(new boolean[] {false}), new int[] {0, 1}, shortBlock);
        ByteArrayBlock byteBlock = new ByteArrayBlock(1, Optional.of(new boolean[] {false}), new byte[] {1});
        Block byteArrayBlock = ArrayBlock.fromElementBlock(1, Optional.of(new boolean[] {false}), new int[] {0, 1}, byteBlock);
        LongArrayBlock longBlock = new LongArrayBlock(1, Optional.of(new boolean[] {false}), new long[] {1});
        Block nestedRowSubBlock1 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {byteBlock});
        Block nestedRow1 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {nestedRowSubBlock1});
        Block nestedRowSubBlock2 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {shortArrayBlock});
        Block nestedRow2 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {nestedRowSubBlock2});
        Block nestedrow3 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {longBlock});
        Block nestedrow4 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {byteArrayBlock});
        Page constructedPage = unit.construct(new Block[] {arraycolBlock, nestedRow1, nestedRow2, nestedrow3, nestedrow4}, 1);
        assertEquals(constructedPage.getChannelCount(), 2);
        assertTrue(constructedPage.getBlock(0) instanceof ArrayBlock);
        Block rowBlock = constructedPage.getBlock(1);
        assertTrue(rowBlock instanceof RowBlock);
        List<Block> children = rowBlock.getChildren();
        assertEquals(children.size(), 3);
        Block nestedRow = children.get(0);
        assertTrue(nestedRow instanceof RowBlock);
        List<Block> grandChildren = nestedRow.getChildren();
        assertEquals(grandChildren.size(), 2);
        Block boolBlock = grandChildren.get(0);
        Block arrayBlock = grandChildren.get(1);
        assertTrue(boolBlock instanceof ByteArrayBlock);
        assertTrue(arrayBlock instanceof ArrayBlock);
        Block nestedBigint = children.get(1);
        assertTrue(nestedBigint instanceof LongArrayBlock);
        Block nestedArray = children.get(2);
        assertTrue(nestedArray instanceof ArrayBlock);
    }

    @Test
    public void testDeconstructMultiLevelNesting3()
    {
        // flat list: [rowcol(x(a, b)] = [rowcol, rowcol.x, rowcol.x.a, rowcol.x.b, rowcol.x.b.b]
        // = [0, 1, 2, 3, 4]
        // reverse index mapping: {0:0, 1:0, 2:1, 3:1, 4:3]
        List<Integer> testProjections = List.of(2, 4);
        Field a = TypeUtils.convertTrinoTypeToArrowField(BOOLEAN, "a", true);
        Field b = TypeUtils.convertTrinoTypeToArrowField(new ArrayType(BIGINT), "b", true);
        Field x = new Field("x", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(a, b));
        Field rowCol = new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(x));
        Schema testSchema = new Schema(List.of(rowCol));
        LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> baseFieldWithProjections =
                new QueryDataBaseFieldsWithProjectionsMappingBuilder()
                        .put(rowCol, List.of())
                        .build();
        QueryDataResponseSchemaConstructor unit = QueryDataResponseSchemaConstructor.deconstruct(TRACE_TOKEN, testSchema, testProjections, baseFieldWithProjections);
        List<Field> expectedProjections = List.of(
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(
                        new Field("x", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(a)))),
                new Field("rowcol", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(
                        new Field("x", new FieldType(true, ArrowType.Struct.INSTANCE, null), List.of(b)))));
        List<Field> fields = unit.getFields();
        assertEquals(fields, expectedProjections);
        HashMap<Object, Object> expectedReverseMapping = new HashMap<>();
        expectedReverseMapping.put(0, 0);
        expectedReverseMapping.put(1, 0);
        expectedReverseMapping.put(2, 1);
        expectedReverseMapping.put(3, 1);
        expectedReverseMapping.put(4, 3);
        assertEquals(unit.childToParent, expectedReverseMapping);

        ByteArrayBlock byteBlock = new ByteArrayBlock(1, Optional.of(new boolean[] {false}), new byte[] {1});
        Block nestedRowSubBlock1 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {byteBlock});
        Block nestedRow1 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {nestedRowSubBlock1});

        LongArrayBlock longBlock = new LongArrayBlock(1, Optional.of(new boolean[] {false}), new long[] {1});
        Block longArrayBlock = ArrayBlock.fromElementBlock(1, Optional.of(new boolean[] {false}), new int[] {0, 1}, longBlock);
        Block nestedRowSubBlock2 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {longArrayBlock});
        Block nestedRow2 = RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {false}), new Block[] {nestedRowSubBlock2});

        Page constructedPage = unit.construct(new Block[] {nestedRow1, nestedRow2}, 1);
        assertEquals(constructedPage.getChannelCount(), 1);
        Block rowBlock = constructedPage.getBlock(0);
        assertTrue(rowBlock instanceof RowBlock);
        List<Block> children = rowBlock.getChildren();
        assertEquals(children.size(), 1);
        Block childRowBlock = children.get(0);
        List<Block> grandChildren = childRowBlock.getChildren();
        assertEquals(grandChildren.size(), 2);
        Block boolBlock = grandChildren.get(0);
        Block arrayBlock = grandChildren.get(1);
        assertTrue(boolBlock instanceof ByteArrayBlock);
        assertTrue(arrayBlock instanceof ArrayBlock);
    }

    @Test
    public void testProjectionPathsComparator()
    {
        assertEquals(QueryDataResponseSchemaConstructor.PROJECTION_PATH_COMPARATOR.compare(List.of(), List.of()), 0);
        assertTrue(QueryDataResponseSchemaConstructor.PROJECTION_PATH_COMPARATOR.compare(List.of(5), List.of(6)) < 0);
        assertTrue(QueryDataResponseSchemaConstructor.PROJECTION_PATH_COMPARATOR.compare(List.of(6), List.of(5)) > 0);
        assertTrue(QueryDataResponseSchemaConstructor.PROJECTION_PATH_COMPARATOR.compare(List.of(0, 1, 2, 4), List.of(0, 1, 4, 2)) < 0);
        assertEquals(QueryDataResponseSchemaConstructor.PROJECTION_PATH_COMPARATOR.compare(List.of(0, 1, 2), List.of(0, 1, 2)), 0);
        assertTrue(QueryDataResponseSchemaConstructor.PROJECTION_PATH_COMPARATOR.compare(List.of(1), List.of(2, 4)) < 0);
        assertTrue(QueryDataResponseSchemaConstructor.PROJECTION_PATH_COMPARATOR.compare(List.of(2), List.of(1, 4)) < 0);
    }
}
