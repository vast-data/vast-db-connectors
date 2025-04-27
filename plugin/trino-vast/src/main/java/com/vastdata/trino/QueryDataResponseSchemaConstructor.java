/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.vastdata.trino.TypeUtils.TYPE_OPERATORS;
import static com.vastdata.trino.TypeUtils.convertArrowFieldToTrinoType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class QueryDataResponseSchemaConstructor
{
    private static final Logger LOG = Logger.get(QueryDataResponseSchemaConstructor.class);
    @VisibleForTesting
    protected static final Comparator<List<Integer>> PROJECTION_PATH_COMPARATOR = (l1, l2) -> {
        int sizeDiff = l1.size() - l2.size();
        if (sizeDiff == 0) {
            for (int j = 0; j < l1.size(); j++) {
                if (!Objects.equals(l1.get(j), l2.get(j))) {
                    return l1.get(j) - l2.get(j);
                }
            }
            return 0;
        }
        else {
            return sizeDiff;
        }
    };
    protected final LinkedHashMap<Integer, Field> orderedProjectedFieldsMap;
    protected final LinkedHashSet<Field> expectedParentFields;
    protected final Map<Integer, Integer> childToParent;
    protected final Map<Integer, Set<Integer>> parentToChildren;
    protected final List<Integer> projections;
    private final Map<Integer, Integer> reverseProjection;
    private final LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>>  baseFieldWithProjections;
    private final List<Field> flatSchema;
    private final String traceStr;

    /**
     * @param traceStr trace token for logs
     * @param orderedProjectedFieldsMap schema fields (leaves) to project ordered by projections order
     * @param expectedParentFields aggregated expected schema ordered by projections order
     * @param projections projections array - no duplicates, bottom level indices only
     * @param childToParent mapping of child field to its parent
     * @param parentToChildren mapping of parent field to its child fields
     * @param baseFieldWithProjections mapping of expected projected base fields to pairs of projection path and query column index
     */
    private QueryDataResponseSchemaConstructor(final String traceStr, List<Field> flatSchema,
            LinkedHashMap<Integer, Field> orderedProjectedFieldsMap, LinkedHashSet<Field> expectedParentFields,
            List<Integer> projections, HashMap<Integer, Integer> childToParent,
            Map<Integer, Set<Integer>> parentToChildren, LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>>  baseFieldWithProjections)
    {
        this.traceStr = traceStr;
        this.flatSchema = flatSchema;
        this.orderedProjectedFieldsMap = orderedProjectedFieldsMap;
        this.expectedParentFields = expectedParentFields;
        this.childToParent = childToParent;
        this.parentToChildren = parentToChildren;
        this.projections = projections;
        this.baseFieldWithProjections = baseFieldWithProjections;
        reverseProjection = new HashMap<>(projections.size());
        LOG.debug("QueryData(%s) Constructing page: projections=%s", traceStr, projections);
        for (int i = 0; i < projections.size(); i++) {
            verify(reverseProjection.put(projections.get(i), i) == null, "QueryData(%s) Duplicate projection is not allowed: %s", traceStr, projections.get(i));
        }
        LOG.debug("QueryData(%s) Constructing page: reverseProjection=%s", traceStr, reverseProjection);
        LOG.debug("QueryData(%s) Constructing page: flatSchema=%s", traceStr, flatSchema);
        LOG.debug("QueryData(%s) Constructing page: expectedParentFields=%s", traceStr, expectedParentFields);
        LOG.debug("QueryData(%s) Constructing page: baseFieldWithProjections=%s", traceStr, baseFieldWithProjections);
        LOG.debug("QueryData(%s) Constructing page: orderedProjectedFieldsMap=%s", traceStr, orderedProjectedFieldsMap);
        LOG.debug("QueryData(%s) Constructing page: childToParent=%s, parentToChildren=%s", traceStr, childToParent, parentToChildren);
    }

    public List<Field> getFields()
    {
        return new LinkedList<>(orderedProjectedFieldsMap.values());
    }

    private boolean isParent(int projectionIndex)
    {
        return parentToChildren.containsKey(projectionIndex);
    }

    private Block buildParentBlockFromChildren(Block[] blocks, Set<Integer> childIndexes)
    {
        LOG.debug("QueryData(%s) Building parent block from children: %s", traceStr, childIndexes);
        verify(childIndexes != null && !childIndexes.isEmpty(),
                "QueryData(%s) Children list must not be %s", traceStr, childIndexes == null ? "null" : "empty");

        if (childIndexes.size() > 1000) {
            LOG.debug("QueryData(%s) Sorting nested projections of size %s might be time consuming", traceStr, childIndexes.size());
        }
        List<Integer> sortedChildrenIndexList = childIndexes.stream().sorted().toList();
        int anyChildIndex = sortedChildrenIndexList.getFirst();
        int nestingLevel = getNestingLevel(anyChildIndex);

        Field parentField = getParentSchemaType(anyChildIndex, nestingLevel);
        Block parentBlock = getParentBlock(blocks, anyChildIndex, nestingLevel);
        int nextBlock = 0;
        Block[] nestedBlocks = new Block[childIndexes.size()];
        for (Integer childIndex : sortedChildrenIndexList) {
            if (!isParent(childIndex)) {
                Integer blockIndex = reverseProjection.get(childIndex);
                Block block = blocks[blockIndex];
                for (int i = nestingLevel; i > 0; i--) {
                    block = getChildBlockByIndex(block, 0);
                }
                LOG.debug("QueryData(%s) Projected field %s (received block index %s): %s is added to child list as child no. %s", traceStr, childIndex, blockIndex, block, nextBlock);
                nestedBlocks[nextBlock] = block;
            }
            else {
                Set<Integer> grandChildren = parentToChildren.get(childIndex);
                LOG.debug("QueryData(%s) Projected field index %s is a parent, will build from its child blocks %s", traceStr, childIndex, grandChildren);
                Block block = buildParentBlockFromChildren(blocks, grandChildren);
                LOG.debug("QueryData(%s) Built child block %s of type %s is added to child list as child no. %s", traceStr, block, block.getClass(), nextBlock, grandChildren);
                nestedBlocks[nextBlock] = block;
            }
            nextBlock++;
        }
        if (parentField.getType().equals(ArrowType.List.INSTANCE)) {
            verify(nestedBlocks.length == 1, "QueryData(%s) Expected block %s of Array type to have a single child", traceStr, parentBlock);
            Block nestedBlock = nestedBlocks[0];
            int parentPositionCount = parentBlock.getPositionCount();
            ColumnarArray parentAsColumnarArrayBlock = ColumnarArray.toColumnarArray(parentBlock);
            boolean[] arrayNulls = new boolean[parentPositionCount];
            int[] arrayOffsets = new int[parentPositionCount + 1];
            arrayOffsets[0] = 0;
            for (int i = 0; i < parentPositionCount; i++) {
                arrayNulls[i] = parentBlock.isNull(i);
                if (parentBlock.isNull(i)) {
                    arrayOffsets[i + 1] = arrayOffsets[i];
                }
                else {
                    int offset = parentAsColumnarArrayBlock.getOffset(i + 1);
                    arrayOffsets[i + 1] = offset;
                }
            }
            LOG.debug("QueryData(%s) Parent type is a Array - returning constructed Array block with nulls=%s, offsets=%s",
                    traceStr, Arrays.toString(arrayNulls), Arrays.toString(arrayOffsets));
            return ArrayBlock.fromElementBlock(parentPositionCount, Optional.of(arrayNulls), arrayOffsets, nestedBlock);
        }
        else if (parentField.getType().equals(ArrowType.Struct.INSTANCE)) {
            int parentPositionCount = parentBlock.getPositionCount();
            boolean[] nulls = new boolean[parentPositionCount];
            for (int i = 0; i < parentPositionCount; i++) {
                nulls[i] = parentBlock.isNull(i);
            }
            LOG.debug("QueryData(%s) Parent type is a row - returning new row from nestedBlocks=%s with positionCount=%s, nulls=%s",
                    traceStr, Arrays.asList(nestedBlocks), parentPositionCount, Arrays.toString(nulls));
            return RowBlock.fromNotNullSuppressedFieldBlocks(parentPositionCount, Optional.of(nulls), nestedBlocks);
        }
        else if (parentField.getType() instanceof ArrowType.Map) { // Map block is passed as two separate Array blocks - keys/values
            verify(nestedBlocks.length == 1,
                    "QueryData(%s) Expected block %s of Map type to have a single child, but had %s", traceStr, parentBlock, nestedBlocks.length);
            int parentPositionCount = parentBlock.getPositionCount();
            LOG.debug("QueryData(%s) Parent type is a Map - constructing from nestedBlocks=%s with positionCount=%s",
                    traceStr, Arrays.asList(nestedBlocks), parentPositionCount);

            // Map = two blocks of List(Struct(keysBlock)) & List(Struct(valuesBlock))
            // Nested projected blocks are array blocks
            Block nestedBlock = nestedBlocks[0];
            return mapBlock(parentField, parentBlock, parentPositionCount, nestedBlock);
        }
        else {
            throw new UnsupportedOperationException(format("QueryData(%s) unsupported block type: %s", traceStr, parentBlock));
        }
    }

    private int getNestingLevel(int anyChildIndex)
    {
        int tmp = anyChildIndex;
        int nestingLevel = 0;
        while (childToParent.get(tmp) != tmp) {
            nestingLevel++;
            tmp = childToParent.get(tmp);
        }
        return nestingLevel;
    }

    private MapBlock mapBlock(Field parentField, Block parentBlock, int parentPositionCount, Block nestedRowBlock)
    {
        RowBlock nestedRowBlockAsRowBlock = (RowBlock) nestedRowBlock;
        Block mapKeysBlock = nestedRowBlockAsRowBlock.getFieldBlock(0);
        Block mapValuesBlock = nestedRowBlockAsRowBlock.getFieldBlock(1);

        ColumnarArray parentAsColumnarArrayBlock = ColumnarArray.toColumnarArray(parentBlock);

        Type keyType = convertArrowFieldToTrinoType(parentField.getChildren().getFirst().getChildren().get(0));
        Type valueType = convertArrowFieldToTrinoType(parentField.getChildren().getFirst().getChildren().get(1));
        boolean[] mapNulls = new boolean[parentPositionCount];
        int[] mapOffsets = new int[parentPositionCount + 1];
        mapOffsets[0] = 0;
        for (int i = 0; i < parentPositionCount; i++) {
            boolean parentBlockNull = parentBlock.isNull(i);
            mapNulls[i] = parentBlockNull;
            if (parentBlockNull) {
                mapOffsets[i + 1] = mapOffsets[i];
            }
            else {
                int offset = parentAsColumnarArrayBlock.getOffset(i + 1);
                mapOffsets[i + 1] = offset;
            }
        }
        LOG.debug("QueryData(%s) Parent type is a Map - returning constructed Map block with nulls=%s, offsets=%s",
                traceStr, Arrays.toString(mapNulls), Arrays.toString(mapOffsets));
        return MapBlock.fromKeyValueBlock(Optional.of(mapNulls), mapOffsets, mapKeysBlock, mapValuesBlock, new MapType(keyType, valueType, TYPE_OPERATORS));
    }

    private Field getParentSchemaType(int anyChildIndex, int nestingLevel)
    {
        Queue<Field> parentSubTree = new LinkedList<>();
        Field field = flatSchema.get(anyChildIndex);
        parentSubTree.add(field);
        while (!field.getChildren().isEmpty()) {
            field = field.getChildren().getFirst(); // TODO - test list of struct
            parentSubTree.add(field);
        }
        // There are at least two objects in the Q, so polling two times won't return null
        Field parentType = parentSubTree.poll();
        Field childType = parentSubTree.poll();
        while (nestingLevel-- > 1) {
            parentType = childType;
            childType = parentSubTree.poll();
        }
        return parentType;
    }

    private Block getParentBlock(Block[] blocks, Integer index, int nestingLevel)
    {
        Block parentBlock = null;
        if (reverseProjection.containsKey(index)) {
            parentBlock = blocks[reverseProjection.get(index)];
        }
        else {
            int toSearch = index;
            while (parentBlock == null) {
                Set<Integer> children = parentToChildren.get(toSearch);
                for (Integer child : children) {
                    if (reverseProjection.containsKey(child)) {
                        parentBlock = blocks[reverseProjection.get(child)];
                    }
                }
                toSearch = children.stream().findAny().orElseThrow();
            }
        }
        requireNonNull(parentBlock);
        while (nestingLevel-- > 1) {
            parentBlock = getChildBlockByIndex(parentBlock, 0);
        }
        return parentBlock;
    }

    private Block buildProjectionBlock(Block[] blocks, int projectedFieldIndex, Set<Integer> buildIndexes)
    {
        LOG.debug("QueryData(%s) Building projected block: projectedFieldIndex=%s, builtIndexes=%s", traceStr, projectedFieldIndex, buildIndexes);
        int parentIndex = childToParent.get(projectedFieldIndex);
        if (!buildIndexes.contains(projectedFieldIndex)) {
            if (parentIndex == projectedFieldIndex) {
                Integer blockIndex = reverseProjection.get(projectedFieldIndex);
                Block block = blocks[blockIndex];
                LOG.debug("QueryData(%s) Received block index %s is a root level block, returning for projected field %s: %s", traceStr, blockIndex, projectedFieldIndex, block);
                buildIndexes.add(blockIndex);
                return block; // non nested root
            }
            else {
                LOG.debug("QueryData(%s) Projected field %s is a nested block. Building parent from children", traceStr, projectedFieldIndex);
                int childIndex = parentIndex;
                while ((parentIndex = childToParent.get(childIndex)) != childIndex) {
                    LOG.debug("QueryData(%s) Found parent index %s of child %s", traceStr, parentIndex, childIndex);
                    childIndex = parentIndex;
                }
                if (!buildIndexes.contains(parentIndex)) {
                    Set<Integer> children = parentToChildren.get(parentIndex);
                    LOG.debug("QueryData(%s) Found children for parent index %s: %s", traceStr, parentIndex, children);
                    buildIndexes.addAll(children);
                    buildIndexes.add(parentIndex);
                    return buildParentBlockFromChildren(blocks, children);
                }
                else {
                    LOG.debug("QueryData(%s) Parent index %s of child index %s was already iterated", traceStr, parentIndex, projectedFieldIndex);
                    return null;
                }
            }
        }
        else {
            LOG.debug("QueryData(%s) Projection index %s was already iterated", traceStr, projectedFieldIndex);
            return null;
        }
    }

    private static UnaryOperator<Field> getFieldConstructor(Field parentField)
    {
        return childField -> new Field(parentField.getName(), parentField.getFieldType(), List.of(childField));
    }

    private static int addFlatFields(int nestingLevel, int parentIndex, Optional<UnaryOperator<Field>> parentConstructor, List<Field> schemaFields, List<Field> flatFieldsList, HashMap<Integer, Integer> reverseFlatMapping)
    {
        // flat list: [a, b(x, y(n,m), d] = [a, b, b.x, b.y, b.y.n, b.y.m, d] = [0, 1, 2 ,3, 4, 5, 6]
        // reverse index mapping: {0:0, 1:1, 2:1, 3:1, 4:3, 5:3, 6:6]
        int relativeIndex = parentIndex;
        for (int i = 0; i < schemaFields.size(); i++) {
            Field field = schemaFields.get(i);
            int selfAsParentIndex;
            if (nestingLevel == 0) {
                int size = reverseFlatMapping.size();
                reverseFlatMapping.put(size, size);
                selfAsParentIndex = size;
            }
            else {
                selfAsParentIndex = relativeIndex + 1 + i;
                reverseFlatMapping.put(selfAsParentIndex, parentIndex);
            }
            flatFieldsList.add(parentConstructor.orElse(UnaryOperator.identity()).apply(field));
            List<Field> children = field.getChildren();
            if (!children.isEmpty()) {
                relativeIndex += children.size();
                UnaryOperator<Field> fieldConstructor = f -> {
                    if (parentConstructor.isPresent()) {
                        return getFieldConstructor(field).andThen(parentConstructor.orElseThrow()).apply(f);
                    }
                    else {
                        return getFieldConstructor(field).apply(f);
                    }
                };
                relativeIndex += addFlatFields(nestingLevel + 1, selfAsParentIndex, Optional.of(fieldConstructor), children, flatFieldsList, reverseFlatMapping);
            }
        }
        return relativeIndex - parentIndex;
    }

    public Page construct(Block[] blocks, int rows)
    {
        Set<Integer> builtParents = new HashSet<>();
        Block[] projectedBlocks = applyProjections(IntStream.range(0, blocks.length)
                .mapToObj(i -> buildProjectionBlock(blocks, projections.get(i), builtParents))
                .filter(Objects::nonNull).toArray(Block[]::new));
        Page page = new Page(rows, projectedBlocks);
        LOG.debug("QueryData(%s) Constructed page: %s", traceStr, page);
        return page; //TODO - more validations on returned page
    }

    private Block[] applyProjections(Block[] restoredNestedBlocks)
    {
        verify(restoredNestedBlocks.length == expectedParentFields.size(),
                "QueryData(%s) Failed constructing page: Received %s columns, expected %s", traceStr, restoredNestedBlocks.length, expectedParentFields.size());
        AtomicInteger blocksNum = new AtomicInteger(0);
        baseFieldWithProjections.forEach((field, map) -> blocksNum.addAndGet(map.size()));
        Block[] resultedBlocks = new Block[blocksNum.get()];
        Iterator<Field> fieldIterator = expectedParentFields.iterator();
        IntStream.range(0, restoredNestedBlocks.length).forEach(i -> {
            Block block = restoredNestedBlocks[i];
            Field field = fieldIterator.next();
            LinkedHashMap<List<Integer>, Integer> projectionPaths = baseFieldWithProjections.get(field);
            LOG.debug("QueryData(%s) applyProjections for index %s, on block %s, field %s, projectionPaths=%s", traceStr, i, block, field, projectionPaths);
            LinkedList<LinkedHashMap<Integer, Integer>> absoluteFieldProjections = new LinkedList<>();
            AtomicBoolean rootLevelProjection = new AtomicBoolean(false);
            LinkedHashMap<List<Integer>, Integer> sortedProjectionPaths = new LinkedHashMap<>();

            // nested projected paths might not be matching schema order, need to sort children paths for correct resulted block traversal for projections extraction
            // comparator will sort paths by nesting level & in-level children index order
            List<List<Integer>> projectionPathsSortedByNestedPaths = projectionPaths.keySet().stream().sorted(PROJECTION_PATH_COMPARATOR).toList();
            projectionPathsSortedByNestedPaths.forEach(path -> {
                Integer integer = projectionPaths.get(path);
                sortedProjectionPaths.put(path, integer);
            });
            sortedProjectionPaths.forEach((path, index) -> {
                LOG.debug("QueryData(%s) applyProjections for path: %s", traceStr, path);
                if (path.isEmpty()) {
                    rootLevelProjection.set(true);
                }
                else {
                    for (int x = 0; x < path.size(); x++) {
                        Integer key = path.get(x);
                        if (absoluteFieldProjections.size() <= x) {
                            int mappedIndex = (x == 0 && rootLevelProjection.get()) ? key : 0;
                            LOG.debug("QueryData(%s) applyProjections Adding first entry to level %s: %s=%s", traceStr, x, key, mappedIndex);
                            LinkedHashMap<Integer, Integer> xLevelIndices = new LinkedHashMap<>(1);
                            xLevelIndices.put(key, mappedIndex);
                            absoluteFieldProjections.add(xLevelIndices);
                        }
                        else {
                            LinkedHashMap<Integer, Integer> xLevelIndices = absoluteFieldProjections.get(x);
                            int value = xLevelIndices.size();
                            LOG.debug("QueryData(%s) applyProjections Adding another entry to level %s: %s=%s", traceStr, x, key, value);
                            Integer previousMapping = xLevelIndices.putIfAbsent(key, value);
                            if (previousMapping != null) {
                                LOG.debug("QueryData(%s) applyProjections level %s already has mapping for %s: %s", traceStr, x, key, previousMapping);
                            }
                        }
                    }
                }
            });
            LOG.debug("QueryData(%s) absoluteFieldProjections=%s", traceStr, absoluteFieldProjections);
            projectionPaths.forEach((path, index) -> resultedBlocks[index] = traverseBlockProjectionPath(block, Optional.empty(), field, absoluteFieldProjections, path, 0));
        });
        return resultedBlocks;
    }

    private Block traverseBlockProjectionPath(Block block, Optional<boolean[]> parentNullVector, Field field, LinkedList<LinkedHashMap<Integer, Integer>> absoluteFieldProjections, List<Integer> path, int searchDepth)
    {
        LOG.debug("QueryData(%s) traverseBlockProjectionPath for block=%s, field=%s, path=%s, searchDepth=%s", traceStr, block, field, path, searchDepth);
        if (searchDepth == path.size()) {
            return block;
        }
        Integer projectionSubIndex = path.get(searchDepth);
        Integer blockChildIndex = absoluteFieldProjections.get(searchDepth).get(projectionSubIndex);
        Optional<boolean[]> nullsForChild = getNullVector(block, parentNullVector);
        LOG.debug("QueryData(%s) traverseBlockProjectionPath projectionSubIndex=%s, blockChildIndex=%s, nullsForChild=%s", traceStr, projectionSubIndex, blockChildIndex, nullsForChild.map(Arrays::toString).orElse("empty"));

        Block child = getChildBlockByIndex(block, blockChildIndex);
        return traverseBlockProjectionPath(child, nullsForChild, field, absoluteFieldProjections, path, searchDepth + 1);
    }

    private static Block getChildBlockByIndex(Block block, Integer blockChildIndex)
    {
        if (block instanceof RowBlock rowBlock) {
            return rowBlock.getFieldBlock(blockChildIndex);
        }
        else if (block instanceof MapBlock mapBlock) {
            if (blockChildIndex == 0) {
                return mapBlock.getMap(0).getRawKeyBlock();
            }
            else if (blockChildIndex == 1) {
                return mapBlock.getMap(0).getRawValueBlock();
            }
            else {
                throw new RuntimeException("Unexpected map block child index: " + blockChildIndex);
            }
        }
        else if (block instanceof ArrayBlock arrayBlock){
            if (blockChildIndex != 0) {
                throw new RuntimeException("Unexpected array block child index: " + blockChildIndex);
            }
            try {
                Method getRawElementBlock = ArrayBlock.class.getDeclaredMethod("getRawElementBlock");
                getRawElementBlock.setAccessible(true);
                return (Block) getRawElementBlock.invoke(arrayBlock);
            }
            catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("Failed extracting child block from array block", e);
            }
        }
        throw new RuntimeException(format("Unexpected block type for child block extraction: %s, blockChildIndex: %s", block.getClass(), blockChildIndex));
    }

    private Optional<boolean[]> getNullVector(Block block, Optional<boolean[]> parentNullVector)
    {
        if (!block.mayHaveNull()) {
            return parentNullVector;
        }
        else {
            if (parentNullVector.isPresent()) {
                boolean[] nullsFromParent = parentNullVector.orElseThrow();
                int newSize = nullsFromParent.length;
                boolean[] newNulls = new boolean[newSize];
                int selfPositionIteration = 0;
                for (int i = 0; i < newSize; i++) {
                    if (nullsFromParent[i]) {
                        newNulls[i] = true;
                    }
                    else {
                        newNulls[i] = block.isNull(selfPositionIteration);
                        selfPositionIteration++;
                    }
                }
                return Optional.of(newNulls);
            }
            else {
                boolean[] newNulls = new boolean[block.getPositionCount()];
                for (int i = 0; i < block.getPositionCount(); i++) {
                    newNulls[i] = block.isNull(i);
                }
                return Optional.of(newNulls);
            }
        }
    }

    static QueryDataResponseSchemaConstructor deconstruct(String traceStr, Schema schema, List<Integer> projections,
            LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>>  baseFieldWithProjections)
    {
        LOG.debug("QueryData(%s): Analyzing schema projections: schema=%s, projections=%s, projectionPaths=%s", traceStr, schema, projections, baseFieldWithProjections);
        HashMap<Integer, Integer> reverseFlatMapping = new HashMap<>();
        List<Field> flattenedFields = new ArrayList<>();
        addFlatFields(0, 0, Optional.empty(), schema.getFields(), flattenedFields, reverseFlatMapping);
        LinkedHashSet<Field> expectedParentFields = new LinkedHashSet<>();
        LinkedHashMap<Integer, Field> orderedProjectedFieldsMap = new LinkedHashMap<>();
        for (int i : projections) {
            orderedProjectedFieldsMap.put(i, flattenedFields.get(i));
            int child = i;
            int parent = reverseFlatMapping.get(child);
            while (child != parent) {
                child = parent;
                parent = reverseFlatMapping.get(child);
            }
            expectedParentFields.add(flattenedFields.get(parent));
        }
        Map<Integer, Set<Integer>> parentToChildren = new HashMap<>();
        for (int i : projections) {
            int projection = i;
            Integer parent = reverseFlatMapping.get(projection);
            while (parent != projection) {
                parentToChildren.computeIfAbsent(parent, LinkedHashSet::new).add(projection);
                projection = parent;
                parent = reverseFlatMapping.get(projection);
            }
        }
        return new QueryDataResponseSchemaConstructor(traceStr, flattenedFields, orderedProjectedFieldsMap, expectedParentFields, projections,
                reverseFlatMapping, parentToChildren, baseFieldWithProjections);
    }
}
