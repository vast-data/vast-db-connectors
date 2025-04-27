/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.vastdata.client.adaptor.VectorAdaptor;
import com.vastdata.client.adaptor.VectorAdaptorFactory;
import com.vastdata.client.tx.VastTraceToken;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import io.airlift.log.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

public class ArrowQueryDataSchemaHelper
{
    private static final Logger LOG = Logger.get(ArrowQueryDataSchemaHelper.class);

    private final LinkedHashMap<Integer, Set<Integer>> parentToChildren;
    private final LinkedHashMap<Integer, Integer> childToParent;
    private final HashMap<Integer, Integer> projectionsIndexedMappingReversed;
    private final Map<Integer, Field> enumeratedTopFields;
    private final VastTraceToken token;
    private final List<Field> projectionFields;
    private final VectorAdaptorFactory vectorAdaptorFactory;

    private ArrowQueryDataSchemaHelper(VastTraceToken token, Map<Integer, Field> enumeratedTopFields, List<Field> projectionFields,
                                       LinkedHashMap<Integer, Integer> projectionsIndexedMappingReversed, LinkedHashMap<Integer, Set<Integer>> parentToChildren, LinkedHashMap<Integer, Integer> childToParent,
                                       VectorAdaptorFactory vectorAdaptorFactory)
    {
        this.token = token;
        this.enumeratedTopFields = enumeratedTopFields;
        this.projectionFields = projectionFields;
        this.projectionsIndexedMappingReversed = projectionsIndexedMappingReversed;
        this.parentToChildren = parentToChildren;
        this.childToParent = childToParent;
        this.vectorAdaptorFactory = vectorAdaptorFactory;
        LOG.info("new ArrowQueryDataSchemaHelper:{}: enumeratedTopFields={}, projectionFields={}, " +
                        "projectionsIndexedMappingReversed={}, parentToChildren={}, childToParent={}",
                this.token, enumeratedTopFields, projectionFields,
                projectionsIndexedMappingReversed, parentToChildren, childToParent);
    }

    private static int splitNestedFields(int i, Field parent, BiConsumer<Integer, Field> leafFieldConsumer,
                                         Map<Integer, Set<Integer>> parentToChildren)
    {
//        split nested type field to multiple fields per leaf node.
//        Struct(col1, Struct(col2, col3)) -> Struct(col1), Struct(Struct(col2)), Struct(Struct(col3)) in order to adapt to vast query data projections protocol
        if (!parent.getType().isComplex()) {
            leafFieldConsumer.accept(i, parent);
            return i + 1;
        }
        else {
            BiConsumer<Integer, Field> complexConsumer = (index, field) -> {
                List<Field> children = ImmutableList.of(field);
                leafFieldConsumer.accept(index, new Field(parent.getName(), parent.getFieldType(), children));
            };
            int next = i + 1;
            for (int j = 0; j < parent.getChildren().size(); j++) {
                Field child = parent.getChildren().get(j);
                parentToChildren.computeIfAbsent(i, x -> new LinkedHashSet<>()).add(next);
                next = splitNestedFields(next, child, complexConsumer, parentToChildren);
            }
            return next;
        }
    }

    public static ArrowQueryDataSchemaHelper deconstruct(VastTraceToken token, Schema projectionSchema, VectorAdaptorFactory vectorAdaptorFactory)
    {
        LinkedHashMap<Integer, Field> indexedProjections = new LinkedHashMap<>();
        LinkedHashMap<Integer, Set<Integer>> parentToChildren = new LinkedHashMap<>();
        List<Field> fields = projectionSchema.getFields();
        Map<Integer, Field> enumeratedTopFields = new HashMap<>();
        int next = 0;
        for (Field field : fields) {
            enumeratedTopFields.put(next, field);
            next = splitNestedFields(next, field, indexedProjections::put, parentToChildren);
        }
        LinkedHashMap<Integer, Integer> childToParent = new LinkedHashMap<>();
        LinkedHashMap<Integer, Integer> projectionsIndexedMappingReversed = new LinkedHashMap<>();
        AtomicInteger ind = new AtomicInteger(0);
        indexedProjections.keySet().forEach(i -> projectionsIndexedMappingReversed.put(i, ind.getAndIncrement()));
        List<Field> projectionFields = ImmutableList.copyOf(indexedProjections.values());
        parentToChildren.forEach((parent, childrenSet) -> childrenSet.forEach(l -> childToParent.put(l, parent)));
        return new ArrowQueryDataSchemaHelper(token, enumeratedTopFields, projectionFields, projectionsIndexedMappingReversed, parentToChildren, childToParent, vectorAdaptorFactory);
    }

    public List<Field> getFields()
    {
        return projectionFields;
    }

    public VectorSchemaRoot construct(List<FieldVector> vectors, int rowCount, BufferAllocator allocator)
    {
        verify(vectors.size() == projectionFields.size(),
                format("%s Expected %s vectors, but got %s", this.token, projectionFields.size(), vectors.size()));
        Map<Integer, FieldVector> builtVectors = new HashMap<>();
        Set<Integer> resultVectorsIndexes = new LinkedHashSet<>();
        enumeratedTopFields.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach((entry) -> {
            Integer parentIndex = entry.getKey();
            Field parentField = entry.getValue();
            LOG.debug("{} Handling parent index: {}, field: {}", token, parentIndex, parentField);
            buildVector(builtVectors, vectors, 0, parentField, parentIndex, parentToChildren.get(parentIndex), allocator);
            resultVectorsIndexes.add(parentIndex);
            LOG.debug("{} Built parent index {}", token, parentIndex);
        });
        List<FieldVector> resultVectors = resultVectorsIndexes.stream().map(builtVectors::get).collect(Collectors.toList());
        LOG.debug("{} Resulted built indexes: {}, builtVectors={}, original vectors={}, resulted vectors={}", token, resultVectorsIndexes, builtVectors, vectors, resultVectors);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(resultVectors);
        vectorSchemaRoot.setRowCount(rowCount);
        return vectorSchemaRoot;
    }

    private void buildVector(Map<Integer, FieldVector> builtVectors, List<FieldVector> valueVectors,
                             int nestingLevel, Field parentField, int index, Set<Integer> childIndexes,
                             BufferAllocator allocator)
    {
        LOG.debug("{} builtVectors START: index={}, childIndexes={}, parentField={}, nestingLevel={}, builtVectors={}",
                token, index, childIndexes, parentField, nestingLevel, builtVectors);
        if (!builtVectors.containsKey(index)) {
            if (childIndexes == null || childToParent.isEmpty()) {
                // for leaf projections - adapt vector types if needed and add to builtVectors map
                FieldVector tmpVector = valueVectors.get(this.projectionsIndexedMappingReversed.get(index));
                for (int i = 0; i < nestingLevel; i++) {
                    tmpVector = Iterables.getOnlyElement(tmpVector.getChildrenFromFields()); // assuming only one child
                }
                Field field = tmpVector.getField();
                FieldVector newVector = changeVectorAllocatorAndAdaptIfNeeded(tmpVector, field, allocator);
                LOG.debug("{} builtVectors END put({}, {}) before adaptation: {}", token, index, newVector.getField(), field);
                builtVectors.put(index, newVector);
            }
            else {
                // for non leaf projections - build all child fields recursively and construct a new nested type according to field type
                List<Integer> sortedChildren = childIndexes.stream().sorted().collect(Collectors.toList());
                for (int i = 0; i < sortedChildren.size(); i++) {
                    Integer childIndex = sortedChildren.get(i);
                    buildVector(builtVectors, valueVectors, nestingLevel + 1,
                            parentField.getChildren().get(i), childIndex, parentToChildren.get(childIndex), allocator);
                }
                FieldVector nestedVector = findFieldVectorByNestingLevelFromChildren(valueVectors, nestingLevel, sortedChildren);
                if (parentField.getType().getTypeID().equals(ArrowType.ArrowTypeID.Struct)) {
                    LOG.debug("builtVectors Struct for index={}", index);
                    StructVector structVector = StructVector.empty(parentField.getName(), nestedVector.getAllocator());
                    childIndexes.forEach(childIndex -> {
                        FieldVector vector = builtVectors.get(childIndex);
                        LOG.debug("builtVectors Struct addOrGet to vector: {}, {}, {}, structVector: {}, structVector.children: {}",
                                vector.getName(), vector.getField().getFieldType().getType(), vector.getClass(), structVector, structVector.getChildrenFromFields());
                        FieldVector newVector = structVector.addOrGet(vector.getName(), vector.getField().getFieldType(), vector.getClass());
                        TransferPair transferPair = vector.makeTransferPair(newVector);
                        transferPair.transfer();
                    });
                    List<ArrowBuf> validityBuffer = Lists.newArrayList(nestedVector.getValidityBuffer());
                    ArrowFieldNode node = new ArrowFieldNode(nestedVector.getValueCount(), nestedVector.getNullCount());
                    structVector.loadFieldBuffers(node, validityBuffer);
                    LOG.debug("builtVectors END built new Struct for index={}: {}", index, structVector.getField());
                    builtVectors.put(index, structVector);
                }
                else if (parentField.getType().getTypeID().equals(ArrowType.ArrowTypeID.List) || parentField.getType().getTypeID().equals(ArrowType.ArrowTypeID.Map)) {
                    // List and Map differ only in vector constructor
                    ArrowType.ArrowTypeID listOrMap = parentField.getType().getTypeID();
                    LOG.debug("{} builtVectors {} for index={}, parentField={}", token, listOrMap, index, parentField);
                    Integer arrayChildIndex = Iterables.getOnlyElement(childIndexes);
                    FieldVector builtChildVector = builtVectors.get(arrayChildIndex);
                    LOG.debug("{} builtVectors {} for index={}, arrayChildIndex={}, builtChildVector of type: {}", token, builtChildVector, arrayChildIndex, arrayChildIndex, builtChildVector.getField());
                    ListVector resultListVector;
                    if (parentField.getType().getTypeID().equals(ArrowType.ArrowTypeID.List)) {
                        resultListVector = ListVector.empty(parentField.getName(), nestedVector.getAllocator());
                    }
                    else {
                        resultListVector = MapVector.empty(parentField.getName(), nestedVector.getAllocator(), false);
                    }
                    FieldVector resultListDataVector = (FieldVector) resultListVector.addOrGetVector(new FieldType(false, builtChildVector.getField().getType(), builtChildVector.getField().getDictionary())).getVector();
                    LOG.debug("{} builtVectors {} for index={} of type: {}, resultListDataVector type: {}", token, listOrMap, index, builtChildVector.getField(), resultListDataVector.getField());
                    TransferPair transferPair = builtChildVector.makeTransferPair(resultListDataVector);
                    transferPair.transfer();
                    ArrowFieldNode node = new ArrowFieldNode(nestedVector.getValueCount(), nestedVector.getNullCount());
                    List<ArrowBuf> arrayBuffs = Lists.newArrayList(nestedVector.getValidityBuffer(), nestedVector.getOffsetBuffer());
                    resultListVector.loadFieldBuffers(node, arrayBuffs);
                    LOG.debug("{} builtVectors END built new {} for index={}: {}", token, listOrMap, index, resultListVector.getField());
                    builtVectors.put(index, resultListVector);
                }
                else {
                    throw new RuntimeException(format("%s Unsupported type: %s", token, parentField.getType()));
                }
            }
        }
        else {
            LOG.debug("{} builtVectors END skipping already built index: {}", token, index);
        }
    }

    private FieldVector findFieldVectorByNestingLevelFromChildren(List<FieldVector> valueVectors, int nestingLevel, List<Integer> sortedChildren)
    {
        Integer aChild = sortedChildren.get(0);
        while (this.parentToChildren.get(aChild) != null && !this.parentToChildren.get(aChild).isEmpty()) {
            aChild = this.parentToChildren.get(aChild).stream().findAny().get();
        }
        FieldVector nestedVector = valueVectors.get(projectionsIndexedMappingReversed.get(aChild));
        for (int i = 0; i < nestingLevel; i++) {
            nestedVector = Iterables.getOnlyElement(nestedVector.getChildrenFromFields());
        }
        return nestedVector;
    }

    private FieldVector changeVectorAllocatorAndAdaptIfNeeded(FieldVector tmpVector, Field field, BufferAllocator allocator)
    {
        FieldVector newVector;
        Optional<VectorAdaptor> adaptorOptional = vectorAdaptorFactory.forField(field);
        if (adaptorOptional.isPresent())
        {
            VectorAdaptor vectorAdaptor = adaptorOptional.get();
            LOG.debug("{} adaptVectorIfNeeded Got adaptor {} for vector: {}, of field: {}",
                    token, vectorAdaptor.getClass(), tmpVector, tmpVector.getField());
            newVector = vectorAdaptor.adapt(tmpVector, field, allocator);
        }
        else {
            newVector = tmpVector.getField().createVector(allocator);
            tmpVector.makeTransferPair(newVector).transfer();
            LOG.debug("{} adaptVectorIfNeeded No adaptor for vector: {}, of type: {}. New vector: {}, of field: {}",
                    token, tmpVector, tmpVector.getField(), newVector, newVector.getField());
        }
        return newVector;
    }
}
