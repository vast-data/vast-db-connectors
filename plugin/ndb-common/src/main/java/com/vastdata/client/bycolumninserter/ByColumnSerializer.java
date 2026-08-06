/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.bycolumninserter;

import com.vastdata.client.RecordBatchSplitter;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.client.schema.VastPayloadSerializer;
import io.airlift.log.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ByColumnSerializer
{
    private static final Logger LOG = Logger.get(ByColumnSerializer.class);

    private static final VastPayloadSerializer<VectorSchemaRoot> SERIALIZER = VastPayloadSerializer.getInstanceForRecordBatch();

    private final VastConfig vastConfig;
    private final RecordBatchSplitterMetrics metrics;
    private final RowIDStrategyType rowIdType;
    private final String traceToken;
    private final List<String> allFields;
    private final List<String> nonUpdatableFields;
    private final List<String> updatableFields;

    public ByColumnSerializer(VastConfig vastConfig,
            RecordBatchSplitterMetrics metrics, RowIDStrategyType rowIdType,
            String traceToken)
    {
        this.vastConfig = vastConfig;
        this.metrics = metrics;
        this.rowIdType = rowIdType;
        this.traceToken = traceToken;
        this.allFields = new ArrayList<>();
        this.nonUpdatableFields = new ArrayList<>();
        this.updatableFields = new ArrayList<>();
    }

    private static VectorSchemaRoot vsrToAppendTo(List<Field> fields,
            int expectedRowCount, BufferAllocator allocator)
    {
        List<FieldVector> mergedVectors = new ArrayList<>(fields.size());
        for (Field field : fields) {
            FieldVector newVec = field.createVector(allocator);
            newVec.setInitialCapacity(expectedRowCount);
            newVec.allocateNew();
            mergedVectors.add(newVec);
        }

        return new VectorSchemaRoot(mergedVectors);
    }

    private static boolean isPrimitive(FieldVector vector)
    {
        MinorType type = vector.getMinorType();

        // These are the standard complex (nested) types in Arrow
        switch (type) {
            case LIST:
            case FIXED_SIZE_LIST:
            case LARGELIST:
            case STRUCT:
            case MAP:
            case UNION:
            case DENSEUNION:
                return false;
            default:
                return true;
        }
    }

    private void initializeFieldsIfNeeded(List<VectorSchemaRoot> vsrs,
            Predicate<String> isNonUpdateableColumn)
    {
        if (!allFields.isEmpty()) {
            return;
        }
        VectorSchemaRoot vsr = vsrs.get(0);
        List<String> updatablePrimitiveFields = new ArrayList<>();
        List<String> updatableNestedFields = new ArrayList<>();

        for (FieldVector fv : vsr.getFieldVectors()) {
            String name = fv.getName();
            allFields.add(name);
            if (isNonUpdateableColumn.test(name)) {
                nonUpdatableFields.add(name);
            }
            else if (isPrimitive(fv)) {
                updatablePrimitiveFields.add(name);
            }
            else {
                updatableNestedFields.add(name);
            }
        }
        updatableFields.addAll(updatablePrimitiveFields);
        updatableFields.addAll(updatableNestedFields);
    }

    private InsertPlan makeNonUpdateableColsInsertPlan(
            List<VectorSchemaRoot> vsrs,
            Predicate<String> isNonUpdateableColumn, BufferAllocator allocator)
            throws VastUserException
    {
        if (vsrs.isEmpty()) {
            throw new RuntimeException("vsr must not be empty here");
        }

        int totalRowCount = vsrs
                .stream()
                .mapToInt(VectorSchemaRoot::getRowCount)
                .sum();

        initializeFieldsIfNeeded(vsrs, isNonUpdateableColumn);

        VectorSchemaRoot firstVsr = vsrs.get(0);

        int mustsApproximateSize = SerializedSizeApproximator
                .approximateSizeByColumnVsrs(vsrs, nonUpdatableFields)
                .values()
                .stream()
                .mapToInt(Integer::intValue)
                .sum();
        Map<String, Integer> approximateSizes = SerializedSizeApproximator.approximateSizeByColumnVsrs(
                vsrs, updatableFields);
        List<String> mightGoInUpdatableFields = SerializedSizeApproximator.estimateWhichWillGoIn(
                updatableFields, approximateSizes,
                vastConfig.getMaxRequestBodySize() - mustsApproximateSize);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "[%s] makeNonUpdateableColsInsertPlan: non-updatable fields to serialize: %s",
                    traceToken,
                    nonUpdatableFields);
            LOG.debug(
                    "[%s] makeNonUpdateableColsInsertPlan: updatable fields that might go in: %s",
                    traceToken,
                    mightGoInUpdatableFields);
        }

        List<String> fieldsToMergeNames = new ArrayList<>(nonUpdatableFields);
        fieldsToMergeNames.addAll(mightGoInUpdatableFields);
        List<Field> fieldsToMerge = fieldsToMergeNames
                .stream()
                .map(f -> firstVsr.getSchema().findField(f))
                .collect(Collectors.toList());

        try (VectorSchemaRoot mergedVsr = vsrToAppendTo(fieldsToMerge,
                totalRowCount, allocator)) {
            for (VectorSchemaRoot origVsr : vsrs) {
                VectorSchemaRoot vsr = vsrGetColumns(origVsr,
                        fieldsToMergeNames);
                VectorSchemaRootAppender.append(false, mergedVsr, vsr);
            }

            Step step = packAndSerialize(mergedVsr, nonUpdatableFields,
                    mightGoInUpdatableFields);

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "[%s] makeNonUpdateableColsInsertPlan: fields left from mightGoIn for update: %s",
                        traceToken,
                        step.restFields);
            }

            List<String> fieldsForUpdate = allFields
                    .stream()
                    .filter(f -> !step.serializedFields.contains(f))
                    .collect(Collectors.toList());

            List<VectorSchemaRoot> vsrsForUpdate = vsrs
                    .stream()
                    .map(vsr -> vsrGetColumns(vsr, fieldsForUpdate))
                    .collect(Collectors.toList());

            return new InsertPlan(step.payloads, vsrsForUpdate);
        }
    }

    /**
     *
     * Assumes - all VSRs have the same schema. - no empty VSRs (at least one
     * column at least one row) - at least one VSR
     */
    public InsertPlan makeInsertPlan(List<VectorSchemaRoot> vsrs,
            Predicate<String> isNonUpdateableColumn, VastConfig config,
            BufferAllocator allocator)
            throws VastUserException
    {
        initializeFieldsIfNeeded(vsrs, isNonUpdateableColumn);

        LOG.debug("[%s] makeInsertPlan: starting with %d VSRs", traceToken,
                vsrs.size());
        VectorSchemaRoot firstVsr = vsrs.get(0);

        boolean hasNonUpdateableCols = !nonUpdatableFields.isEmpty();
        boolean hasPrimitiveCols = updatableFields
                .stream()
                .anyMatch(f -> isPrimitive(firstVsr.getVector(f)));

        LOG.debug(
                "[%s] makeInsertPlan: hasNonUpdateableCols=%b, hasPrimitiveCols=%b",
                traceToken,
                hasNonUpdateableCols, hasPrimitiveCols);

        if (hasNonUpdateableCols) {
            LOG.debug(
                    "[%s] makeInsertPlan: delegating to makeNonUpdateableColsInsertPlan",
                    traceToken);
            return makeNonUpdateableColsInsertPlan(vsrs, isNonUpdateableColumn,
                    allocator);
        }
        else if (hasPrimitiveCols) {
            LOG.debug(
                    "[%s] makeInsertPlan: delegating to singleInsertRpcMaxPrimitiveColumns",
                    traceToken);
            return singleInsertRpcMaxPrimitiveColumns(vsrs, allocator);
        }
        else {
            LOG.debug("[%s] makeInsertPlan: delegating to simpleInsertPlan",
                    traceToken);
            return simpleInsertPlan(vsrs);
        }
    }

    private List<FieldVector> getFieldVectors(VectorSchemaRoot vsr,
            List<String> columns)
    {
        return vsr
                .getFieldVectors()
                .stream()
                .filter(fv -> columns.contains(fv.getName()))
                .collect(Collectors.toList());
    }

    private Step packAndSerialize(VectorSchemaRoot vsr, List<String> musts,
            List<String> optionals)
            throws VastUserException
    {
        List<String> allFields = new ArrayList<>(musts);
        allFields.addAll(optionals);

        List<FieldVector> mustVectors = getFieldVectors(vsr, musts);
        List<FieldVector> optionalVectors = getFieldVectors(vsr, optionals);

        long mustsApproximateSize = SerializedSizeApproximator.approximateSize(
                mustVectors);
        long spaceLeftForOptionals = vastConfig.getMaxRequestBodySize() - mustsApproximateSize;

        List<String> mightGoInFields = SerializedSizeApproximator.estimateWhichWillGoIn(
                SerializedSizeApproximator.approximateSizeByColumnFieldVectors(
                        optionalVectors), spaceLeftForOptionals);

        List<String> optionalCandidates = mightGoInFields
                .stream()
                .collect(Collectors.toList());
        List<byte[]> serialized;
        List<String> serializedFields;

        while (true) {
            serializedFields = new ArrayList<>(musts);
            serializedFields.addAll(optionalCandidates);
            List<FieldVector> toSerialize = getFieldVectors(vsr,
                    serializedFields);

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "[%s] packAndSerialize: attempting to serialize candidates: %s",
                        traceToken,
                        toSerialize
                                .stream()
                                .map(FieldVector::getName)
                                .collect(Collectors.toList()));
            }

            serialized = List.of(serialize(toSerialize, vsr.getRowCount()));

            if (serialized.get(
                    0).length <= vastConfig.getMaxRequestBodySize()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "[%s] packAndSerialize: successfully fit fields: %s",
                            traceToken,
                            toSerialize
                                    .stream()
                                    .map(FieldVector::getName)
                                    .collect(Collectors.toList()));
                }
                break;
            }
            else if (optionalCandidates.isEmpty() || (musts.isEmpty() && optionalCandidates.size() == 1)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "[%s] packAndSerialize: falling back to splitByRows for fields: %s",
                            traceToken,
                            toSerialize
                                    .stream()
                                    .map(FieldVector::getName)
                                    .collect(Collectors.toList()));
                }
                // Can't shrink anymore, fall back to row-level splitting
                serialized = splitByRows(toSerialize, vsr.getRowCount());
                break;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "[%s] packAndSerialize: payload too big (%d > %d), dropping column: %s",
                        traceToken,
                        serialized.get(0).length,
                        vastConfig.getMaxRequestBodySize(),
                        optionalCandidates.get(optionalCandidates.size() - 1));
            }

            // Payload too big, drop the last optional column and retry
            optionalCandidates.remove(optionalCandidates.size() - 1);
        }

        List<String> finalSerializedFields = serializedFields;
        List<String> restFields = allFields
                .stream()
                .filter(f -> !finalSerializedFields.contains(f))
                .collect(Collectors.toList());

        return new Step(serialized, serializedFields, restFields);
    }

    private InsertPlan singleInsertRpcMaxPrimitiveColumns(
            List<VectorSchemaRoot> vsrs, BufferAllocator allocator)
            throws VastUserException
    {
        Schema schema = vsrs.get(0).getSchema();

        Map<String, Integer> columnToApproximateSize = SerializedSizeApproximator.approximateSizeByColumnVsrs(
                vsrs);

        int sumRows = vsrs
                .stream()
                .mapToInt(VectorSchemaRoot::getRowCount)
                .sum();

        LOG.debug(
                "[%s] singleInsertRpcMaxPrimitiveColumns: processing %d vsrs, sumRows: %d",
                traceToken,
                vsrs.size(), sumRows);
        LOG.debug(
                "[%s] singleInsertRpcMaxPrimitiveColumns: updatable fields count: %d",
                traceToken,
                updatableFields.size());
        String mustField = updatableFields.get(0);
        List<String> optionalFields = updatableFields.size() == 1 ?
                List.of() :
                updatableFields.subList(1, updatableFields.size());

        List<String> mightGoInFields = SerializedSizeApproximator.estimateWhichWillGoIn(
                optionalFields, columnToApproximateSize,
                ((int) vastConfig.getMaxRequestBodySize()) - columnToApproximateSize.get(
                        mustField));

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "[%s] singleInsertRpcMaxPrimitiveColumns: estimated %d field columns might go in: %s",
                    traceToken,
                    mightGoInFields.size(), mightGoInFields);
        }

        List<String> targetFieldNames = Stream
                .concat(Stream.of(mustField), mightGoInFields.stream())
                .collect(Collectors.toList());
        List<Field> targetFields = targetFieldNames
                .stream()
                .map(schema::findField)
                .collect(Collectors.toList());

        Step insertStep;
        try (VectorSchemaRoot mergedVsr = vsrToAppendTo(targetFields, sumRows,
                allocator)) {
            for (VectorSchemaRoot origVsr : vsrs) {
                VectorSchemaRoot vsr = vsrGetColumns(origVsr, targetFieldNames);
                LOG.debug("[%s] issue place : %s, %s", traceToken,
                        mergedVsr.getSchema(),
                        vsr.getSchema(), targetFieldNames);
                VectorSchemaRootAppender.append(false, mergedVsr, vsr);
            }

            LOG.debug("[%s] ASDF : %s, %s", traceToken, List.of(mustField),
                    optionalFields);
            insertStep = packAndSerialize(mergedVsr, List.of(mustField),
                    optionalFields);
        }

        List<String> unsentFields = allFields
                .stream()
                .filter(f -> !insertStep.serializedFields.contains(f))
                .collect(Collectors.toList());

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "[%s] singleInsertRpcMaxPrimitiveColumns: unsent fields left for update: %s",
                    traceToken,
                    unsentFields.stream().collect(Collectors.toList()));
        }

        List<VectorSchemaRoot> toUpdate = new ArrayList<>();
        if (!unsentFields.isEmpty()) {
            for (VectorSchemaRoot origVsr : vsrs) {
                LOG.debug(
                        "[%s] unsentVsr. schema: %s. rowCount: %s, unsentFields: %s",
                        traceToken, origVsr.getSchema(), origVsr.getRowCount(),
                        unsentFields);
                VectorSchemaRoot vsr = vsrGetColumns(origVsr, unsentFields);
                LOG.debug("[%s] unsentVsr. schema: %s. rowCount: %s",
                        traceToken, vsr.getSchema(), vsr.getRowCount());
                toUpdate.add(vsr);
            }
        }

        LOG.debug(
                "[%s] singleInsertRpcMaxPrimitiveColumns: planned insert with %d payloads, and %d vsrs for subsequent updates",
                traceToken,
                insertStep.payloads.size(), toUpdate.size());

        return new InsertPlan(insertStep.payloads, toUpdate);
    }

    private VectorSchemaRoot vsrGetColumns(VectorSchemaRoot vsr,
            List<String> columns)
    {
        List<Field> fields = filterFields(columns, vsr.getSchema().getFields());
        List<FieldVector> vectors = filterFieldVectors(columns,
                vsr.getFieldVectors());
        return new VectorSchemaRoot(fields, vectors, vsr.getRowCount());
    }

    private List<FieldVector> filterFieldVectors(List<String> columns,
            List<FieldVector> fieldVectors)
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug("[%s] filterFieldVectors filtering for: %s out of: %s",
                    traceToken,
                    columns, fieldVectors
                            .stream()
                            .map(FieldVector::getName)
                            .collect(Collectors.toList()));
        }

        List<FieldVector> res = columns
                .stream()
                .flatMap(col -> fieldVectors
                        .stream()
                        .filter(fv -> col.equals(fv.getName())))
                .collect(Collectors.toList());

        if (LOG.isDebugEnabled()) {
            LOG.debug("[%s] filterFieldVectors result: %s", traceToken, res
                    .stream()
                    .map(FieldVector::getName)
                    .collect(Collectors.toList()));
        }
        return res;
    }

    private List<Field> getFields(List<FieldVector> fieldVectors)
    {
        return fieldVectors
                .stream()
                .map(FieldVector::getField)
                .collect(Collectors.toList());
    }

    private List<Field> filterFields(List<String> toFilter, List<Field> fields)
    {
        return toFilter
                .stream()
                .flatMap(name -> fields
                        .stream()
                        .filter(f -> name.equals(f.getName())))
                .collect(Collectors.toList());
    }

    private InsertPlan simpleInsertPlan(List<VectorSchemaRoot> vsrs)
            throws VastUserException
    {
        List<InsertPlan> plans = new ArrayList<>();

        for (VectorSchemaRoot vsr : vsrs) {
            plans.add(simpleInsertPlan(vsr));
        }

        List<byte[]> allPayloads = plans
                .stream()
                .flatMap(p -> p.insertPayloads.stream())
                .collect(Collectors.toList());

        List<VectorSchemaRoot> allUpdates = plans
                .stream()
                .flatMap(p -> p.updateVsrs.stream())
                .collect(Collectors.toList());

        return new InsertPlan(allPayloads, allUpdates);
    }

    private InsertPlan simpleInsertPlan(VectorSchemaRoot vsr)
            throws VastUserException
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug("[%s] simpleInsertPlan: planning for fields: %s",
                    traceToken, allFields);
        }
        String firstField = allFields.get(0);
        List<String> restFields = allFields.size() == 1 ?
                List.of() :
                allFields.subList(1, allFields.size());
        Step step = packAndSerialize(vsr, List.of(firstField),
                restFields);

        VectorSchemaRoot restVsr = vsrGetColumns(vsr, step.restFields);
        return new InsertPlan(step.payloads, List.of(restVsr));
    }

    public List<byte[]> serializeUpdate(VectorSchemaRoot rowIdColumnVsr,
            VectorSchemaRoot columnsToUpdate, BufferAllocator allocator)
            throws VastUserException
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "[%s] SerializeUpdate: starting. rowIdColumnRowCount: %d, columnsToUpdate: %s",
                    traceToken,
                    rowIdColumnVsr.getRowCount(), columnsToUpdate
                            .getFieldVectors()
                            .stream()
                            .map(FieldVector::getName)
                            .collect(Collectors.joining(",")));
        }

        String rowIdField = rowIdColumnVsr
                .getSchema()
                .getFields()
                .get(0)
                .getName();
        List<String> remaining = columnsToUpdate
                .getSchema()
                .getFields()
                .stream()
                .map(Field::getName)
                .collect(Collectors.toList());

        try (VectorSchemaRoot vsr = new VectorSchemaRoot(Stream
                .concat(Stream.of(rowIdColumnVsr.getFieldVectors().get(0)),
                        columnsToUpdate.getFieldVectors().stream())
                .collect(Collectors.toList()))) {
            List<byte[]> payloads = new ArrayList<>();

            while (!remaining.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "[%s] SerializeUpdate: processing chunk. Remaining fields: %s",
                            traceToken,
                            remaining.stream().collect(Collectors.toList()));
                }
                List<String> musts = List.of(rowIdField, remaining.get(0));
                List<String> optionals = remaining
                        .stream()
                        .skip(1)
                        .collect(Collectors.toList());

                Step packed = packAndSerialize(vsr, musts, optionals);

                payloads.addAll(packed.payloads);
                remaining = packed.restFields;
            }
            LOG.debug("[%s] SerializeUpdate: finished. Total payloads: %d",
                    traceToken,
                    payloads.size());
            return payloads;
        }
        finally {
            rowIdColumnVsr.close();
            columnsToUpdate.close();
        }
    }

    private List<byte[]> splitByRows(VectorSchemaRoot vsr)
            throws VastUserException
    {
        if (LOG.isDebugEnabled()) {
            List<String> fieldNames = vsr
                    .getSchema()
                    .getFields()
                    .stream()
                    .map(Field::getName)
                    .collect(Collectors.toList());
            LOG.debug(
                    "[%s] splitByRows: splitting VSR with fields: %s, total rowCount: %d",
                    traceToken,
                    fieldNames, vsr.getRowCount());
        }
        RecordBatchSplitter splitter = new RecordBatchSplitter(
                vastConfig.getMaxRequestBodySize(), SERIALIZER,
                Optional.of(metrics));
        List<byte[]> bodies = splitter.split(vsr, getRowIdsMaxRowsPerInsert());
        LOG.debug("[%s] splitByRows: resulted in %d payloads", traceToken,
                bodies.size());
        metrics.recordSplitByRows(bodies.size(),
                bodies.stream().mapToInt(body -> body.length).sum());
        return bodies;
    }

    private List<byte[]> splitByRows(List<FieldVector> fieldVectors,
            int rowCount)
            throws VastUserException
    {
        List<Field> fields = getFields(fieldVectors);
        return splitByRows(
                new VectorSchemaRoot(fields, fieldVectors, rowCount));
    }

    private byte[] serialize(VectorSchemaRoot vsr)
    {
        byte[] payload = SERIALIZER.apply(vsr).get();
        if (LOG.isDebugEnabled()) {
            List<String> fieldNames = vsr
                    .getSchema()
                    .getFields()
                    .stream()
                    .map(Field::getName)
                    .collect(Collectors.toList());
            LOG.debug(
                    "[%s] Serialized VSR. Fields: %s, rowCount: %d, payload size: %d",
                    traceToken,
                    fieldNames, vsr.getRowCount(), payload.length);
        }
        return payload;
    }

    private byte[] serialize(List<FieldVector> fieldVectors, int rowCount)
    {
        return serialize(
                new VectorSchemaRoot(getFields(fieldVectors), fieldVectors,
                        rowCount));
    }

    private int getRowIdsMaxRowsPerInsert()
    {
        int maxRequestBodySize = (int) vastConfig.getMaxRequestBodySize();

        if (rowIdType == RowIDStrategyType.UNSIGNED_INT64) {
            // divide by bytes in int64 (size of rowid)
            return maxRequestBodySize / 8;
        }
        else if (rowIdType == RowIDStrategyType.DECIMAL_128) {
            // divide by bytes in decimal128 (size of rowid)
            return maxRequestBodySize / 16;
        }
        else {
            throw new RuntimeException(
                    String.format("unsupported RowIDStrategyType: %s",
                            rowIdType));
        }
    }

    public static class InsertPlan
    {
        public final List<byte[]> insertPayloads;
        public final List<VectorSchemaRoot> updateVsrs;

        InsertPlan(List<byte[]> insertPayloads,
                List<VectorSchemaRoot> updateVsrs)
        {
            this.insertPayloads = insertPayloads;
            this.updateVsrs = updateVsrs;
        }
    }

    private static class Step
    {
        public final List<byte[]> payloads;
        public final List<String> serializedFields;
        public final List<String> restFields;

        Step(List<byte[]> payloads, List<String> serializedFields,
                List<String> restFields)
        {
            this.payloads = payloads;
            this.restFields = restFields;
            this.serializedFields = serializedFields;
        }
    }
}
