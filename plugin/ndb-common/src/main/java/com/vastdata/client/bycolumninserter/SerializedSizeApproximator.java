/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.bycolumninserter;

import io.airlift.log.Logger;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class SerializedSizeApproximator
{
    private static final Logger LOG = Logger.get(ByColumnInserter.class);

    private static final double BUFFER_SIZE_MULTIPLIER = 1.05;

    public static List<FieldVector> estimateWhichWillGoIn(
            List<FieldVector> candidates, long spaceLeft)
    {
        List<FieldVector> sortedCandidates = new ArrayList<>(candidates);

        sortedCandidates.sort((v1, v2) -> {
            return Integer.compare(fieldVectorEstimatedSize(v2),
                    fieldVectorEstimatedSize(v1));
        });

        List<FieldVector> result = new ArrayList<>();
        long currentSpaceLeft = spaceLeft;

        for (FieldVector v : sortedCandidates) {
            int estimatedSize = fieldVectorEstimatedSize(v);

            if (estimatedSize <= currentSpaceLeft) {
                LOG.debug(
                        "Adding vector '%s' (rows: %d) with estimated size %d. Fits in space left: %d",
                        v.getName(), v.getValueCount(), estimatedSize,
                        currentSpaceLeft);
                result.add(v);
                currentSpaceLeft -= estimatedSize;
            }
            else {
                LOG.debug(
                        "Skipping vector '%s' (rows: %d) with estimated size %d. Does not fit in space left: %d",
                        v.getName(), v.getValueCount(), estimatedSize,
                        currentSpaceLeft);
            }
        }
        return result;
    }

    /**
     * doesn't actually use the field's vector
     */
    public static List<String> estimateWhichWillGoIn(
            Map<String, Integer> fieldToApproximateSize, long spaceLeft)
    {
        return estimateWhichWillGoIn(fieldToApproximateSize
                        .keySet()
                        .stream()
                        .collect(Collectors.toList()), fieldToApproximateSize,
                spaceLeft);
    }

    public static List<String> estimateWhichWillGoIn(Iterable<String> fields,
            Map<String, Integer> fieldToApproximateSize, long spaceLeft)
    {
        List<String> willProbablyGoIn = new ArrayList<>();
        long currentSpaceLeft = spaceLeft;

        for (String field : fields) {
            if (fieldToApproximateSize.get(field) <= currentSpaceLeft) {
                willProbablyGoIn.add(field);
                currentSpaceLeft -= fieldToApproximateSize.get(field);
            }
        }
        return willProbablyGoIn;
    }

    public static Map<String, Integer> approximateSizeByColumnFieldVectors(
            List<FieldVector> vectors)
    {
        return vectors
                .stream()
                .collect(java.util.stream.Collectors.toMap(FieldVector::getName,
                        SerializedSizeApproximator::fieldVectorEstimatedSize,
                        Integer::sum, java.util.LinkedHashMap::new));
    }

    public static Map<String, Integer> approximateSizeByColumnVsrs(
            List<VectorSchemaRoot> vsrs, List<String> interestFields)
    {
        List<FieldVector> allVectors = vsrs
                .stream()
                .flatMap(vsr -> vsr.getFieldVectors().stream())
                .filter(fv -> interestFields.contains(fv.getName()))
                .collect(Collectors.toList());

        return approximateSizeByColumnFieldVectors(allVectors);
    }

    public static Map<String, Integer> approximateSizeByColumnVsrs(
            List<VectorSchemaRoot> vsrs)
    {
        return approximateSizeByColumnVsrs(vsrs, vsrs
                .get(0)
                .getSchema()
                .getFields()
                .stream()
                .map(Field::getName)
                .collect(Collectors.toList()));
    }

    public static int fieldVectorEstimatedSize(FieldVector v)
    {
        return (int) (v.getBufferSize() * BUFFER_SIZE_MULTIPLIER);
    }

    public static int approximateFieldVectorBufferSizeFor(FieldVector fV,
            int rowCount)
    {
        return (int) (fV.getBufferSizeFor(rowCount) * BUFFER_SIZE_MULTIPLIER);
    }

    public static int approximateSize(VectorSchemaRoot vsr)
    {
        return approximateSize(vsr.getFieldVectors());
    }

    public static int approximateSize(List<FieldVector> fieldVectors)
    {
        return fieldVectors
                .stream()
                .mapToInt(SerializedSizeApproximator::fieldVectorEstimatedSize)
                .sum();
    }
}
