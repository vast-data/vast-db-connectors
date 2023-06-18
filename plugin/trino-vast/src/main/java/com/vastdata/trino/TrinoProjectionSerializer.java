/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.vastdata.client.schema.EnumeratedSchema;
import com.vastdata.trino.expression.VastExpression;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import org.apache.arrow.computeir.flatbuf.Project;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class TrinoProjectionSerializer
        extends TrinoExpressionSerializer
{
    private static final Logger LOG = Logger.get(TrinoProjectionSerializer.class);

    private static class ColumnEntry {
        public ColumnEntry(VastColumnHandle column, List<Integer> projections, Field response)
        {
            this.column = column;
            this.projections = projections;
            this.response = response;
        }

        private final VastColumnHandle column;
        private final List<Integer> projections;
        private final Field response;
    }

    private final List<ColumnEntry> entries;

    public TrinoProjectionSerializer(List<VastColumnHandle> projectedColumns, EnumeratedSchema enumeratedSchema)
    {
        ImmutableList.Builder<ColumnEntry> builder = ImmutableList.builder();
        for (VastColumnHandle column : projectedColumns) {
            List<Integer> projections = collectProjectionIndices(column, enumeratedSchema, false);
            Field response = column.getBaseField();
            VastExpression expression = column.getExpression();
            // modify response schema in case of a projection pushdown
            if (nonNull(expression)) {
                response = TypeUtils.convertTrinoTypeToArrowField(expression.getResultType(), expression.toString(), true);
            }
            builder.add(new ColumnEntry(column, projections, response));
        }
        this.entries = builder.build();
    }

    public List<Integer> getProjectionIndices()
    {
        EnumeratedSchema responseSchema = new EnumeratedSchema(getResponseSchema().getFields());
        Set<Integer> result = new LinkedHashSet<>();
        entries.forEach(e -> result.addAll(collectProjectionIndices(e.column, responseSchema, true)));
        LOG.debug("projection indices: %s", result);
        return List.copyOf(result);
    }

    public Schema getResponseSchema()
    {
        Set<Field> responseFields = entries.stream().map(e -> e.response).collect(Collectors.toCollection(LinkedHashSet::new));
        LOG.debug("response fields: %s", responseFields);
        return new Schema(responseFields);
    }

    public LinkedHashMap<Field, LinkedHashMap<List<Integer>, Integer>> getBaseFieldWithProjections()
    {
        // Since the schema is references by projections and predicate FieldIndex expressions, we need to keep its fields ordered
        QueryDataBaseFieldsWithProjectionsMappingBuilder builder = new QueryDataBaseFieldsWithProjectionsMappingBuilder();
        entries.forEach(entry -> builder.put(entry.response, entry.column.getProjectionPath()));
        return builder.build();
    }

    @Override
    protected int serialize()
    {
        Set<Integer> serializedPositions = new HashSet<>();
        ImmutableList.Builder<Integer> expressionsBuilder = ImmutableList.builder();
        for (ColumnEntry entry : entries) {
            for (int position : entry.projections) {
                int expression = buildColumn(position);
                // apply projection pushdown (if needed)
                VastExpression projection = entry.column.getExpression();
                LOG.debug("expression: position=%d projection=%s", position, projection);
                if (nonNull(projection)) {
                    Type columnType = TypeUtils.convertArrowFieldToTrinoType(entry.column.getField());
                    IntStream column = IntStream.of(expression);
                    IntStream args = projection.getArgs().stream().mapToInt(block -> buildLiteral(columnType, readNativeValue(columnType, block, 0)));
                    expression = buildFunction(projection.getFunction(), IntStream.concat(column, args).toArray());
                }
                else if (!serializedPositions.add(position)) {
                    continue;
                }
                expressionsBuilder.add(expression);
            }
        }
        return Project.createExpressionsVector(builder, Ints.toArray(expressionsBuilder.build()));
    }

    private static List<Integer> collectProjectionIndices(VastColumnHandle column, EnumeratedSchema enumeratedSchema, boolean supportExpressionProjection)
    {
        LOG.debug("collecting indices for %s", column);
        Set<Integer> orderedProjectionsSet = new LinkedHashSet<>();
        enumeratedSchema.collectProjectionIndices(
                (supportExpressionProjection && nonNull(column.getExpression())) ? column.getExpression().getSymbol() : column.getBaseField().getName(),
                column.getProjectionPath(),
                orderedProjectionsSet::add);
        return List.copyOf(orderedProjectionsSet);
    }
}