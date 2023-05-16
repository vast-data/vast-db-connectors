/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.vastdata.client.schema.EnumeratedSchema;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class TrinoPredicateSerializer
        extends TrinoExpressionSerializer
{
    private static final Logger LOG = Logger.get(TrinoPredicateSerializer.class);

    private final Map<Long, Domain> domainsMap;  // mapping a column position within the schema to its domain
    private final Map<Long, String> substringsMap;

    public TrinoPredicateSerializer(TupleDomain<VastColumnHandle> tupleDomain, List<VastSubstringMatch> substringMatches, EnumeratedSchema enumeratedSchema)
    {
        LOG.debug("serializing %s and %s (schema=%s) to Arrow Compute IR flatbuffer format", tupleDomain, substringMatches, enumeratedSchema);
        this.domainsMap = tupleDomain.getDomains().orElseThrow().entrySet().stream().collect(Collectors.toMap(
                entry -> getColumnPosition(entry.getKey(), enumeratedSchema),
                Map.Entry::getValue));
        LOG.debug("domainsMap=%s", domainsMap);

        this.substringsMap = new HashMap<>();
        // Represented as an AND between LIKE expressions (one per column)
        for (VastSubstringMatch substringMatch : substringMatches) {
            long columnId = getColumnPosition(substringMatch.getColumn(), enumeratedSchema).longValue();
            String pattern = substringMatch.getPattern();
            this.substringsMap.put(columnId, pattern);
        }
        LOG.debug("substringsMap=%s", substringsMap);
    }

    private static Long getColumnPosition(VastColumnHandle column, EnumeratedSchema enumeratedSchema)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        enumeratedSchema.collectProjectionIndices(column.getBaseField().getName(), column.getProjectionPath(), builder::add);
        return Iterables.getOnlyElement(builder.build()).longValue(); // only predicates over leaf columns are supported
    }

    @Override
    public int serialize()
    {
        // `ALL` predicate is serialized as an empty `domainsMap`.
        int[] offsets = new int[domainsMap.size() + substringsMap.size()];
        int i = 0;
        for (Map.Entry<Long, Domain> entry : domainsMap.entrySet()) {
            LOG.debug("serializing %s for column position %s", entry.getValue(), entry.getKey());
            offsets[i++] = buildDomain(buildColumn(entry.getKey()), entry.getValue());
        }
        for (Map.Entry<Long, String> entry : substringsMap.entrySet()) {
            LOG.debug("serializing substring '%s' match for column position %s", entry.getValue(), entry.getKey());
            int column = buildColumn(entry.getKey());
            int literal = buildLiteral(VARCHAR, Slices.utf8Slice(entry.getValue()));
            int match = buildMatchSubstring(column, literal);
            offsets[i++] = buildOr(match);
        }
        return buildAnd(offsets);
    }

    private int buildDomain(int column, Domain domain)
    {
        ValueSet values = domain.getValues();
        if (values.isAll()) {
            if (!domain.isNullAllowed()) {
                // Special case for pushing down `c IS NOT NULL` - which is represented as a (-inf, +inf) range in Trino
                return buildOr(buildIsValid(column));
            }
            // NULL is allowed and `values` is ALL -> `domain` is ALL -> Trino should have optimized away it from the predicate
            throw new IllegalArgumentException(format("redundant domain %s for column %d", domain, column));
        }
        // `values` is not `ALL` -> there are non trivial ranges:
        Ranges ranges = values.getRanges();
        int length = ranges.getRangeCount();
        if (domain.isNullAllowed()) {
            length += 1;
        }
        int[] offsets = new int[length];
        int i = 0;
        // TODO: can be optimized to use underlying Block
        for (Range range : ranges.getOrderedRanges()) {
            offsets[i++] = buildRange(column, range);
        }
        if (domain.isNullAllowed()) {
            offsets[i++] = buildIsNull(column);
        }
        verify(i == length, "%d predicates were set (instead of %d)", i, length);
        return buildOr(offsets);
    }

    private int buildRange(int column, Range range)
    {
        if (range.isSingleValue()) {
            return buildEqual(column, buildLiteral(range.getType(), range.getSingleValue()));
        }
        verify(!range.isAll(), "redundant %s range for column %d", range, column);
        if (range.isHighUnbounded()) {
            int literal = buildLiteral(range.getType(), range.getLowBoundedValue());
            return buildGreater(column, literal, range.isLowInclusive());
        }
        if (range.isLowUnbounded()) {
            int literal = buildLiteral(range.getType(), range.getHighBoundedValue());
            return buildLess(column, literal, range.isHighInclusive());
        }
        int lowLiteral = buildLiteral(range.getType(), range.getLowBoundedValue());
        int highLiteral = buildLiteral(range.getType(), range.getHighBoundedValue());
        return buildAnd(
                buildGreater(column, lowLiteral, range.isLowInclusive()),
                buildLess(column, highLiteral, range.isHighInclusive()));
    }
}
