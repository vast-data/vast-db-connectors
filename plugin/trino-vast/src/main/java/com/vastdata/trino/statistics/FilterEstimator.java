/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.TupleDomain.ColumnDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.vastdata.client.schema.ArrowSchemaUtils.VASTDB_ROW_ID_FIELD;

import static io.trino.spi.statistics.StatsUtil.toStatsRepresentation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Math.max;
import static java.lang.Math.min;

public final class FilterEstimator
{
    private static final Logger LOG = Logger.get(FilterEstimator.class);
    private static final VastColumnHandle rowIdHandle = VastColumnHandle.fromField(VASTDB_ROW_ID_FIELD);

    private FilterEstimator() {}

    private static boolean isStatsType(Type t) {
        return t == BOOLEAN || t == TINYINT || t == SMALLINT || t == INTEGER || t == BIGINT || t == REAL || t == DOUBLE
            || t == DATE || (t instanceof DecimalType) || (t instanceof TimestampType) 
            || (t instanceof TimestampWithTimeZoneType);
    }

    private static double estimateOpSelectivity(Range r, ColumnStatistics colStat)
    {
        if (r.isAll()) {
            return 1.0;
        }
        Optional<DoubleRange> sr = colStat.getRange();
        Estimate distinctCount = colStat.getDistinctValuesCount();
        if (r.isSingleValue()) { // =
            OptionalDouble v = toStatsRepresentation(r.getType(), r.getSingleValue());
            if (v.isEmpty() || sr.isEmpty()
                || (v.getAsDouble() >= sr.orElseThrow().getMin() && v.getAsDouble() <= sr.orElseThrow().getMax())) {
                if (distinctCount.isUnknown()) {
                    return 1.0;
                }
                else {
                    return 1.0/distinctCount.getValue();
                }
            }
            else {
                return 0.0;
            }
        }
        if (!isStatsType(r.getType()) || sr.isEmpty() || distinctCount.isUnknown()) {
            return 1.0;
        }
        
        double rmin = r.isLowUnbounded()? NEGATIVE_INFINITY : toStatsRepresentation(r.getType(), r.getLowBoundedValue()).getAsDouble();
        double rmax = r.isHighUnbounded()? POSITIVE_INFINITY : toStatsRepresentation(r.getType(), r.getHighBoundedValue()).getAsDouble(); 
        double cmin = max(rmin, sr.orElseThrow().getMin());
        double cmax = min(rmax, sr.orElseThrow().getMax());
        if (cmin > cmax) {
            return 0.0;
        }
        double ph = 1.0;
        if ((rmax == sr.orElseThrow().getMin() && r.isHighInclusive()) || rmin == sr.orElseThrow().getMax() && r.isLowInclusive()) {
            return 1.0/distinctCount.getValue();
        }
        return (cmax - cmin)/(sr.orElseThrow().getMax() - sr.orElseThrow().getMin());
    }

    public static double estimateSelectivity(TupleDomain<VastColumnHandle> tupleDomain, 
                                      TableStatistics statistics, Optional<Long> limit)
    {
        TupleDomain<VastColumnHandle> strippedMetadataTupleDomain = tupleDomain.transformKeys(VastColumnHandle::stripMetadata);
        Map<VastColumnHandle, Domain> domains = strippedMetadataTupleDomain.getDomains().orElse(Map.of());
        LOG.debug("Domains: %s", domains);
        double rv = 1.;
        for (Map.Entry<VastColumnHandle, Domain> entry : domains.entrySet()) {
            if (rowIdHandle.equals(entry.getKey())) {
                continue;
            }
            ColumnStatistics colStats = statistics.getColumnStatistics().get(entry.getKey());
            if (null == colStats) {
                continue;
            }
            Domain dom = entry.getValue();
            double nulls = 0;
            if (dom.isNullAllowed() ) {
                Estimate nf = colStats.getNullsFraction();
                if (!nf.isUnknown()) {
                    nulls = nf.getValue();
                    if (dom.isOnlyNull()) {
                        rv *= nulls;
                        continue;
                    }
                }
            }
            List<Range> orderedRanges = dom.getValues().getRanges().getOrderedRanges();
            final double colSel = orderedRanges.stream()
                .mapToDouble(p -> estimateOpSelectivity(p, colStats))
                .reduce((l, r) -> l + r - l * r)
                .orElse(1.0);
            rv *= colSel + nulls - colSel * nulls;
        }
        return (limit.isPresent() && limit.orElseThrow()/statistics.getRowCount().getValue() < rv)? limit.orElseThrow()/statistics.getRowCount().getValue() : rv;
    }

    public static TupleDomain<VastColumnHandle>[] splitDomains(final TupleDomain<VastColumnHandle> tupleDomain, final List<String> sorted) 
    {
        Map<VastColumnHandle, Domain> origDomains = tupleDomain.getDomains().orElseThrow();
        ArrayList<ColumnDomain<VastColumnHandle>> sortedDoms =
            new ArrayList<>(Collections.nCopies(sorted.size(), (ColumnDomain<VastColumnHandle>) null));
        ArrayList<ColumnDomain<VastColumnHandle>> unsortedDoms = new ArrayList<>();
        for (var entry : origDomains.entrySet()) {
            int idx = sorted.indexOf(entry.getKey().getField().getName());
            if (idx >= 0) {
                sortedDoms.add(idx, new ColumnDomain<>(entry.getKey(), entry.getValue()));
            }
            else {
                unsortedDoms.add(new ColumnDomain<>(entry.getKey(), entry.getValue()));
            }
        }
        int nulli = sortedDoms.size();
        for (int i = 0; i < sortedDoms.size(); i++) {
            if (nulli ==sortedDoms.size() && sortedDoms.get(i) == null) {
                nulli = i;
            }
            if (sortedDoms.get(i) != null) {
                unsortedDoms.add(sortedDoms.get(i));
            }
        }
        sortedDoms.subList(nulli, sortedDoms.size()).clear();
        return (TupleDomain<VastColumnHandle>[]) new TupleDomain[] {TupleDomain.fromColumnDomains(Optional.of(sortedDoms)), TupleDomain.fromColumnDomains(Optional.of(unsortedDoms))};
    }
}
