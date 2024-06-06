/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.airlift.log.Logger;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

import static com.vastdata.client.schema.ArrowSchemaUtils.VASTDB_ROW_ID_FIELD;

public class VastRowsEstimator {
    private static final Logger LOG = Logger.get(VastRowsEstimator.class);
    private static final VastColumnHandle rowIdHandle = VastColumnHandle.fromField(VASTDB_ROW_ID_FIELD);

    public VastRowsEstimator() {
    }

    public Estimate getMinimalRowsEstimation(TupleDomain<VastColumnHandle> tupleDomain, Estimate rowsEstimationByStats) {
        Type rowIdType = rowIdHandle.getColumnSchema().getType();
        LOG.debug("row count estimates by statistics file: %s", rowsEstimationByStats);
        TupleDomain<VastColumnHandle> strippedMetadataTupleDomain = tupleDomain.transformKeys(VastColumnHandle::stripMetadata);
        Map<VastColumnHandle, Domain> domains = strippedMetadataTupleDomain.getDomains().orElse(Map.of());
        LOG.debug("Domains: %s", domains);
        List<Range> orderedRanges = domains.getOrDefault(rowIdHandle, Domain.all(rowIdType)).getValues().getRanges().getOrderedRanges();
        LOG.debug("orderedRanges: %s", orderedRanges);

        Long rowCount = 0L;
        for (Range range : orderedRanges) {
            if (range.isAll()) {
                return rowsEstimationByStats;
            }
            if (rowsEstimationByStats.isUnknown() && range.getHighValue().isEmpty()) {
                return Estimate.unknown();
            } else {
                Long low = (Long) range.getLowValue().orElse(-1L);
                // due to the if condition either range.getHighValue() is defined or rowsEstimationByStats is defined or both
                Long high = (Long) range.getHighValue().orElse(Double.valueOf(rowsEstimationByStats.getValue()).longValue());
                long inclusiveFactor = (range.isLowInclusive() ? 1 : 0) + (range.isHighInclusive() ? 1 : 0) - 1;
                rowCount += high - low + inclusiveFactor;
            }
        }
        LOG.debug("row count by ROW_ID: %s", rowCount);
        return rowsEstimationByStats.isUnknown() ?
                Estimate.of(rowCount.doubleValue()) :
                Estimate.of(Double.min(rowsEstimationByStats.getValue(), rowCount.doubleValue()));
    }
}
