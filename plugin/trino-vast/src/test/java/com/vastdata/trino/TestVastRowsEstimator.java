/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.type.IntegerType;
import org.testng.annotations.Test;

import java.util.Map;

import static com.vastdata.client.schema.ArrowSchemaUtils.VASTDB_ROW_ID_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVastRowsEstimator
{
    private VastRowsEstimator vastRowsEstimator = new VastRowsEstimator();

    @Test
    public void testTableWithoutRowIdPredicates()
    {
        Estimate estimate = this.vastRowsEstimator.getMinimalRowsEstimation(TupleDomain.all(), Estimate.of(200));
        assertThat(estimate.getValue()).isEqualTo(200.0);
    }

    @Test
    public void tableWithRowIdPredicates()
    {
        Range range = Range.range(IntegerType.INTEGER, 1000L, false, 2000L, false);
        ValueSet valueSet = ValueSet.ofRanges(range);
        Domain domain = Domain.create(valueSet, true);
        TupleDomain<VastColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(VastColumnHandle.fromField(VASTDB_ROW_ID_FIELD), domain));
        Estimate estimate = this.vastRowsEstimator.getMinimalRowsEstimation(tupleDomain, Estimate.of(2000));
        assertThat(estimate.getValue()).isEqualTo(999.0);
        estimate = this.vastRowsEstimator.getMinimalRowsEstimation(tupleDomain, Estimate.unknown());
        assertThat(estimate.getValue()).isEqualTo(999.0);
    }

    @Test
    public void tableWithOredRangePredicate()
    {
        Range range1 = Range.lessThan(IntegerType.INTEGER, 1000L);
        Range range2 = Range.greaterThanOrEqual(IntegerType.INTEGER, 2000L);
        ValueSet valueSet = ValueSet.ofRanges(range1, range2);
        Domain domain = Domain.create(valueSet, true);
        TupleDomain<VastColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(VastColumnHandle.fromField(VASTDB_ROW_ID_FIELD), domain));
        Estimate estimate = this.vastRowsEstimator.getMinimalRowsEstimation(tupleDomain, Estimate.of(5000));
        assertThat(estimate.getValue()).isEqualTo(4000.0);
        estimate = this.vastRowsEstimator.getMinimalRowsEstimation(tupleDomain, Estimate.unknown());
        assertThat(estimate.isUnknown()).isTrue();
    }

    @Test
    public void tableWithEqualPredicate()
    {
        Range range = Range.equal(IntegerType.INTEGER, 100L);
        ValueSet valueSet = ValueSet.ofRanges(range);
        Domain domain = Domain.create(valueSet, true);
        TupleDomain<VastColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(VastColumnHandle.fromField(VASTDB_ROW_ID_FIELD), domain));
        Estimate estimate = this.vastRowsEstimator.getMinimalRowsEstimation(tupleDomain, Estimate.of(5000));
        assertThat(estimate.getValue()).isEqualTo(1.0);
    }
}
