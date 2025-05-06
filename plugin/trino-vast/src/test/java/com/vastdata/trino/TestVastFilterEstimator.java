/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVastFilterEstimator
{
    private static VastColumnHandle buildBool()
    {
        ArrowType arrowBool = new ArrowType.Bool();
        Field fieldBool = Field.nullable("bool_c", arrowBool);
        return VastColumnHandle.fromField(fieldBool);
    }

    private static VastColumnHandle buildTiny()
    {
        ArrowType arrowTiny = new ArrowType.Int(8, true);
        Field fieldTiny = Field.nullable("tiny_c", arrowTiny);
        return VastColumnHandle.fromField(fieldTiny);
    }

    private static VastColumnHandle buildSmall()
    {
        ArrowType arrowSmall = new ArrowType.Int(16, true);
        Field fieldSmall = Field.nullable("small_c", arrowSmall);
        return VastColumnHandle.fromField(fieldSmall);
    }

    private static VastColumnHandle buildInt()
    {
        ArrowType arrowInt = new ArrowType.Int(32, true);
        Field fieldInt = Field.nullable("int_c", arrowInt);
        return VastColumnHandle.fromField(fieldInt);
    }

    private VastColumnHandle vcBool = buildBool();
    private VastColumnHandle vcTiny = buildTiny();
    private VastColumnHandle vcSmall = buildSmall();
    private VastColumnHandle vcInt = buildInt();

    private TableStatistics buildStats()
    {
        ColumnStatistics csBool = new ColumnStatistics(Estimate.of(0.01), Estimate.of(2), Estimate.unknown(),
                                                       Optional.of(new DoubleRange(0, 1)));
        ColumnStatistics csTiny = new ColumnStatistics(Estimate.of(0.01), Estimate.of(200), Estimate.unknown(),
                                                       Optional.of(new DoubleRange(-111, 111)));
        ColumnStatistics csSmall = new ColumnStatistics(Estimate.of(0.01), Estimate.of(5000), Estimate.unknown(),
                                                        Optional.of(new DoubleRange(-11111, 11111)));
        ColumnStatistics csInt = new ColumnStatistics(Estimate.of(0.01), Estimate.of(5000), Estimate.unknown(),
                                                        Optional.of(new DoubleRange(-1111111111, 1111111111)));
        return new TableStatistics(Estimate.of(5000),
                                   Map.of(vcBool, csBool, vcTiny, csTiny, vcSmall, csSmall, vcInt, csInt));
    }

    @Test
    public void testTableWithoutPredicate()
    {
        double estimate = FilterEstimator.estimateSelectivity(TupleDomain.all(), buildStats(), Optional.empty());
        assertThat(estimate == 1.0);
    }

    @Test
    public void tableWithPredicate()
    {
        Range range = Range.range(TINYINT, 10L, false, 20L, false);
        ValueSet valueSet = ValueSet.ofRanges(range);
        Domain domain = Domain.create(valueSet, true);
        TupleDomain<VastColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(vcTiny, domain));
        double estimate = FilterEstimator.estimateSelectivity(tupleDomain, buildStats(), Optional.empty());
        assertThat(estimate > 0.03995 && estimate < 0.04);
    }

    @Test
    public void tableWithOredPredicate()
    {
        Range range1 = Range.lessThan(SMALLINT, 1000L);
        Range range2 = Range.greaterThanOrEqual(SMALLINT, 2000L);
        ValueSet valueSet = ValueSet.ofRanges(range1, range2);
        Domain domain = Domain.create(valueSet, true);
        TupleDomain<VastColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(vcSmall, domain));
        double estimate = FilterEstimator.estimateSelectivity(tupleDomain, buildStats(), Optional.empty());
        assertThat(estimate > 0.955 && estimate < 0.956);
    }

    @Test
    public void tableWithEqualPredicate()
    {
        Range range = Range.equal(INTEGER, 100L);
        ValueSet valueSet = ValueSet.ofRanges(range);
        Domain domain = Domain.create(valueSet, true);
        TupleDomain<VastColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(vcInt, domain));
        double estimate = FilterEstimator.estimateSelectivity(tupleDomain, buildStats(), Optional.empty());
        assertThat(estimate == 0.0002);
    }
}
